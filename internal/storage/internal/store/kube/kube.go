// Package kube is the Store on plain Kubernetes ConfigMaps in one namespace.
//
// Each record (volume, snapshot, bucket, operation, token) is one ConfigMap
// named by a hash of its key. One head ConfigMap serializes commits: an
// Update reads with the head clear, then commits by a compare-and-swap that
// writes its whole intent into the head, rolls each write forward with a
// compare-and-swap on the record's resourceVersion, and clears the head. A
// commit that moved the head since the reads retries, so every Update is
// serializable like the SQLite transaction it replaces.
//
// A writer that crashes after the commit leaves the intent in the head. The
// next writer waits TakeoverAfter from the intent's start and rolls it
// forward itself, so a crash between two record writes is repaired before
// anything else is written. An Update whose intent is in the head has
// succeeded, even when its own roll forward did not finish. Every roll runs
// under a deadline of WriterBudget, well inside TakeoverAfter, and reads the
// head again under it, so a roller that wakes late writes nothing. A second
// roll of the same intent is a no-op: the resourceVersion preconditions and
// the txn annotation see the writes already made. An Update that wrote
// nothing checks that the head did not move during its reads.
//
// A removed record stays as a tombstone, so a create can never land on a name
// a later commit removed. Open purges tombstones older than TombstoneTTL
// through the same head.
//
// Object records are not kept here (#141, vws-deploy#478): every object read
// and write fails with errObjects.
package kube

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/storage/internal/store"
)

const (
	headName       = "vws-storage-head"
	labelManagedBy = "app.kubernetes.io/managed-by"
	managedBy      = "vws-storage"
	labelRecord    = "vws.io/record"
	labelLive      = "vws.io/live"
	labelTombstone = "vws.io/tombstone"
	annKey         = "vws.io/key"
	annTxn         = "vws.io/txn"
	annRemovedAt   = "vws.io/removed-at"
	dataRecord     = "record"
	dataIntent     = "intent"
	listPage       = 500
)

var errObjects = errors.New("kubestore: object records are not kept in the Kubernetes API")

// Options configures a Store. Zero durations take the defaults.
type Options struct {
	Namespace string
	// CallTimeout bounds one API call (default 10s).
	CallTimeout time.Duration
	// WriterBudget bounds the roll forward after a commit (default 20s). It
	// exceeds CallTimeout, so one slow call does not leave the intent behind.
	WriterBudget time.Duration
	// TakeoverAfter is how old an intent must be before another writer rolls
	// it forward (default 60s). It must exceed WriterBudget plus clock skew.
	TakeoverAfter time.Duration
	// TombstoneTTL is how long a removed record's tombstone stays (default 24h).
	TombstoneTTL time.Duration
	// LeaseDuration, RenewDeadline and RetryPeriod drive the vws-storage-run
	// Lease that Lead campaigns for (defaults 15s, 10s, 2s).
	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RetryPeriod   time.Duration
	// Identity names this replica in the Lease (default: hostname and a
	// random suffix).
	Identity string
	Now      func() time.Time
	Log      *slog.Logger
}

func (o Options) withDefaults() Options {
	if o.CallTimeout <= 0 {
		o.CallTimeout = 10 * time.Second
	}
	if o.WriterBudget <= 0 {
		o.WriterBudget = 20 * time.Second
	}
	if o.TakeoverAfter <= 0 {
		o.TakeoverAfter = time.Minute
	}
	if o.TombstoneTTL <= 0 {
		o.TombstoneTTL = 24 * time.Hour
	}
	if o.LeaseDuration <= 0 {
		o.LeaseDuration = 15 * time.Second
	}
	if o.RenewDeadline <= 0 {
		o.RenewDeadline = 10 * time.Second
	}
	if o.RetryPeriod <= 0 {
		o.RetryPeriod = 2 * time.Second
	}
	if o.Identity == "" {
		o.Identity = identity()
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Log == nil {
		o.Log = slog.Default()
	}
	return o
}

// Store is safe for concurrent use, also across processes on one namespace.
type Store struct {
	c kubernetes.Interface
	o Options
	// afterCommit, when set by a test, runs between the commit and the roll
	// forward, where a writer can be suspended.
	afterCommit func()
}

var _ store.Store = (*Store)(nil)

// Open creates the head if it is missing, finishes an intent a crashed writer
// left, and purges old tombstones.
func Open(ctx context.Context, c kubernetes.Interface, o Options) (*Store, error) {
	if o.Namespace == "" {
		return nil, fmt.Errorf("kubestore: namespace: %w", model.ErrInvalidArgument)
	}
	s := &Store{c: c, o: o.withDefaults()}
	if err := checkLease(s.o); err != nil {
		return nil, err
	}
	head := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: headName, Labels: map[string]string{labelManagedBy: managedBy}},
	}
	err := s.call(ctx, func(ctx context.Context) error {
		_, err := s.cms().Create(ctx, head, metav1.CreateOptions{})
		return apiErr(err)
	})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return nil, fmt.Errorf("kubestore: create head: %w", err)
	}
	if _, err := s.clearHead(ctx); err != nil {
		return nil, err
	}
	if err := s.purge(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

// Connect opens the store with a client built from the kubeconfig at path.
// An empty path uses the in-cluster credentials.
func Connect(ctx context.Context, path string, o Options) (*Store, error) {
	c, err := client(path)
	if err != nil {
		return nil, err
	}
	return Open(ctx, c, o)
}

// ConnectReader is Connect for reads only. It writes nothing, not even the
// head, so a namespace the store never used reads as empty. An intent in the
// head means a commit is not finished, and is refused rather than rolled.
func ConnectReader(ctx context.Context, path string, o Options) (store.Reader, error) {
	c, err := client(path)
	if err != nil {
		return nil, err
	}
	if o.Namespace == "" {
		return nil, fmt.Errorf("kubestore: namespace: %w", model.ErrInvalidArgument)
	}
	s := &Store{c: c, o: o.withDefaults()}
	head, err := s.head(ctx)
	switch {
	case apierrors.IsNotFound(err):
		return s, nil
	case err != nil:
		return nil, err
	}
	if _, ok := head.Data[dataIntent]; ok {
		return nil, fmt.Errorf("kubestore: a commit in %s is not finished: open the store for writing once", o.Namespace)
	}
	return s, nil
}

func client(path string) (kubernetes.Interface, error) {
	rc, err := clientcmd.BuildConfigFromFlags("", path)
	if err != nil {
		return nil, fmt.Errorf("kubestore: kubeconfig: %w", err)
	}
	// No client-side limit: a call queued in the limiter holds a stale head
	// read, so concurrent commits lose the head race until they give up.
	// The API server's priority and fairness throttles instead, and call
	// retries its 429.
	rc.QPS = -1
	c, err := kubernetes.NewForConfig(rc)
	if err != nil {
		return nil, fmt.Errorf("kubestore: client: %w", err)
	}
	return c, nil
}

// Close releases nothing; the client belongs to the caller.
func (s *Store) Close() error { return nil }

func (s *Store) cms() typedcorev1.ConfigMapInterface { return s.c.CoreV1().ConfigMaps(s.o.Namespace) }

func (s *Store) Volume(ctx context.Context, id model.VolumeID) (model.Volume, error) {
	return get[model.Volume](ctx, s, id)
}

func (s *Store) ListVolumes(ctx context.Context, after model.VolumeID, limit int) ([]model.Volume, error) {
	return listAfter[model.Volume](ctx, s, "volume", after.String(), limit)
}

func (s *Store) Snapshot(ctx context.Context, id model.SnapshotID) (model.Snapshot, error) {
	return get[model.Snapshot](ctx, s, id)
}

func (s *Store) ListSnapshots(ctx context.Context, after model.SnapshotID, limit int) ([]model.Snapshot, error) {
	return listAfter[model.Snapshot](ctx, s, "snapshot", after.String(), limit)
}

func (s *Store) Bucket(ctx context.Context, name model.BucketName) (model.Bucket, error) {
	return get[model.Bucket](ctx, s, name)
}

func (s *Store) ListBuckets(ctx context.Context, after model.BucketName, limit int) ([]model.Bucket, error) {
	return listAfter[model.Bucket](ctx, s, "bucket", after.String(), limit)
}

func (s *Store) BucketsNamed(ctx context.Context, name string) ([]model.Bucket, error) {
	all, err := listAfter[model.Bucket](ctx, s, "bucket", "", 0)
	if err != nil {
		return nil, err
	}
	out := all[:0]
	for _, b := range all {
		if b.Name.Name() == name {
			out = append(out, b)
		}
	}
	return out, nil
}

func (s *Store) Object(context.Context, model.ObjectPath) (model.Object, error) {
	return model.Object{}, errObjects
}

func (s *Store) ListObjects(context.Context, model.BucketName, string, model.ObjectKey, int) ([]model.Object, error) {
	return nil, errObjects
}

func (s *Store) HasObjects(context.Context, model.BucketName) (bool, error) {
	return false, errObjects
}

func (s *Store) Operation(ctx context.Context, id model.OperationID) (model.Operation, error) {
	return get[model.Operation](ctx, s, id)
}

func (s *Store) ListOperations(
	ctx context.Context,
	f store.OperationFilter,
	after model.OperationID,
	limit int,
) ([]model.Operation, error) {
	all, err := s.operations(ctx, f.NonTerminal)
	if err != nil {
		return nil, err
	}
	return store.PageOperations(all, f, after, limit), nil
}

func (s *Store) Runnable(ctx context.Context, now time.Time, limit int) ([]model.Operation, error) {
	live, err := s.operations(ctx, true)
	if err != nil {
		return nil, err
	}
	var out []model.Operation
	for _, op := range live {
		terminal, notBefore := op.Schedule()
		if terminal || notBefore.After(now) {
			continue
		}
		out = append(out, op)
	}
	sort.Slice(out, func(i, j int) bool {
		_, ai := out[i].Schedule()
		_, aj := out[j].Schedule()
		if ai.Equal(aj) {
			return out[i].ID.String() < out[j].ID.String()
		}
		return ai.Before(aj)
	})
	return cut(out, limit), nil
}

// InFlight derives the refs from the live operations, so there is no ref
// index to keep in step with them.
func (s *Store) InFlight(ctx context.Context, ref model.Ref) (bool, error) {
	if ref == nil || ref.String() == "" {
		return false, nil
	}
	live, err := s.operations(ctx, true)
	if err != nil {
		return false, err
	}
	for _, op := range live {
		if op.Terminal() || op.Work == nil {
			continue
		}
		for _, r := range op.Work.Refs() {
			if r != nil && r.String() == ref.String() {
				return true, nil
			}
		}
	}
	return false, nil
}

func (s *Store) Token(ctx context.Context, key model.TokenKey) (model.TokenRecord, error) {
	return get[model.TokenRecord](ctx, s, key)
}

// operations lists operation rows. live narrows to the ones labelled live,
// a superset of the non-terminal ones.
func (s *Store) operations(ctx context.Context, live bool) ([]model.Operation, error) {
	sel := ""
	if live {
		sel = labelLive + "=true"
	}
	recs, err := s.list(ctx, "operation", sel)
	if err != nil {
		return nil, err
	}
	out := make([]model.Operation, 0, len(recs))
	for _, rec := range recs {
		op, err := store.Cast[model.Operation](rec)
		if err != nil {
			return nil, err
		}
		out = append(out, op)
	}
	return out, nil
}

func get[T model.Record](ctx context.Context, s *Store, key model.Key) (T, error) {
	var zero T
	rec, _, err := s.fetch(ctx, key)
	if err != nil {
		return zero, err
	}
	if rec == nil {
		return zero, model.ErrNotFound
	}
	return store.Cast[T](rec)
}

func listAfter[T model.Record](ctx context.Context, s *Store, kind, after string, limit int) ([]T, error) {
	recs, err := s.list(ctx, kind, "")
	if err != nil {
		return nil, err
	}
	out := make([]T, 0, len(recs))
	for _, rec := range recs {
		v, err := store.Cast[T](rec)
		if err != nil {
			return nil, err
		}
		if after != "" && v.RecordKey().String() <= after {
			continue
		}
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RecordKey().String() < out[j].RecordKey().String() })
	return cut(out, limit), nil
}

// fetch returns the record under key and the ConfigMap's resourceVersion.
// A tombstone returns a nil record and its version; a missing row returns
// a nil record and "".
func (s *Store) fetch(ctx context.Context, key model.Key) (model.Record, string, error) {
	name, err := recordName(key)
	if err != nil {
		return nil, "", err
	}
	var cm *corev1.ConfigMap
	err = s.call(ctx, func(ctx context.Context) error {
		var err error
		cm, err = s.cms().Get(ctx, name, metav1.GetOptions{})
		return apiErr(err)
	})
	switch {
	case apierrors.IsNotFound(err):
		return nil, "", nil
	case err != nil:
		return nil, "", fmt.Errorf("kubestore: get %s: %w", key, err)
	}
	if cm.Labels[labelTombstone] == "true" {
		return nil, cm.ResourceVersion, nil
	}
	rec, err := decode(cm)
	return rec, cm.ResourceVersion, err
}

func (s *Store) list(ctx context.Context, kind, extra string) ([]model.Record, error) {
	sel := labelManagedBy + "=" + managedBy + "," + labelRecord + "=" + kind + "," + labelTombstone + "!=true"
	if extra != "" {
		sel += "," + extra
	}
	var out []model.Record
	cont := ""
	for {
		var page *corev1.ConfigMapList
		err := s.call(ctx, func(ctx context.Context) error {
			var err error
			page, err = s.cms().List(ctx, metav1.ListOptions{LabelSelector: sel, Limit: listPage, Continue: cont})
			return apiErr(err)
		})
		if err != nil {
			return nil, fmt.Errorf("kubestore: list %s: %w", kind, err)
		}
		for i := range page.Items {
			rec, err := decode(&page.Items[i])
			if err != nil {
				return nil, err
			}
			out = append(out, rec)
		}
		if cont = page.Continue; cont == "" {
			return out, nil
		}
	}
}

func decode(cm *corev1.ConfigMap) (model.Record, error) {
	rec, err := model.UnmarshalRecord([]byte(cm.Data[dataRecord]))
	if err != nil {
		return nil, fmt.Errorf("kubestore: decode %s: %w", cm.Name, err)
	}
	return rec, nil
}

// recordName names a record's ConfigMap. Keys hold characters a name cannot,
// so the name is a hash; the key itself is in the vws.io/key annotation.
func recordName(key model.Key) (string, error) {
	kind, err := kindOf(key)
	if err != nil {
		return "", err
	}
	if key.String() == "" {
		return "", fmt.Errorf("kubestore: empty %s key: %w", kind, model.ErrInvalidArgument)
	}
	sum := sha256.Sum256([]byte(key.String()))
	return "vws-" + kind + "-" + hex.EncodeToString(sum[:20]), nil
}

func kindOf(key model.Key) (string, error) {
	switch key.(type) {
	case model.VolumeID:
		return "volume", nil
	case model.SnapshotID:
		return "snapshot", nil
	case model.BucketName:
		return "bucket", nil
	case model.OperationID:
		return "operation", nil
	case model.TokenKey:
		return "token", nil
	case model.ObjectPath:
		return "", errObjects
	default:
		return "", fmt.Errorf("kubestore: key %T: %w", key, model.ErrInvalidArgument)
	}
}

// configMap is the row for rec, or a tombstone when rec is nil.
func configMap(w write, txn string, now time.Time) *corev1.ConfigMap {
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name:            w.Name,
		ResourceVersion: w.RV,
		Labels:          map[string]string{labelManagedBy: managedBy, labelRecord: w.Kind},
		Annotations:     map[string]string{annKey: w.Key, annTxn: txn},
	}}
	if w.Record == nil {
		cm.Labels[labelTombstone] = "true"
		cm.Annotations[annRemovedAt] = now.UTC().Format(time.RFC3339)
		return cm
	}
	if w.Live {
		cm.Labels[labelLive] = "true"
	}
	cm.Data = map[string]string{dataRecord: string(w.Record)}
	return cm
}

func cut[T any](in []T, limit int) []T {
	if limit > 0 && len(in) > limit {
		return in[:limit]
	}
	return in
}
