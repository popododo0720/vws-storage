// Package storage is the control-plane engine.
// Submit is the only mutation. Run is the only driver, including work a
// previous process left unfinished. Call Run after Open; a submitted
// operation does not progress until Run is executing.
package storage

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/popododo0720/vws-storage/internal/backend"
	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/storage/internal/store"
	"github.com/popododo0720/vws-storage/internal/storage/internal/store/memory"
	"github.com/popododo0720/vws-storage/internal/storage/internal/store/sqlite"
)

// Options configures a Service.
type Options struct {
	Store        StoreConfig
	Block        backend.Block
	Object       backend.Object
	Signer       backend.Signer
	Zones        []model.Zone
	Workers      int
	ScanInterval time.Duration
	UploadWindow time.Duration
	LocatorTTL   time.Duration
	RetryBase    time.Duration
	RetryMax     time.Duration
	UploadPoll   time.Duration
	Now          func() time.Time
}

// StoreConfig selects a store without exporting the store package.
type StoreConfig struct {
	open func(context.Context) (store.Store, error)
}

// Memory keeps state in the process.
func Memory() StoreConfig {
	return StoreConfig{open: func(context.Context) (store.Store, error) { return memory.Open(), nil }}
}

// SQLite persists to path. One service may open a given file.
func SQLite(path string) StoreConfig {
	return StoreConfig{open: func(ctx context.Context) (store.Store, error) { return sqlite.Open(ctx, path) }}
}

// OperationStatus is an operation plus a locator derived on read.
type OperationStatus struct {
	model.Operation
	Upload *model.Locator
}

// OperationQuery narrows ListOperations.
type OperationQuery struct {
	Target      model.Ref
	NonTerminal bool
}

// Service is the storage engine.
type Service struct {
	st     store.Store
	block  backend.Block
	object backend.Object
	signer backend.Signer
	zones  map[model.Zone]struct{}
	now    func() time.Time
	policy retryPolicy
	window time.Duration
	ttl    time.Duration

	workers      int
	scanInterval time.Duration
	wake         chan model.OperationID

	mu      sync.Mutex
	running bool
}

// Open opens the store. Operations do not progress until Run.
func Open(ctx context.Context, opts Options) (*Service, error) {
	if opts.Store.open == nil || opts.Block == nil || opts.Object == nil || opts.Signer == nil {
		return nil, fmt.Errorf("%w: store, block, object, and signer are required", model.ErrInvalidArgument)
	}
	st, err := opts.Store.open(ctx)
	if err != nil {
		return nil, err
	}
	workers := opts.Workers
	if workers <= 0 {
		workers = 8
	}
	scan := opts.ScanInterval
	if scan <= 0 {
		scan = time.Second
	}
	window := opts.UploadWindow
	if window <= 0 {
		window = time.Hour
	}
	ttl := opts.LocatorTTL
	if ttl <= 0 {
		ttl = 15 * time.Minute
	}
	base := opts.RetryBase
	if base <= 0 {
		base = 50 * time.Millisecond
	}
	max := opts.RetryMax
	if max <= 0 {
		max = 5 * time.Second
	}
	poll := opts.UploadPoll
	if poll <= 0 {
		poll = 200 * time.Millisecond
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	zones := make(map[model.Zone]struct{}, len(opts.Zones))
	for _, z := range opts.Zones {
		zones[z] = struct{}{}
	}
	return &Service{
		st: st, block: opts.Block, object: opts.Object, signer: opts.Signer,
		zones: zones, now: now, policy: retryPolicy{base: base, max: max, poll: poll},
		window: window, ttl: ttl, workers: workers, scanInterval: scan,
		wake: make(chan model.OperationID, workers),
	}, nil
}

// Close closes the store. Call it after Run has returned.
func (s *Service) Close() error { return s.st.Close() }

// Submit is the only mutation. The same token and request return the same
// operation. The same token and a different request return ErrTokenReused.
// A rejected precondition stores nothing.
func (s *Service) Submit(ctx context.Context, tok model.ClientToken, req model.Request) (OperationStatus, error) {
	if tok.IsZero() {
		return OperationStatus{}, fmt.Errorf("%w: client token", model.ErrInvalidArgument)
	}
	if err := req.Validate(); err != nil {
		return OperationStatus{}, err
	}
	claim, err := model.Claim(tok, req)
	if err != nil {
		return OperationStatus{}, err
	}
	var op model.Operation
	err = s.st.Update(ctx, func(r store.Reader) (model.Change, error) {
		rec, err := r.Token(ctx, tok)
		switch {
		case err == nil:
			id, err := rec.Match(claim)
			if err != nil {
				return model.Change{}, err
			}
			op, err = r.Operation(ctx, id)
			return model.Change{}, err
		case !errors.Is(err, model.ErrNotFound):
			return model.Change{}, err
		}
		st := model.Start{Op: model.NewOperationID(), Claim: claim, Now: s.now()}
		admitted, err := s.admit(ctx, r, st, req)
		if err != nil {
			return model.Change{}, err
		}
		op = admitted.Op
		return admitted.Change, nil
	})
	if err != nil {
		return OperationStatus{}, err
	}
	if !op.Terminal() {
		s.poke(op.ID)
	}
	return s.status(ctx, op)
}

func (s *Service) admit(ctx context.Context, r store.Reader, st model.Start, req model.Request) (model.Admitted, error) {
	switch req := req.(type) {
	case model.CreateVolumeRequest:
		return s.admitCreateVolume(ctx, r, st, req)
	case model.DeleteVolumeRequest:
		return s.admitDeleteVolume(ctx, r, st, req)
	case model.CreateSnapshotRequest:
		return s.admitCreateSnapshot(ctx, r, st, req)
	case model.DeleteSnapshotRequest:
		return s.admitDeleteSnapshot(ctx, r, st, req)
	case model.CreateBucketRequest:
		return s.admitCreateBucket(ctx, r, st, req)
	case model.DeleteBucketRequest:
		return s.admitDeleteBucket(ctx, r, st, req)
	case model.PutObjectRequest:
		return s.admitPutObject(ctx, r, st, req)
	case model.DeleteObjectRequest:
		return s.admitDeleteObject(ctx, r, st, req)
	default:
		return model.Admitted{}, fmt.Errorf("%w: request", model.ErrInvalidArgument)
	}
}

// Operation returns one operation.
func (s *Service) Operation(ctx context.Context, id model.OperationID) (OperationStatus, error) {
	op, err := s.st.Operation(ctx, id)
	if err != nil {
		return OperationStatus{}, err
	}
	return s.status(ctx, op)
}

// ListOperations lists operations in id order.
func (s *Service) ListOperations(ctx context.Context, q OperationQuery, p model.PageRequest) (model.Page[OperationStatus], error) {
	n, err := model.NormalizePage(p)
	if err != nil {
		return model.Page[OperationStatus]{}, err
	}
	after, err := decodeID(p.Token, model.ParseOperationID)
	if err != nil {
		return model.Page[OperationStatus]{}, err
	}
	ops, err := s.st.ListOperations(ctx, store.OperationFilter{Target: q.Target, NonTerminal: q.NonTerminal}, after, n+1)
	if err != nil {
		return model.Page[OperationStatus]{}, err
	}
	ops, next := trim(ops, n, func(op model.Operation) string { return op.ID.String() })
	items := make([]OperationStatus, 0, len(ops))
	for _, op := range ops {
		st, err := s.status(ctx, op)
		if err != nil {
			return model.Page[OperationStatus]{}, err
		}
		items = append(items, st)
	}
	return model.Page[OperationStatus]{Items: items, NextToken: next}, nil
}

// Volume returns a volume.
func (s *Service) Volume(ctx context.Context, id model.VolumeID) (model.Volume, error) {
	return s.st.Volume(ctx, id)
}

// ListVolumes lists volumes in id order.
func (s *Service) ListVolumes(ctx context.Context, p model.PageRequest) (model.Page[model.Volume], error) {
	return page(ctx, p, model.ParseVolumeID, s.st.ListVolumes, func(v model.Volume) string { return v.ID.String() })
}

// Snapshot returns a snapshot.
func (s *Service) Snapshot(ctx context.Context, id model.SnapshotID) (model.Snapshot, error) {
	return s.st.Snapshot(ctx, id)
}

// ListSnapshots lists snapshots in id order.
func (s *Service) ListSnapshots(ctx context.Context, p model.PageRequest) (model.Page[model.Snapshot], error) {
	return page(ctx, p, model.ParseSnapshotID, s.st.ListSnapshots, func(v model.Snapshot) string { return v.ID.String() })
}

// Bucket returns a bucket.
func (s *Service) Bucket(ctx context.Context, name model.BucketName) (model.Bucket, error) {
	return s.st.Bucket(ctx, name)
}

// ListBuckets lists buckets in name order.
func (s *Service) ListBuckets(ctx context.Context, p model.PageRequest) (model.Page[model.Bucket], error) {
	return page(ctx, p, model.ParseBucketName, s.st.ListBuckets, func(v model.Bucket) string { return v.Name.String() })
}

// Object returns the committed head at p.
func (s *Service) Object(ctx context.Context, p model.ObjectPath) (model.Object, error) {
	return s.st.Object(ctx, p)
}

// LocateObject returns the head and a fresh download locator.
func (s *Service) LocateObject(ctx context.Context, p model.ObjectPath) (model.Object, model.Locator, error) {
	obj, err := s.st.Object(ctx, p)
	if err != nil {
		return model.Object{}, model.Locator{}, err
	}
	loc, err := s.signer.PresignGet(ctx, model.ObjectRef{Path: obj.Path, Generation: obj.Generation}, s.now().Add(s.ttl))
	if err != nil {
		return model.Object{}, model.Locator{}, err
	}
	return obj, loc, nil
}

// ListObjects lists committed heads in key order.
func (s *Service) ListObjects(ctx context.Context, b model.BucketName, prefix string, p model.PageRequest) (model.Page[model.Object], error) {
	n, err := model.NormalizePage(p)
	if err != nil {
		return model.Page[model.Object]{}, err
	}
	after, err := decodeID(p.Token, model.ParseObjectKey)
	if err != nil {
		return model.Page[model.Object]{}, err
	}
	items, err := s.st.ListObjects(ctx, b, prefix, after, n+1)
	if err != nil {
		return model.Page[model.Object]{}, err
	}
	items, next := trim(items, n, func(o model.Object) string { return o.Path.Key.String() })
	return model.Page[model.Object]{Items: items, NextToken: next}, nil
}

func (s *Service) status(ctx context.Context, op model.Operation) (OperationStatus, error) {
	out := OperationStatus{Operation: op}
	w, ok := op.Work.(model.ObjectUpload)
	if !ok {
		return out, nil
	}
	if _, ok := model.AsRunning[model.ObjectUpload](op); !ok || !s.now().Before(w.Deadline) {
		return out, nil
	}
	if _, err := s.object.Stat(ctx, w.Object); err == nil {
		return out, nil
	} else if !errors.Is(err, backend.ErrNotFound) {
		return OperationStatus{}, err
	}
	loc, err := s.signer.PresignPut(ctx, w.Object, w.Size, w.SHA256, w.Deadline)
	if err != nil {
		return OperationStatus{}, err
	}
	out.Upload = &loc
	return out, nil
}

func (s *Service) poke(id model.OperationID) {
	select {
	case s.wake <- id:
	default:
	}
}

func page[T any, ID any](ctx context.Context, p model.PageRequest, parse func(string) (ID, error), list func(context.Context, ID, int) ([]T, error), name func(T) string) (model.Page[T], error) {
	n, err := model.NormalizePage(p)
	if err != nil {
		return model.Page[T]{}, err
	}
	after, err := decodeID(p.Token, parse)
	if err != nil {
		return model.Page[T]{}, err
	}
	items, err := list(ctx, after, n+1)
	if err != nil {
		return model.Page[T]{}, err
	}
	items, next := trim(items, n, name)
	return model.Page[T]{Items: items, NextToken: next}, nil
}

func trim[T any](items []T, n int, name func(T) string) ([]T, string) {
	if len(items) <= n {
		return items, ""
	}
	items = items[:n]
	return items, encodeToken(name(items[len(items)-1]))
}

func decodeID[T any](token string, parse func(string) (T, error)) (T, error) {
	var zero T
	if token == "" {
		return zero, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return zero, fmt.Errorf("%w: page token", model.ErrInvalidArgument)
	}
	id, err := parse(string(raw))
	if err != nil {
		return zero, fmt.Errorf("%w: page token", model.ErrInvalidArgument)
	}
	return id, nil
}

func encodeToken(s string) string {
	if s == "" {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString([]byte(s))
}
