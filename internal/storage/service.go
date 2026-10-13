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
	"log"
	"sync"
	"sync/atomic"
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
// Project selects one project's operations. The zero project selects historical unscoped rows.
type OperationQuery struct {
	Target      model.Ref
	NonTerminal bool
	Project     model.Project
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
	queueDepth   atomic.Int64
	queueOldest  atomic.Int64 // unix nanos of the oldest non-terminal operation, 0 for none
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
	retryMax := opts.RetryMax
	if retryMax <= 0 {
		retryMax = 5 * time.Second
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
		zones: zones, now: now, policy: retryPolicy{base: base, max: retryMax, poll: poll},
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
		rec, err := r.Token(ctx, model.TokenKey{Project: req.Project(), Token: tok})
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
		st := model.Start{Op: model.NewOperationID().WithProject(req.Project()), Claim: claim, Now: s.now()}
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

// replay returns the in-flight operation, or records a succeeded operation for a fact that is already stored.
func replay(ctx context.Context, r store.Reader, st model.Start, creating model.OperationID, work model.Work, guard model.Record) (model.Admitted, error) {
	if !creating.IsZero() {
		op, err := r.Operation(ctx, creating)
		if err != nil {
			return model.Admitted{}, err
		}
		return model.NoteExisting(st, op)
	}
	return model.FinishNow(st, work, guard)
}

func expectVersion(got, want model.Version) error {
	if want != 0 && got != want {
		return fmt.Errorf("%w: etag", model.ErrETagMismatch)
	}
	return nil
}

func findOpen(ctx context.Context, r store.Reader, kind model.Kind, match func(model.Operation) bool) (model.Operation, bool, error) {
	var after model.OperationID
	for {
		ops, err := r.ListOperations(ctx, store.OperationFilter{Kind: kind, NonTerminal: true}, after, 64)
		if err != nil {
			return model.Operation{}, false, err
		}
		for _, op := range ops {
			after = op.ID
			if match(op) {
				return op, true, nil
			}
		}
		if len(ops) < 64 {
			return model.Operation{}, false, nil
		}
	}
}

func (s *Service) admit(ctx context.Context, r store.Reader, st model.Start, req model.Request) (model.Admitted, error) {
	switch req := req.(type) {
	case model.CreateVolumeRequest:
		return s.admitCreateVolume(ctx, r, st, req)
	case model.DeleteVolumeRequest:
		return s.admitDeleteVolume(ctx, r, st, req)
	case model.ExpandVolumeRequest:
		return s.admitExpandVolume(ctx, r, st, req)
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
	case model.UpdateObjectRequest:
		return s.admitUpdateObject(ctx, r, st, req)
	case model.WriteObjectRequest:
		return s.admitWriteObject(ctx, r, st, req)
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
	var matched []model.Operation
	for {
		batch, err := s.st.ListOperations(ctx, store.OperationFilter{Target: q.Target, NonTerminal: q.NonTerminal}, after, n+1)
		if err != nil {
			return model.Page[OperationStatus]{}, err
		}
		if len(batch) == 0 {
			break
		}
		stop := false
		for _, op := range batch {
			after = op.ID
			if op.ID.Project() != q.Project {
				continue
			}
			matched = append(matched, op)
			if len(matched) == n+1 {
				stop = true
				break
			}
		}
		if stop || len(batch) <= n {
			break
		}
	}
	ops, next := trim(matched, n, func(op model.Operation) string { return op.ID.String() })
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

// Attachments returns, for each volume in vols that an instance uses, the
// instance id. It is read from the backend on each call and never stored:
// attachment is owned by the instance spec. A backend without attachments, or
// one that fails to answer, reports none; the error is logged.
func (s *Service) Attachments(ctx context.Context, vols []model.Volume) map[model.VolumeID]string {
	a, ok := s.block.(backend.Attacher)
	if !ok || len(vols) == 0 {
		return nil
	}
	byClaim, err := a.Attachments(ctx)
	if err != nil {
		log.Printf("vws-storage: attachments: %v", err)
		return nil
	}
	out := map[model.VolumeID]string{}
	for _, v := range vols {
		if name := s.ClaimName(v); name != "" {
			if inst, ok := byClaim[name]; ok {
				out[v.ID] = inst
			}
		}
	}
	return out
}

// ClaimName is the backend object name for v. It is empty while v is Creating,
// because the object may not exist yet.
func (s *Service) ClaimName(v model.Volume) string {
	if _, creating := v.State.(model.Creating); creating {
		return ""
	}
	return s.block.ClaimName(v.ID)
}

// ListVolumes lists volumes in id order for one project.
// A non-zero zone keeps only that zone.
// The page token stays the engine cursor, including across volumes in other zones.
func (s *Service) ListVolumes(ctx context.Context, project model.Project, zone model.Zone, p model.PageRequest) (model.Page[model.Volume], error) {
	n, err := model.NormalizePage(p)
	if err != nil {
		return model.Page[model.Volume]{}, err
	}
	after, err := decodeID(p.Token, model.ParseVolumeID)
	if err != nil {
		return model.Page[model.Volume]{}, err
	}
	var matched []model.Volume
	for {
		batch, err := s.st.ListVolumes(ctx, after, n+1)
		if err != nil {
			return model.Page[model.Volume]{}, err
		}
		if len(batch) == 0 {
			break
		}
		stop := false
		for _, v := range batch {
			after = v.ID
			if v.ID.Project() != project {
				continue
			}
			if !zone.IsZero() && v.Spec.Zone != zone {
				continue
			}
			matched = append(matched, v)
			if len(matched) == n+1 {
				stop = true
				break
			}
		}
		if stop || len(batch) <= n {
			break
		}
	}
	items, next := trim(matched, n, func(v model.Volume) string { return v.ID.String() })
	return model.Page[model.Volume]{Items: items, NextToken: next}, nil
}

// Snapshot returns a snapshot.
func (s *Service) Snapshot(ctx context.Context, id model.SnapshotID) (model.Snapshot, error) {
	return s.st.Snapshot(ctx, id)
}

// SnapshotObjectName is the backend object name for sn. It is empty while sn is Creating.
func (s *Service) SnapshotObjectName(sn model.Snapshot) string {
	if _, creating := sn.State.(model.Creating); creating {
		return ""
	}
	return s.block.SnapshotObjectName(sn.ID)
}

// ListSnapshots lists snapshots in id order for one project.
// A non-zero region keeps only that region.
func (s *Service) ListSnapshots(ctx context.Context, project model.Project, region model.Region, p model.PageRequest) (model.Page[model.Snapshot], error) {
	n, err := model.NormalizePage(p)
	if err != nil {
		return model.Page[model.Snapshot]{}, err
	}
	after, err := decodeID(p.Token, model.ParseSnapshotID)
	if err != nil {
		return model.Page[model.Snapshot]{}, err
	}
	var matched []model.Snapshot
	for {
		batch, err := s.st.ListSnapshots(ctx, after, n+1)
		if err != nil {
			return model.Page[model.Snapshot]{}, err
		}
		if len(batch) == 0 {
			break
		}
		stop := false
		for _, sn := range batch {
			after = sn.ID
			if sn.ID.Project() != project {
				continue
			}
			if !region.IsZero() && sn.Region != region {
				continue
			}
			matched = append(matched, sn)
			if len(matched) == n+1 {
				stop = true
				break
			}
		}
		if stop || len(batch) <= n {
			break
		}
	}
	items, next := trim(matched, n, func(sn model.Snapshot) string { return sn.ID.String() })
	return model.Page[model.Snapshot]{Items: items, NextToken: next}, nil
}

// Bucket returns a bucket.
func (s *Service) Bucket(ctx context.Context, name model.BucketName) (model.Bucket, error) {
	return s.st.Bucket(ctx, name)
}

// ListBuckets lists buckets in name order for one project.
func (s *Service) ListBuckets(ctx context.Context, project model.Project, p model.PageRequest) (model.Page[model.Bucket], error) {
	n, err := model.NormalizePage(p)
	if err != nil {
		return model.Page[model.Bucket]{}, err
	}
	after, err := decodeID(p.Token, model.ParseBucketName)
	if err != nil {
		return model.Page[model.Bucket]{}, err
	}
	var matched []model.Bucket
	for {
		batch, err := s.st.ListBuckets(ctx, after, n+1)
		if err != nil {
			return model.Page[model.Bucket]{}, err
		}
		if len(batch) == 0 {
			break
		}
		stop := false
		for _, b := range batch {
			after = b.Name
			if b.Name.Project() != project {
				continue
			}
			matched = append(matched, b)
			if len(matched) == n+1 {
				stop = true
				break
			}
		}
		if stop || len(batch) <= n {
			break
		}
	}
	items, next := trim(matched, n, func(b model.Bucket) string { return b.Name.String() })
	return model.Page[model.Bucket]{Items: items, NextToken: next}, nil
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
	if _, err := s.st.Bucket(ctx, b); err != nil {
		return model.Page[model.Object]{}, err
	}
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

// Healthy reports whether the engine is running and the store answers. It
// reads one row and writes nothing.
func (s *Service) Healthy(ctx context.Context) error {
	s.mu.Lock()
	running := s.running
	s.mu.Unlock()
	if !running {
		return errors.New("storage: engine is not running")
	}
	if _, err := s.st.Runnable(ctx, s.now(), 1); err != nil {
		return fmt.Errorf("storage: store: %w", err)
	}
	return nil
}
