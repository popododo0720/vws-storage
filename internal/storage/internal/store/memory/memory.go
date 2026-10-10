// Package memory is the in-process Store.
package memory

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/storage/internal/store"
)

// Open returns an empty memory store.
func Open() store.Store {
	return &mem{rows: map[string]map[string]model.Record{}, refs: map[string]map[string]struct{}{}}
}

type mem struct {
	mu   sync.Mutex
	rows map[string]map[string]model.Record
	refs map[string]map[string]struct{}
}

func (m *mem) Close() error { return nil }

func (m *mem) Update(ctx context.Context, fn func(store.Reader) (model.Change, error)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	ch, err := fn(view{m})
	if err != nil {
		return err
	}
	// Apply mutates its table as it goes. A rejected write must leave the
	// published maps untouched, so the change lands on a copy.
	next := m.clone()
	if err := store.Apply(ctx, next, ch); err != nil {
		return err
	}
	m.rows = next.rows
	m.refs = next.refs
	return nil
}

func (m *mem) clone() *mem {
	rows := make(map[string]map[string]model.Record, len(m.rows))
	for kind, tab := range m.rows {
		cp := make(map[string]model.Record, len(tab))
		for k, rec := range tab {
			cp[k] = rec
		}
		rows[kind] = cp
	}
	refs := make(map[string]map[string]struct{}, len(m.refs))
	for ref, set := range m.refs {
		cp := make(map[string]struct{}, len(set))
		for id := range set {
			cp[id] = struct{}{}
		}
		refs[ref] = cp
	}
	return &mem{rows: rows, refs: refs}
}

func (m *mem) Volume(ctx context.Context, id model.VolumeID) (model.Volume, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return view{m}.Volume(ctx, id)
}

func (m *mem) ListVolumes(ctx context.Context, after model.VolumeID, limit int) ([]model.Volume, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return view{m}.ListVolumes(ctx, after, limit)
}

func (m *mem) Snapshot(ctx context.Context, id model.SnapshotID) (model.Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return view{m}.Snapshot(ctx, id)
}

func (m *mem) ListSnapshots(ctx context.Context, after model.SnapshotID, limit int) ([]model.Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return view{m}.ListSnapshots(ctx, after, limit)
}

func (m *mem) Bucket(ctx context.Context, name model.BucketName) (model.Bucket, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return view{m}.Bucket(ctx, name)
}

func (m *mem) ListBuckets(ctx context.Context, after model.BucketName, limit int) ([]model.Bucket, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return view{m}.ListBuckets(ctx, after, limit)
}

func (m *mem) Object(ctx context.Context, p model.ObjectPath) (model.Object, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return view{m}.Object(ctx, p)
}

func (m *mem) ListObjects(ctx context.Context, b model.BucketName, prefix string, after model.ObjectKey, limit int) ([]model.Object, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return view{m}.ListObjects(ctx, b, prefix, after, limit)
}

func (m *mem) HasObjects(ctx context.Context, b model.BucketName) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return view{m}.HasObjects(ctx, b)
}

func (m *mem) Operation(ctx context.Context, id model.OperationID) (model.Operation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return view{m}.Operation(ctx, id)
}

func (m *mem) ListOperations(ctx context.Context, f store.OperationFilter, after model.OperationID, limit int) ([]model.Operation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return view{m}.ListOperations(ctx, f, after, limit)
}

func (m *mem) Runnable(ctx context.Context, now time.Time, limit int) ([]model.Operation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return view{m}.Runnable(ctx, now, limit)
}

func (m *mem) InFlight(ctx context.Context, ref model.Ref) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return view{m}.InFlight(ctx, ref)
}

func (m *mem) Token(ctx context.Context, tok model.TokenKey) (model.TokenRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return view{m}.Token(ctx, tok)
}

type view struct{ m *mem }

func (v view) get(kind string, key model.Key) (model.Record, error) {
	rec, ok := v.m.rows[kind][idOf(key)]
	if !ok {
		return nil, model.ErrNotFound
	}
	return rec, nil
}

func (v view) Volume(_ context.Context, id model.VolumeID) (model.Volume, error) {
	rec, err := v.get("volume", id)
	if err != nil {
		return model.Volume{}, err
	}
	return rec.(model.Volume), nil
}

func (v view) ListVolumes(_ context.Context, after model.VolumeID, limit int) ([]model.Volume, error) {
	var out []model.Volume
	for _, rec := range v.m.rows["volume"] {
		vol := rec.(model.Volume)
		if after.String() != "" && vol.ID.String() <= after.String() {
			continue
		}
		out = append(out, vol)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID.String() < out[j].ID.String() })
	return cut(out, limit), nil
}

func (v view) Snapshot(_ context.Context, id model.SnapshotID) (model.Snapshot, error) {
	rec, err := v.get("snapshot", id)
	if err != nil {
		return model.Snapshot{}, err
	}
	return rec.(model.Snapshot), nil
}

func (v view) ListSnapshots(_ context.Context, after model.SnapshotID, limit int) ([]model.Snapshot, error) {
	var out []model.Snapshot
	for _, rec := range v.m.rows["snapshot"] {
		s := rec.(model.Snapshot)
		if after.String() != "" && s.ID.String() <= after.String() {
			continue
		}
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID.String() < out[j].ID.String() })
	return cut(out, limit), nil
}

func (v view) Bucket(_ context.Context, name model.BucketName) (model.Bucket, error) {
	rec, err := v.get("bucket", name)
	if err != nil {
		return model.Bucket{}, err
	}
	return rec.(model.Bucket), nil
}

func (v view) ListBuckets(_ context.Context, after model.BucketName, limit int) ([]model.Bucket, error) {
	var out []model.Bucket
	for _, rec := range v.m.rows["bucket"] {
		b := rec.(model.Bucket)
		if after.String() != "" && b.Name.String() <= after.String() {
			continue
		}
		out = append(out, b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name.String() < out[j].Name.String() })
	return cut(out, limit), nil
}

func (v view) Object(_ context.Context, p model.ObjectPath) (model.Object, error) {
	rec, err := v.get("object", p)
	if err != nil {
		return model.Object{}, err
	}
	return rec.(model.Object), nil
}

func (v view) ListObjects(_ context.Context, b model.BucketName, prefix string, after model.ObjectKey, limit int) ([]model.Object, error) {
	var out []model.Object
	for _, rec := range v.m.rows["object"] {
		o := rec.(model.Object)
		if o.Path.Bucket != b || !strings.HasPrefix(o.Path.Key.String(), prefix) {
			continue
		}
		if !after.IsZero() && o.Path.Key.String() <= after.String() {
			continue
		}
		out = append(out, o)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path.Key.String() < out[j].Path.Key.String() })
	return cut(out, limit), nil
}

func (v view) HasObjects(_ context.Context, b model.BucketName) (bool, error) {
	for _, rec := range v.m.rows["object"] {
		if rec.(model.Object).Path.Bucket == b {
			return true, nil
		}
	}
	return false, nil
}

func (v view) Operation(_ context.Context, id model.OperationID) (model.Operation, error) {
	rec, err := v.get("operation", id)
	if err != nil {
		return model.Operation{}, err
	}
	return rec.(model.Operation), nil
}

func (v view) ListOperations(_ context.Context, f store.OperationFilter, after model.OperationID, limit int) ([]model.Operation, error) {
	return store.PageOperations(v.allOps(), f, after, limit), nil
}

func (v view) Runnable(_ context.Context, now time.Time, limit int) ([]model.Operation, error) {
	var out []model.Operation
	for _, op := range v.allOps() {
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

func (v view) InFlight(_ context.Context, ref model.Ref) (bool, error) {
	if ref == nil {
		return false, nil
	}
	for id := range v.m.refs[ref.String()] {
		rec, ok := v.m.rows["operation"][id]
		if !ok {
			continue
		}
		if !rec.(model.Operation).Terminal() {
			return true, nil
		}
	}
	return false, nil
}

func (v view) Token(_ context.Context, tok model.TokenKey) (model.TokenRecord, error) {
	rec, err := v.get("token", tok)
	if err != nil {
		return model.TokenRecord{}, err
	}
	return rec.(model.TokenRecord), nil
}

func (v view) allOps() []model.Operation {
	out := make([]model.Operation, 0, len(v.m.rows["operation"]))
	for _, rec := range v.m.rows["operation"] {
		out = append(out, rec.(model.Operation))
	}
	return out
}

func (m *mem) Get(_ context.Context, key model.Key) (model.Record, error) {
	kind, err := kindOf(key)
	if err != nil {
		return nil, err
	}
	rec, ok := m.rows[kind][idOf(key)]
	if !ok {
		return nil, model.ErrNotFound
	}
	return rec, nil
}

func (m *mem) Put(_ context.Context, rec model.Record) error {
	kind, err := kindOf(rec.RecordKey())
	if err != nil {
		return err
	}
	if m.rows[kind] == nil {
		m.rows[kind] = map[string]model.Record{}
	}
	m.rows[kind][idOf(rec.RecordKey())] = rec
	return nil
}

func (m *mem) Delete(_ context.Context, key model.Key) error {
	kind, err := kindOf(key)
	if err != nil {
		return err
	}
	delete(m.rows[kind], idOf(key))
	return nil
}

func (m *mem) SetRefs(_ context.Context, id model.OperationID, refs []model.Ref) error {
	m.clearRef(id)
	for _, ref := range refs {
		if ref == nil || ref.String() == "" {
			continue
		}
		set := m.refs[ref.String()]
		if set == nil {
			set = map[string]struct{}{}
			m.refs[ref.String()] = set
		}
		set[id.String()] = struct{}{}
	}
	return nil
}

func (m *mem) ClearRefs(_ context.Context, id model.OperationID) error {
	m.clearRef(id)
	return nil
}

func (m *mem) clearRef(id model.OperationID) {
	for ref, set := range m.refs {
		delete(set, id.String())
		if len(set) == 0 {
			delete(m.refs, ref)
		}
	}
}

func idOf(key model.Key) string { return key.String() }

func kindOf(key model.Key) (string, error) {
	switch key.(type) {
	case model.VolumeID:
		return "volume", nil
	case model.SnapshotID:
		return "snapshot", nil
	case model.BucketName:
		return "bucket", nil
	case model.ObjectPath:
		return "object", nil
	case model.OperationID:
		return "operation", nil
	case model.TokenKey:
		return "token", nil
	default:
		return "", model.ErrInvalidArgument
	}
}

func cut[T any](in []T, limit int) []T {
	if limit > 0 && len(in) > limit {
		return in[:limit]
	}
	return in
}
