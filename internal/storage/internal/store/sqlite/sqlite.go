// Package sqlite persists the store with the pure-Go modernc.org/sqlite driver.
package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/storage/internal/store"

	_ "modernc.org/sqlite" // register the pure-Go driver.
)

const schema = `
CREATE TABLE records (
  kind TEXT NOT NULL,
  key TEXT NOT NULL,
  payload BLOB NOT NULL,
  PRIMARY KEY (kind, key)
);
CREATE TABLE op_sched (
  id TEXT PRIMARY KEY,
  terminal INTEGER NOT NULL,
  not_before INTEGER NOT NULL
);
CREATE TABLE op_refs (
  ref TEXT NOT NULL,
  op TEXT NOT NULL,
  PRIMARY KEY (ref, op)
);
`

// Open opens path and migrates it to schema version 1.
// path may be relative. The parent directory is created. The connection is
// exclusive so a second process fails instead of driving the same operations.
func Open(ctx context.Context, path string) (store.Store, error) {
	// A relative Path in a file URI becomes the authority (file://data/storage.db).
	// SQLite rejects that, and the driver reports it as "SQL logic error: out of memory".
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0o750); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", sqliteDSN(abs))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &dbStore{sql: db}
	if err := s.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func sqliteDSN(path string) string {
	// Pragmas travel with the connection. EXCLUSIVE is taken by the write in migrate.
	q := url.Values{}
	q.Add("_pragma", "busy_timeout(250)")
	q.Add("_pragma", "locking_mode(EXCLUSIVE)")
	return (&url.URL{Scheme: "file", Path: path, RawQuery: q.Encode()}).String()
}

type dbStore struct {
	mu  sync.Mutex
	sql *sql.DB
	tx  *sql.Tx
}

func (s *dbStore) Close() error { return s.sql.Close() }

func (s *dbStore) migrate(ctx context.Context) error {
	// BEGIN IMMEDIATE takes the write lock. locking_mode=EXCLUSIVE keeps it
	// until the connection closes, so a second process fails here.
	if _, err := s.sql.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = s.sql.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()
	var v int
	if err := s.sql.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&v); err != nil {
		return err
	}
	switch v {
	case 1:
	case 0:
		if _, err := s.sql.ExecContext(ctx, schema); err != nil {
			return err
		}
		if _, err := s.sql.ExecContext(ctx, `PRAGMA user_version = 1`); err != nil {
			return err
		}
	default:
		return errSchema(v)
	}
	if _, err := s.sql.ExecContext(ctx, `COMMIT`); err != nil {
		return err
	}
	committed = true
	return nil
}

type schemaError int

func (e schemaError) Error() string { return "sqlite: unknown schema version" }

func errSchema(v int) error { return schemaError(v) }

func (s *dbStore) Update(ctx context.Context, fn func(store.Reader) (model.Change, error)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.sql.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	s.tx = tx
	defer func() { s.tx = nil }()
	ch, err := fn(view{s})
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := store.Apply(ctx, s, ch); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (s *dbStore) lock(fn func() error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fn()
}

func (s *dbStore) Volume(ctx context.Context, id model.VolumeID) (model.Volume, error) {
	var out model.Volume
	err := s.lock(func() error {
		var err error
		out, err = view{s}.Volume(ctx, id)
		return err
	})
	return out, err
}

func (s *dbStore) ListVolumes(ctx context.Context, after model.VolumeID, limit int) ([]model.Volume, error) {
	var out []model.Volume
	err := s.lock(func() error {
		var err error
		out, err = view{s}.ListVolumes(ctx, after, limit)
		return err
	})
	return out, err
}

func (s *dbStore) Snapshot(ctx context.Context, id model.SnapshotID) (model.Snapshot, error) {
	var out model.Snapshot
	err := s.lock(func() error {
		var err error
		out, err = view{s}.Snapshot(ctx, id)
		return err
	})
	return out, err
}

func (s *dbStore) ListSnapshots(ctx context.Context, after model.SnapshotID, limit int) ([]model.Snapshot, error) {
	var out []model.Snapshot
	err := s.lock(func() error {
		var err error
		out, err = view{s}.ListSnapshots(ctx, after, limit)
		return err
	})
	return out, err
}

func (s *dbStore) Bucket(ctx context.Context, name model.BucketName) (model.Bucket, error) {
	var out model.Bucket
	err := s.lock(func() error {
		var err error
		out, err = view{s}.Bucket(ctx, name)
		return err
	})
	return out, err
}

func (s *dbStore) ListBuckets(ctx context.Context, after model.BucketName, limit int) ([]model.Bucket, error) {
	var out []model.Bucket
	err := s.lock(func() error {
		var err error
		out, err = view{s}.ListBuckets(ctx, after, limit)
		return err
	})
	return out, err
}

func (s *dbStore) Object(ctx context.Context, p model.ObjectPath) (model.Object, error) {
	var out model.Object
	err := s.lock(func() error {
		var err error
		out, err = view{s}.Object(ctx, p)
		return err
	})
	return out, err
}

func (s *dbStore) ListObjects(ctx context.Context, b model.BucketName, prefix string, after model.ObjectKey, limit int) ([]model.Object, error) {
	var out []model.Object
	err := s.lock(func() error {
		var err error
		out, err = view{s}.ListObjects(ctx, b, prefix, after, limit)
		return err
	})
	return out, err
}

func (s *dbStore) HasObjects(ctx context.Context, b model.BucketName) (bool, error) {
	var out bool
	err := s.lock(func() error {
		var err error
		out, err = view{s}.HasObjects(ctx, b)
		return err
	})
	return out, err
}

func (s *dbStore) Operation(ctx context.Context, id model.OperationID) (model.Operation, error) {
	var out model.Operation
	err := s.lock(func() error {
		var err error
		out, err = view{s}.Operation(ctx, id)
		return err
	})
	return out, err
}

func (s *dbStore) ListOperations(ctx context.Context, f store.OperationFilter, after model.OperationID, limit int) ([]model.Operation, error) {
	var out []model.Operation
	err := s.lock(func() error {
		var err error
		out, err = view{s}.ListOperations(ctx, f, after, limit)
		return err
	})
	return out, err
}

func (s *dbStore) Runnable(ctx context.Context, now time.Time, limit int) ([]model.Operation, error) {
	var out []model.Operation
	err := s.lock(func() error {
		var err error
		out, err = view{s}.Runnable(ctx, now, limit)
		return err
	})
	return out, err
}

func (s *dbStore) InFlight(ctx context.Context, ref model.Ref) (bool, error) {
	var out bool
	err := s.lock(func() error {
		var err error
		out, err = view{s}.InFlight(ctx, ref)
		return err
	})
	return out, err
}

func (s *dbStore) Token(ctx context.Context, tok model.TokenKey) (model.TokenRecord, error) {
	var out model.TokenRecord
	err := s.lock(func() error {
		var err error
		out, err = view{s}.Token(ctx, tok)
		return err
	})
	return out, err
}

type view struct{ s *dbStore }

func (v view) Volume(ctx context.Context, id model.VolumeID) (model.Volume, error) {
	rec, err := v.s.Get(ctx, id)
	if err != nil {
		return model.Volume{}, err
	}
	return rec.(model.Volume), nil
}

func (v view) ListVolumes(ctx context.Context, after model.VolumeID, limit int) ([]model.Volume, error) {
	all, err := v.s.byKind(ctx, "volume")
	if err != nil {
		return nil, err
	}
	var out []model.Volume
	for _, rec := range all {
		vol := rec.(model.Volume)
		if after.String() != "" && vol.ID.String() <= after.String() {
			continue
		}
		out = append(out, vol)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID.String() < out[j].ID.String() })
	return cut(out, limit), nil
}

func (v view) Snapshot(ctx context.Context, id model.SnapshotID) (model.Snapshot, error) {
	rec, err := v.s.Get(ctx, id)
	if err != nil {
		return model.Snapshot{}, err
	}
	return rec.(model.Snapshot), nil
}

func (v view) ListSnapshots(ctx context.Context, after model.SnapshotID, limit int) ([]model.Snapshot, error) {
	all, err := v.s.byKind(ctx, "snapshot")
	if err != nil {
		return nil, err
	}
	var out []model.Snapshot
	for _, rec := range all {
		snap := rec.(model.Snapshot)
		if after.String() != "" && snap.ID.String() <= after.String() {
			continue
		}
		out = append(out, snap)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID.String() < out[j].ID.String() })
	return cut(out, limit), nil
}

func (v view) Bucket(ctx context.Context, name model.BucketName) (model.Bucket, error) {
	rec, err := v.s.Get(ctx, name)
	if err != nil {
		return model.Bucket{}, err
	}
	return rec.(model.Bucket), nil
}

func (v view) ListBuckets(ctx context.Context, after model.BucketName, limit int) ([]model.Bucket, error) {
	all, err := v.s.byKind(ctx, "bucket")
	if err != nil {
		return nil, err
	}
	var out []model.Bucket
	for _, rec := range all {
		b := rec.(model.Bucket)
		if after.String() != "" && b.Name.String() <= after.String() {
			continue
		}
		out = append(out, b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name.String() < out[j].Name.String() })
	return cut(out, limit), nil
}

func (v view) Object(ctx context.Context, p model.ObjectPath) (model.Object, error) {
	rec, err := v.s.Get(ctx, p)
	if err != nil {
		return model.Object{}, err
	}
	return rec.(model.Object), nil
}

func (v view) ListObjects(ctx context.Context, b model.BucketName, prefix string, after model.ObjectKey, limit int) ([]model.Object, error) {
	all, err := v.s.byKind(ctx, "object")
	if err != nil {
		return nil, err
	}
	var out []model.Object
	for _, rec := range all {
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

func (v view) HasObjects(ctx context.Context, b model.BucketName) (bool, error) {
	all, err := v.s.byKind(ctx, "object")
	if err != nil {
		return false, err
	}
	for _, rec := range all {
		if rec.(model.Object).Path.Bucket == b {
			return true, nil
		}
	}
	return false, nil
}

func (v view) Operation(ctx context.Context, id model.OperationID) (model.Operation, error) {
	rec, err := v.s.Get(ctx, id)
	if err != nil {
		return model.Operation{}, err
	}
	return rec.(model.Operation), nil
}

func (v view) ListOperations(ctx context.Context, f store.OperationFilter, after model.OperationID, limit int) ([]model.Operation, error) {
	all, err := v.ops(ctx)
	if err != nil {
		return nil, err
	}
	return store.PageOperations(all, f, after, limit), nil
}

func (v view) Runnable(ctx context.Context, now time.Time, limit int) ([]model.Operation, error) {
	if limit <= 0 {
		limit = 1000000
	}
	rows, err := v.s.query().QueryContext(ctx, `
		SELECT r.payload FROM op_sched s
		JOIN records r ON r.kind = 'operation' AND r.key = s.id
		WHERE s.terminal = 0 AND s.not_before <= ?
		ORDER BY s.not_before, s.id
		LIMIT ?`, unixMicro(now), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Operation
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		rec, err := model.UnmarshalRecord(payload)
		if err != nil {
			return nil, err
		}
		out = append(out, rec.(model.Operation))
	}
	return out, rows.Err()
}

func (v view) InFlight(ctx context.Context, ref model.Ref) (bool, error) {
	if ref == nil || ref.String() == "" {
		return false, nil
	}
	var n int
	err := v.s.query().QueryRowContext(ctx, `
		SELECT COUNT(*) FROM op_refs r
		JOIN op_sched s ON s.id = r.op
		WHERE r.ref = ? AND s.terminal = 0`, ref.String()).Scan(&n)
	return n > 0, err
}

func (v view) Token(ctx context.Context, tok model.TokenKey) (model.TokenRecord, error) {
	rec, err := v.s.Get(ctx, tok)
	if err != nil {
		return model.TokenRecord{}, err
	}
	return rec.(model.TokenRecord), nil
}

func (v view) ops(ctx context.Context) ([]model.Operation, error) {
	all, err := v.s.byKind(ctx, "operation")
	if err != nil {
		return nil, err
	}
	out := make([]model.Operation, 0, len(all))
	for _, rec := range all {
		out = append(out, rec.(model.Operation))
	}
	return out, nil
}

func (s *dbStore) Get(ctx context.Context, key model.Key) (model.Record, error) {
	kind, id, err := kindID(key)
	if err != nil {
		return nil, err
	}
	var payload []byte
	err = s.query().QueryRowContext(ctx, `SELECT payload FROM records WHERE kind = ? AND key = ?`, kind, id).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, model.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return model.UnmarshalRecord(payload)
}

func (s *dbStore) Put(ctx context.Context, rec model.Record) error {
	kind, id, err := kindID(rec.RecordKey())
	if err != nil {
		return err
	}
	payload, err := model.MarshalRecord(rec)
	if err != nil {
		return err
	}
	_, err = s.query().ExecContext(ctx, `
		INSERT INTO records(kind, key, payload) VALUES(?, ?, ?)
		ON CONFLICT(kind, key) DO UPDATE SET payload = excluded.payload`, kind, id, payload)
	if err != nil {
		return err
	}
	op, ok := rec.(model.Operation)
	if !ok {
		return nil
	}
	terminal, notBefore := op.Schedule()
	flag := 0
	if terminal {
		flag = 1
	}
	_, err = s.query().ExecContext(ctx, `
		INSERT INTO op_sched(id, terminal, not_before) VALUES(?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET terminal = excluded.terminal, not_before = excluded.not_before`,
		id, flag, unixMicro(notBefore))
	return err
}

func (s *dbStore) Delete(ctx context.Context, key model.Key) error {
	kind, id, err := kindID(key)
	if err != nil {
		return err
	}
	if _, err := s.query().ExecContext(ctx, `DELETE FROM records WHERE kind = ? AND key = ?`, kind, id); err != nil {
		return err
	}
	if kind == "operation" {
		_, err = s.query().ExecContext(ctx, `DELETE FROM op_sched WHERE id = ?`, id)
	}
	return err
}

func (s *dbStore) SetRefs(ctx context.Context, id model.OperationID, refs []model.Ref) error {
	if err := s.ClearRefs(ctx, id); err != nil {
		return err
	}
	for _, ref := range refs {
		if ref == nil || ref.String() == "" {
			continue
		}
		if _, err := s.query().ExecContext(ctx, `INSERT INTO op_refs(ref, op) VALUES(?, ?)`, ref.String(), id.String()); err != nil {
			return err
		}
	}
	return nil
}

func (s *dbStore) ClearRefs(ctx context.Context, id model.OperationID) error {
	_, err := s.query().ExecContext(ctx, `DELETE FROM op_refs WHERE op = ?`, id.String())
	return err
}

func (s *dbStore) byKind(ctx context.Context, kind string) ([]model.Record, error) {
	rows, err := s.query().QueryContext(ctx, `SELECT payload FROM records WHERE kind = ?`, kind)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Record
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		rec, err := model.UnmarshalRecord(payload)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

func (s *dbStore) query() querier {
	if s.tx != nil {
		return s.tx
	}
	return s.sql
}

type querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func kindID(key model.Key) (string, string, error) {
	if key == nil {
		return "", "", model.ErrInvalidArgument
	}
	id := key.String()
	switch key.(type) {
	case model.VolumeID:
		return "volume", id, nil
	case model.SnapshotID:
		return "snapshot", id, nil
	case model.BucketName:
		return "bucket", id, nil
	case model.ObjectPath:
		return "object", id, nil
	case model.OperationID:
		return "operation", id, nil
	case model.TokenKey:
		return "token", id, nil
	default:
		return "", "", model.ErrInvalidArgument
	}
}

func unixMicro(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UTC().UnixMicro()
}

func cut[T any](in []T, limit int) []T {
	if limit > 0 && len(in) > limit {
		return in[:limit]
	}
	return in
}
