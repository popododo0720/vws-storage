// Package store is the persistence port of the storage engine.
// Update is the only write. The package sits under internal/storage/internal
// so only the engine can import it.
package store

import (
	"context"
	"time"

	"github.com/popododo0720/vws-storage/internal/model"
)

// Store is the desired-state database.
type Store interface {
	Reader
	Update(ctx context.Context, fn func(r Reader) (model.Change, error)) error
	Close() error
}

// Reader is the read side. Missing rows return model.ErrNotFound.
// Inside Update, Reader sees the transaction. The callback must not call the
// outer Store methods; those take the same lock.
type Reader interface {
	Volume(ctx context.Context, id model.VolumeID) (model.Volume, error)
	ListVolumes(ctx context.Context, after model.VolumeID, limit int) ([]model.Volume, error)
	Snapshot(ctx context.Context, id model.SnapshotID) (model.Snapshot, error)
	ListSnapshots(ctx context.Context, after model.SnapshotID, limit int) ([]model.Snapshot, error)
	Bucket(ctx context.Context, name model.BucketName) (model.Bucket, error)
	ListBuckets(ctx context.Context, after model.BucketName, limit int) ([]model.Bucket, error)
	// BucketsNamed returns every bucket row, in any project, whose raw name
	// is name. S3 bucket names are one namespace shared by all accounts.
	BucketsNamed(ctx context.Context, name string) ([]model.Bucket, error)
	Object(ctx context.Context, p model.ObjectPath) (model.Object, error)
	ListObjects(ctx context.Context, b model.BucketName, prefix string, after model.ObjectKey, limit int) ([]model.Object, error)
	HasObjects(ctx context.Context, b model.BucketName) (bool, error)
	Operation(ctx context.Context, id model.OperationID) (model.Operation, error)
	ListOperations(ctx context.Context, f OperationFilter, after model.OperationID, limit int) ([]model.Operation, error)
	Runnable(ctx context.Context, now time.Time, limit int) ([]model.Operation, error)
	InFlight(ctx context.Context, ref model.Ref) (bool, error)
	Token(ctx context.Context, key model.TokenKey) (model.TokenRecord, error)
}

// OperationFilter narrows ListOperations. The zero value matches everything.
type OperationFilter struct {
	Target      model.Ref
	NonTerminal bool
	Kind        model.Kind
}

// Allows reports whether op matches f.
func (f OperationFilter) Allows(op model.Operation) bool {
	if f.NonTerminal && op.Terminal() {
		return false
	}
	if f.Kind != model.KindUnspecified && op.Kind() != f.Kind {
		return false
	}
	if f.Target != nil && (op.Target() == nil || f.Target.String() != op.Target().String()) {
		return false
	}
	return true
}

// Table is the row port Apply writes through.
type Table interface {
	Get(ctx context.Context, key model.Key) (model.Record, error)
	Put(ctx context.Context, rec model.Record) error
	Delete(ctx context.Context, key model.Key) error
	SetRefs(ctx context.Context, id model.OperationID, refs []model.Ref) error
	ClearRefs(ctx context.Context, id model.OperationID) error
}
