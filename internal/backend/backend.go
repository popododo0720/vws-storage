// Package backend is the data-plane port. This module ships fakes only.
// A Ceph RBD adapter would implement Block. A Ceph RGW adapter would
// implement Object and Signer. Neither lives in this repository.
package backend

import (
	"context"
	"errors"
	"time"

	"github.com/popododo0720/vws-storage/internal/model"
)

// Block is the block backend.
// Create calls are safe to repeat for the same id and the same arguments.
// A repeat with different arguments is Permanent(ErrConflict).
// Delete of an absent id returns nil.
// An image source is a compute image name on the spec. Storage has not checked
// that the image exists, that the volume's project may use it, or that the
// volume size covers it. The adapter decides those. Permanent still drops a
// volume whose create is undone.
type Block interface {
	CreateVolume(ctx context.Context, id model.VolumeID, spec model.VolumeSpec) error
	DeleteVolume(ctx context.Context, id model.VolumeID) error
	CreateSnapshot(ctx context.Context, id model.SnapshotID, source model.VolumeID) error
	DeleteSnapshot(ctx context.Context, id model.SnapshotID) error
}

// Object is the mutating object backend.
// Bytes are addressed by ObjectRef. How a generation is stored is private
// to the adapter.
type Object interface {
	CreateBucket(ctx context.Context, name model.BucketName) error
	DeleteBucket(ctx context.Context, name model.BucketName) error
	Stat(ctx context.Context, ref model.ObjectRef) (model.ObservedObject, error)
	DeleteObject(ctx context.Context, ref model.ObjectRef) error
}

// Signer derives short-lived locators. It does not publish bytes.
// A generation is written at most once. PresignPut must reject a body whose
// length differs from size, and the target must stop working at expires.
// A locator is not valid past the expires value this call stored.
type Signer interface {
	PresignPut(ctx context.Context, ref model.ObjectRef, size model.Bytes, sha model.Digest, expires time.Time) (model.Locator, error)
	PresignGet(ctx context.Context, ref model.ObjectRef, expires time.Time) (model.Locator, error)
}

// Sentinel backend errors. Wrap with Permanent to stop retries.
var (
	ErrNotFound = errors.New("backend: not found")
	ErrConflict = errors.New("backend: exists with different arguments")
	ErrBusy     = errors.New("backend: busy")
	// ErrInvalid means the arguments can never succeed, such as an unmapped
	// volume type or a shrink.
	ErrInvalid = errors.New("backend: invalid argument")
)

type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

// Permanent marks err as not worth retrying.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err: err}
}

// IsPermanent reports whether err was marked by Permanent.
func IsPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p)
}
