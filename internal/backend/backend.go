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
// Expand grows a volume to size. The same size is a no-op, a smaller size is
// Permanent(ErrInvalid), and an absent id is Permanent(ErrNotFound).
// An image source is a compute image name on the spec. The server has checked
// that the image is in the volume's project; storage has not checked that the
// image exists, that it is the owner's (an image id is not unique across
// projects), or that the volume size covers it. The adapter decides those. Permanent still drops a
// volume whose create is undone.
type Block interface {
	CreateVolume(ctx context.Context, id model.VolumeID, spec model.VolumeSpec) error
	DeleteVolume(ctx context.Context, id model.VolumeID) error
	Expand(ctx context.Context, id model.VolumeID, size model.Bytes) error
	CreateSnapshot(ctx context.Context, id model.SnapshotID, source model.VolumeID) error
	DeleteSnapshot(ctx context.Context, id model.SnapshotID) error
	// ClaimName and SnapshotObjectName are the names of the objects the
	// adapter creates for a volume and a snapshot. They are pure functions of
	// the id, so they never change and need no stored copy. Callers outside
	// storage use them to find the object.
	ClaimName(id model.VolumeID) string
	SnapshotObjectName(id model.SnapshotID) string
}

// Attacher is an optional Block capability: it reports which instance uses
// each claim. The map is claim name to instance id (the last segment of the
// instance name). A backend without it reports no attachments.
type Attacher interface {
	Attachments(ctx context.Context) (map[string]string, error)
}

// Retyper is an optional Block capability: Retype reports whether a volume
// stored as type from may be stored as type to with its object left as it is.
// It reads only configuration. A non-nil error is the reason it may not. A
// backend without it refuses every type change.
type Retyper interface {
	Retype(from, to model.VolumeType) error
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
	// ErrGaveUp means the backend retried a failing resource up to its limit
	// and stopped. The message is written by the backend for the caller.
	ErrGaveUp = errors.New("backend: gave up")
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

// MissingImageError is an image the caller cannot clone. The text is the
// client message. There is no reason field.
type MissingImageError struct{ Image model.ImageName }

func (e *MissingImageError) Error() string { return "image " + e.Image.String() + " not found" }

func (e *MissingImageError) Unwrap() error { return ErrNotFound }

func MissingImage(img model.ImageName) error {
	return Permanent(&MissingImageError{Image: img})
}
