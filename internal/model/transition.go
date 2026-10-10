package model

import (
	"fmt"
	"time"
)

// NewVolume admits a blank volume. Spec.Source must be empty.
func NewVolume(st Start, id VolumeID, req CreateVolumeRequest) (Admitted, error) {
	if _, ok := req.Spec.Source.Snapshot(); ok {
		return Admitted{}, fmt.Errorf("%w: source requires restore", ErrInvalidArgument)
	}
	if id.IsZero() || st.Op.IsZero() {
		return Admitted{}, fmt.Errorf("%w: id", ErrInvalidArgument)
	}
	vol := Volume{ID: id, Spec: req.Spec, State: Creating{Op: st.Op}, Meta: fresh(st.Now)}
	op := newOp(st, VolumeCreation{Volume: id, Spec: req.Spec})
	ws := withToken(st, []Write{putNew(vol), putNew(op)})
	return Admitted{Change: writes(ws...), Op: op}, nil
}

// CreatingVolume proves a volume was read in Creating.
type CreatingVolume struct {
	v  Volume
	st Creating
}

// AsCreating narrows v.
func (v Volume) AsCreating() (CreatingVolume, bool) {
	st, ok := v.State.(Creating)
	if !ok {
		return CreatingVolume{}, false
	}
	return CreatingVolume{v: v, st: st}, true
}

// Created settles a successful create.
func (c CreatingVolume) Created(op RunningOp[VolumeCreation], now time.Time) (Change, error) {
	if c.st.Op != op.ID() || op.Work().Volume != c.v.ID {
		return Change{}, ErrStale
	}
	next := c.v
	next.State = Available{}
	next.Meta = bump(c.v.Meta, now)
	return writes(putUpdate(c.v, next), putUpdate(op.Operation(), succeedOp(op.Operation(), now))), nil
}

// Abandoned removes a volume whose create was undone.
func (c CreatingVolume) Abandoned(op CompensatingOp[VolumeCreation], now time.Time) (Change, error) {
	if c.st.Op != op.ID() || op.Work().Volume != c.v.ID {
		return Change{}, ErrStale
	}
	return writes(removeRec(c.v), putUpdate(op.Operation(), failOp(op.Operation(), op.Cause(), now))), nil
}

// AvailableVolume proves a volume was read in Available.
type AvailableVolume struct{ v Volume }

// AsAvailable narrows v.
func (v Volume) AsAvailable() (AvailableVolume, bool) {
	if _, ok := v.State.(Available); !ok {
		return AvailableVolume{}, false
	}
	return AvailableVolume{v: v}, true
}

// Delete admits deletion of the volume.
func (a AvailableVolume) Delete(st Start) (Admitted, error) {
	if st.Op.IsZero() {
		return Admitted{}, fmt.Errorf("%w: id", ErrInvalidArgument)
	}
	next := a.v
	next.State = Deleting{Op: st.Op}
	next.Meta = bump(a.v.Meta, st.Now)
	op := newOp(st, VolumeDeletion{Volume: a.v.ID, Zone: a.v.Spec.Zone})
	ws := withToken(st, []Write{putUpdate(a.v, next), putNew(op)})
	return Admitted{Change: writes(ws...), Op: op}, nil
}

// Snapshot admits a snapshot of the volume and guards the volume row.
func (a AvailableVolume) Snapshot(st Start, id SnapshotID) (Admitted, error) {
	if id.IsZero() || st.Op.IsZero() {
		return Admitted{}, fmt.Errorf("%w: id", ErrInvalidArgument)
	}
	snap := Snapshot{
		ID: id, Source: a.v.ID, Size: a.v.Spec.Size,
		State: Creating{Op: st.Op}, Meta: fresh(st.Now),
	}
	op := newOp(st, SnapshotCreation{Snapshot: id, Source: a.v.ID})
	ws := withToken(st, []Write{guardRec(a.v), putNew(snap), putNew(op)})
	return Admitted{Change: writes(ws...), Op: op}, nil
}

// DeletingVolume proves a volume was read in Deleting.
type DeletingVolume struct {
	v  Volume
	st Deleting
}

// AsDeleting narrows v.
func (v Volume) AsDeleting() (DeletingVolume, bool) {
	st, ok := v.State.(Deleting)
	if !ok {
		return DeletingVolume{}, false
	}
	return DeletingVolume{v: v, st: st}, true
}

// Deleted removes the volume and succeeds the operation.
func (d DeletingVolume) Deleted(op RunningOp[VolumeDeletion], now time.Time) (Change, error) {
	if d.st.Op != op.ID() || op.Work().Volume != d.v.ID {
		return Change{}, ErrStale
	}
	return writes(removeRec(d.v), putUpdate(op.Operation(), succeedOp(op.Operation(), now))), nil
}

// DeleteFailed returns the volume to Available and fails the operation.
func (d DeletingVolume) DeleteFailed(op RunningOp[VolumeDeletion], cause Status, now time.Time) (Change, error) {
	if d.st.Op != op.ID() || op.Work().Volume != d.v.ID {
		return Change{}, ErrStale
	}
	next := d.v
	next.State = Available{}
	next.Meta = bump(d.v.Meta, now)
	return writes(putUpdate(d.v, next), putUpdate(op.Operation(), failOp(op.Operation(), cause, now))), nil
}

// CreatingSnapshot proves a snapshot was read in Creating.
type CreatingSnapshot struct {
	s  Snapshot
	st Creating
}

// AsCreating narrows s.
func (s Snapshot) AsCreating() (CreatingSnapshot, bool) {
	st, ok := s.State.(Creating)
	if !ok {
		return CreatingSnapshot{}, false
	}
	return CreatingSnapshot{s: s, st: st}, true
}

// Created settles a successful snapshot.
func (c CreatingSnapshot) Created(op RunningOp[SnapshotCreation], now time.Time) (Change, error) {
	if c.st.Op != op.ID() || op.Work().Snapshot != c.s.ID {
		return Change{}, ErrStale
	}
	next := c.s
	next.State = Available{}
	next.Meta = bump(c.s.Meta, now)
	return writes(putUpdate(c.s, next), putUpdate(op.Operation(), succeedOp(op.Operation(), now))), nil
}

// Abandoned removes a snapshot whose create was undone.
func (c CreatingSnapshot) Abandoned(op CompensatingOp[SnapshotCreation], now time.Time) (Change, error) {
	if c.st.Op != op.ID() || op.Work().Snapshot != c.s.ID {
		return Change{}, ErrStale
	}
	return writes(removeRec(c.s), putUpdate(op.Operation(), failOp(op.Operation(), op.Cause(), now))), nil
}

// AvailableSnapshot proves a snapshot was read in Available.
type AvailableSnapshot struct{ s Snapshot }

// AsAvailable narrows s.
func (s Snapshot) AsAvailable() (AvailableSnapshot, bool) {
	if _, ok := s.State.(Available); !ok {
		return AvailableSnapshot{}, false
	}
	return AvailableSnapshot{s: s}, true
}

// Delete admits deletion of the snapshot.
func (a AvailableSnapshot) Delete(st Start) (Admitted, error) {
	if st.Op.IsZero() {
		return Admitted{}, fmt.Errorf("%w: id", ErrInvalidArgument)
	}
	next := a.s
	next.State = Deleting{Op: st.Op}
	next.Meta = bump(a.s.Meta, st.Now)
	op := newOp(st, SnapshotDeletion{Snapshot: a.s.ID})
	ws := withToken(st, []Write{putUpdate(a.s, next), putNew(op)})
	return Admitted{Change: writes(ws...), Op: op}, nil
}

// RestoreVolume admits a volume created from this snapshot.
func (a AvailableSnapshot) RestoreVolume(st Start, id VolumeID, req CreateVolumeRequest) (Admitted, error) {
	src, ok := req.Spec.Source.Snapshot()
	if !ok || src != a.s.ID {
		return Admitted{}, fmt.Errorf("%w: source", ErrInvalidArgument)
	}
	if req.Spec.Size < a.s.Size {
		return Admitted{}, fmt.Errorf("%w: volume size", ErrInvalidArgument)
	}
	if id.IsZero() || st.Op.IsZero() {
		return Admitted{}, fmt.Errorf("%w: id", ErrInvalidArgument)
	}
	vol := Volume{ID: id, Spec: req.Spec, State: Creating{Op: st.Op}, Meta: fresh(st.Now)}
	op := newOp(st, VolumeCreation{Volume: id, Spec: req.Spec})
	ws := withToken(st, []Write{guardRec(a.s), putNew(vol), putNew(op)})
	return Admitted{Change: writes(ws...), Op: op}, nil
}

// DeletingSnapshot proves a snapshot was read in Deleting.
type DeletingSnapshot struct {
	s  Snapshot
	st Deleting
}

// AsDeleting narrows s.
func (s Snapshot) AsDeleting() (DeletingSnapshot, bool) {
	st, ok := s.State.(Deleting)
	if !ok {
		return DeletingSnapshot{}, false
	}
	return DeletingSnapshot{s: s, st: st}, true
}

// Deleted removes the snapshot and succeeds the operation.
func (d DeletingSnapshot) Deleted(op RunningOp[SnapshotDeletion], now time.Time) (Change, error) {
	if d.st.Op != op.ID() || op.Work().Snapshot != d.s.ID {
		return Change{}, ErrStale
	}
	return writes(removeRec(d.s), putUpdate(op.Operation(), succeedOp(op.Operation(), now))), nil
}

// DeleteFailed returns the snapshot to Available and fails the operation.
func (d DeletingSnapshot) DeleteFailed(op RunningOp[SnapshotDeletion], cause Status, now time.Time) (Change, error) {
	if d.st.Op != op.ID() || op.Work().Snapshot != d.s.ID {
		return Change{}, ErrStale
	}
	next := d.s
	next.State = Available{}
	next.Meta = bump(d.s.Meta, now)
	return writes(putUpdate(d.s, next), putUpdate(op.Operation(), failOp(op.Operation(), cause, now))), nil
}

// NewBucket admits a bucket.
func NewBucket(st Start, req CreateBucketRequest) (Admitted, error) {
	if req.Bucket.IsZero() || st.Op.IsZero() {
		return Admitted{}, fmt.Errorf("%w: bucket", ErrInvalidArgument)
	}
	b := Bucket{Name: req.Bucket, State: Creating{Op: st.Op}, Meta: fresh(st.Now)}
	op := newOp(st, BucketCreation(req))
	ws := withToken(st, []Write{putNew(b), putNew(op)})
	return Admitted{Change: writes(ws...), Op: op}, nil
}

// CreatingBucket proves a bucket was read in Creating.
type CreatingBucket struct {
	b  Bucket
	st Creating
}

// AsCreating narrows b.
func (b Bucket) AsCreating() (CreatingBucket, bool) {
	st, ok := b.State.(Creating)
	if !ok {
		return CreatingBucket{}, false
	}
	return CreatingBucket{b: b, st: st}, true
}

// Created settles a successful bucket create.
func (c CreatingBucket) Created(op RunningOp[BucketCreation], now time.Time) (Change, error) {
	if c.st.Op != op.ID() || op.Work().Bucket != c.b.Name {
		return Change{}, ErrStale
	}
	next := c.b
	next.State = Available{}
	next.Meta = bump(c.b.Meta, now)
	return writes(putUpdate(c.b, next), putUpdate(op.Operation(), succeedOp(op.Operation(), now))), nil
}

// Abandoned removes a bucket whose create was undone.
func (c CreatingBucket) Abandoned(op CompensatingOp[BucketCreation], now time.Time) (Change, error) {
	if c.st.Op != op.ID() || op.Work().Bucket != c.b.Name {
		return Change{}, ErrStale
	}
	return writes(removeRec(c.b), putUpdate(op.Operation(), failOp(op.Operation(), op.Cause(), now))), nil
}

// AvailableBucket proves a bucket was read in Available.
type AvailableBucket struct{ b Bucket }

// AsAvailable narrows b.
func (b Bucket) AsAvailable() (AvailableBucket, bool) {
	if _, ok := b.State.(Available); !ok {
		return AvailableBucket{}, false
	}
	return AvailableBucket{b: b}, true
}

// Delete admits deletion of the bucket.
func (a AvailableBucket) Delete(st Start) (Admitted, error) {
	if st.Op.IsZero() {
		return Admitted{}, fmt.Errorf("%w: id", ErrInvalidArgument)
	}
	next := a.b
	next.State = Deleting{Op: st.Op}
	next.Meta = bump(a.b.Meta, st.Now)
	op := newOp(st, BucketDeletion{Bucket: a.b.Name})
	ws := withToken(st, []Write{putUpdate(a.b, next), putNew(op)})
	return Admitted{Change: writes(ws...), Op: op}, nil
}

// Upload admits an object upload and guards the bucket row.
func (a AvailableBucket) Upload(st Start, gen Generation, req PutObjectRequest, deadline time.Time) (Admitted, error) {
	if gen.IsZero() || st.Op.IsZero() {
		return Admitted{}, fmt.Errorf("%w: id", ErrInvalidArgument)
	}
	if req.Path.Bucket != a.b.Name {
		return Admitted{}, fmt.Errorf("%w: bucket", ErrInvalidArgument)
	}
	ref := ObjectRef{Path: req.Path, Generation: gen}
	work := ObjectUpload{
		Object: ref, Size: req.Size, SHA256: req.SHA256,
		ContentType: req.ContentType, Deadline: norm(deadline),
	}
	op := newOp(st, work)
	ws := withToken(st, []Write{guardRec(a.b), putNew(op)})
	return Admitted{Change: writes(ws...), Op: op}, nil
}

// DeletingBucket proves a bucket was read in Deleting.
type DeletingBucket struct {
	b  Bucket
	st Deleting
}

// AsDeleting narrows b.
func (b Bucket) AsDeleting() (DeletingBucket, bool) {
	st, ok := b.State.(Deleting)
	if !ok {
		return DeletingBucket{}, false
	}
	return DeletingBucket{b: b, st: st}, true
}

// Deleted removes the bucket and succeeds the operation.
func (d DeletingBucket) Deleted(op RunningOp[BucketDeletion], now time.Time) (Change, error) {
	if d.st.Op != op.ID() || op.Work().Bucket != d.b.Name {
		return Change{}, ErrStale
	}
	return writes(removeRec(d.b), putUpdate(op.Operation(), succeedOp(op.Operation(), now))), nil
}

// DeleteFailed returns the bucket to Available and fails the operation.
func (d DeletingBucket) DeleteFailed(op RunningOp[BucketDeletion], cause Status, now time.Time) (Change, error) {
	if d.st.Op != op.ID() || op.Work().Bucket != d.b.Name {
		return Change{}, ErrStale
	}
	next := d.b
	next.State = Available{}
	next.Meta = bump(d.b.Meta, now)
	return writes(putUpdate(d.b, next), putUpdate(op.Operation(), failOp(op.Operation(), cause, now))), nil
}

// CommitUpload settles an upload whose bytes passed Check.
// head is the current object, or nil when the path has none.
// purge is the id of an internal reclaim operation when one is required.
// The caller mints purge before the store transaction so a transaction retry
// uses the same id.
func CommitUpload(op RunningOp[ObjectUpload], head *Object, obs ObservedObject, purge OperationID, now time.Time) (Change, error) {
	w := op.Work()
	if err := w.Check(obs); err != nil {
		return Change{}, fmt.Errorf("%w: %s", ErrFailedPrecondition, err.Error())
	}
	done := succeedOp(op.Operation(), now)
	switch {
	case head == nil:
		obj := Object{
			Path: w.Object.Path, Generation: w.Object.Generation, Size: obs.Size,
			ETag: obs.ETag, SHA256: obs.SHA256, ContentType: w.ContentType,
			UploadedAt: norm(obs.UploadedAt), Meta: fresh(now),
		}
		return writes(putNew(obj), putUpdate(op.Operation(), done)), nil
	case head.Generation == w.Object.Generation:
		return writes(guardRec(*head), putUpdate(op.Operation(), done)), nil
	case !head.UploadedAt.After(obs.UploadedAt):
		next := *head
		next.Generation = w.Object.Generation
		next.Size = obs.Size
		next.ETag = obs.ETag
		next.SHA256 = obs.SHA256
		next.ContentType = w.ContentType
		next.UploadedAt = norm(obs.UploadedAt)
		next.Meta = bump(head.Meta, now)
		old := ObjectRef{Path: head.Path, Generation: head.Generation}
		reclaim := internalOp(purge, ObjectPurge{Object: old}, now)
		return writes(putUpdate(*head, next), putNew(reclaim), putUpdate(op.Operation(), done)), nil
	default:
		reclaim := internalOp(purge, ObjectPurge{Object: w.Object}, now)
		return writes(guardRec(*head), putNew(reclaim), putUpdate(op.Operation(), done)), nil
	}
}

// AbandonUpload fails an upload after its bytes were deleted.
func AbandonUpload(op CompensatingOp[ObjectUpload], now time.Time) (Change, error) {
	return writes(putUpdate(op.Operation(), failOp(op.Operation(), op.Cause(), now))), nil
}

// Delete admits a purge and guards the head. The row stays until Purged,
// so a failed purge does not drop the only record of the generation.
func (o Object) Delete(st Start) (Admitted, error) {
	if st.Op.IsZero() {
		return Admitted{}, fmt.Errorf("%w: id", ErrInvalidArgument)
	}
	work := ObjectPurge{Object: ObjectRef{Path: o.Path, Generation: o.Generation}}
	op := newOp(st, work)
	ws := withToken(st, []Write{guardRec(o), putNew(op)})
	return Admitted{Change: writes(ws...), Op: op}, nil
}

// Purged succeeds a purge. head is the current row, or nil when the path
// has none. The row is removed only when it is still this generation.
func Purged(op RunningOp[ObjectPurge], head *Object, now time.Time) (Change, error) {
	done := putUpdate(op.Operation(), succeedOp(op.Operation(), now))
	if head == nil || head.Generation != op.Work().Object.Generation || head.Path != op.Work().Object.Path {
		return writes(done), nil
	}
	return writes(removeRec(*head), done), nil
}

// PurgeFailed fails a purge. The bytes may remain; the operation records why.
func PurgeFailed(op RunningOp[ObjectPurge], cause Status, now time.Time) (Change, error) {
	return writes(putUpdate(op.Operation(), failOp(op.Operation(), cause, now))), nil
}

func internalOp(id OperationID, w Work, now time.Time) Operation {
	return Operation{ID: id, Work: w, State: OpRunning{}, Meta: fresh(now)}
}
