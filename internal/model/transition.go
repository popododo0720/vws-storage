package model

import (
	"fmt"
	"time"
)

// NewVolume admits a volume that needs no other local row: blank, or from a
// compute image. A snapshot source goes through AvailableSnapshot.RestoreVolume.
func NewVolume(st Start, id VolumeID, req CreateVolumeRequest) (Admitted, error) {
	if _, ok := req.Spec.Source.Snapshot(); ok {
		return Admitted{}, fmt.Errorf("%w: source requires restore", ErrInvalidArgument)
	}
	if id.IsZero() || st.Op.IsZero() {
		return Admitted{}, fmt.Errorf("%w: id", ErrInvalidArgument)
	}
	vol := Volume{ID: id, Spec: req.Spec, Labels: req.Labels, State: Creating{Op: st.Op}, Meta: fresh(st.Now)}
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

// Failed keeps a volume whose create failed permanently, in Failed with the
// cause, and fails the operation. The backend may hold leftovers of the
// attempt; DeleteVolume removes them.
func (c CreatingVolume) Failed(op RunningOp[VolumeCreation], cause Status, now time.Time) (Change, error) {
	if c.st.Op != op.ID() || op.Work().Volume != c.v.ID {
		return Change{}, ErrStale
	}
	if cause.IsZero() {
		cause = Status{Code: CodeInternal, Message: "unspecified failure"}
	}
	next := c.v
	next.State = Failed{Reason: cause}
	next.Meta = bump(c.v.Meta, now)
	return writes(putUpdate(c.v, next), putUpdate(op.Operation(), failOp(op.Operation(), cause, now))), nil
}

// FailedVolume proves a volume was read in Failed.
type FailedVolume struct {
	v  Volume
	st Failed
}

// AsFailed narrows v.
func (v Volume) AsFailed() (FailedVolume, bool) {
	st, ok := v.State.(Failed)
	if !ok {
		return FailedVolume{}, false
	}
	return FailedVolume{v: v, st: st}, true
}

// Delete admits deletion of the volume. A failed delete returns it to Failed.
func (f FailedVolume) Delete(st Start) (Admitted, error) {
	if st.Op.IsZero() {
		return Admitted{}, fmt.Errorf("%w: id", ErrInvalidArgument)
	}
	next := f.v
	next.State = Deleting{Op: st.Op, Failed: f.st.Reason}
	next.Meta = bump(f.v.Meta, st.Now)
	op := newOp(st, VolumeDeletion{Volume: f.v.ID, Zone: f.v.Spec.Zone})
	ws := withToken(st, []Write{putUpdate(f.v, next), putNew(op)})
	return Admitted{Change: writes(ws...), Op: op}, nil
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

// Expand admits growing the volume to size. The volume row is guarded and
// keeps its state until Expanded.
func (a AvailableVolume) Expand(st Start, size Bytes) (Admitted, error) {
	if st.Op.IsZero() {
		return Admitted{}, fmt.Errorf("%w: id", ErrInvalidArgument)
	}
	if size <= a.v.Spec.Size {
		return Admitted{}, fmt.Errorf("%w: volume can only grow", ErrInvalidArgument)
	}
	if err := checkSize(a.v.Spec.Type, size); err != nil {
		return Admitted{}, err
	}
	op := newOp(st, VolumeExpansion{Volume: a.v.ID, Size: size})
	ws := withToken(st, []Write{guardRec(a.v), putNew(op)})
	return Admitted{Change: writes(ws...), Op: op}, nil
}

// Expanded stores the new size and succeeds the operation. A volume already
// at least that large only succeeds the operation.
func (a AvailableVolume) Expanded(op RunningOp[VolumeExpansion], now time.Time) (Change, error) {
	if op.Work().Volume != a.v.ID {
		return Change{}, ErrStale
	}
	done := putUpdate(op.Operation(), succeedOp(op.Operation(), now))
	if a.v.Spec.Size >= op.Work().Size {
		return writes(guardRec(a.v), done), nil
	}
	next := a.v
	next.Spec.Size = op.Work().Size
	next.Meta = bump(a.v.Meta, now)
	return writes(putUpdate(a.v, next), done), nil
}

// Modify stores labels and typ on the volume and records the operation as
// succeeded in the same write: nothing outside the store changes. Values
// equal to the stored ones only record the operation. A new type re-derives
// IOPS and throughput (retypePerformance).
func (a AvailableVolume) Modify(st Start, labels Labels, typ VolumeType) (Admitted, error) {
	if st.Op.IsZero() {
		return Admitted{}, fmt.Errorf("%w: id", ErrInvalidArgument)
	}
	if !typ.Valid() {
		return Admitted{}, fmt.Errorf("%w: volume type", ErrInvalidArgument)
	}
	w := VolumeModification{Volume: a.v.ID, Labels: labels, Type: typ}
	if a.v.Labels == labels && a.v.Spec.Type == typ {
		return FinishNow(st, w, a.v)
	}
	next := a.v
	if typ != a.v.Spec.Type {
		if err := checkSize(typ, a.v.Spec.Size); err != nil {
			return Admitted{}, err
		}
		iops, mibs, err := retypePerformance(typ, a.v.Spec.Size)
		if err != nil {
			return Admitted{}, err
		}
		next.Spec.Iops, next.Spec.Throughput = iops, mibs
	}
	op := newOp(st, w)
	op.State = OpSucceeded{}
	next.Labels = labels
	next.Spec.Type = typ
	next.Meta = bump(a.v.Meta, st.Now)
	ws := withToken(st, []Write{putUpdate(a.v, next), putNew(op)})
	return Admitted{Change: writes(ws...), Op: op}, nil
}

// Snapshot admits a snapshot of the volume and guards the volume row.
func (a AvailableVolume) Snapshot(st Start, id SnapshotID, region Region, labels Labels, description string) (Admitted, error) {
	if id.IsZero() || st.Op.IsZero() {
		return Admitted{}, fmt.Errorf("%w: id", ErrInvalidArgument)
	}
	snap := Snapshot{
		ID: id, Source: a.v.ID, Region: region, Labels: labels, Description: description, Size: a.v.Spec.Size,
		State: Creating{Op: st.Op}, Meta: fresh(st.Now),
	}
	op := newOp(st, SnapshotCreation{Snapshot: id, Source: a.v.ID, Region: region})
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

// DeleteFailed returns the volume to the state it was deleted from,
// Available or Failed, and fails the operation.
func (d DeletingVolume) DeleteFailed(op RunningOp[VolumeDeletion], cause Status, now time.Time) (Change, error) {
	if d.st.Op != op.ID() || op.Work().Volume != d.v.ID {
		return Change{}, ErrStale
	}
	next := d.v
	next.State = Available{}
	if !d.st.Failed.IsZero() {
		next.State = Failed{Reason: d.st.Failed}
	}
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
	op := newOp(st, SnapshotDeletion{Snapshot: a.s.ID, Region: a.s.Region})
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
	vol := Volume{ID: id, Spec: req.Spec, Labels: req.Labels, State: Creating{Op: st.Op}, Meta: fresh(st.Now)}
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
	b := Bucket{Name: req.Bucket, Region: req.Region, Labels: req.Labels, State: Creating{Op: st.Op}, Meta: fresh(st.Now)}
	op := newOp(st, BucketCreation{Bucket: req.Bucket})
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
		ContentType: req.ContentType, Labels: req.Labels, Deadline: norm(deadline),
	}
	op := newOp(st, work)
	ws := withToken(st, []Write{guardRec(a.b), putNew(op)})
	return Admitted{Change: writes(ws...), Op: op}, nil
}

// Replace admits an upload that supersedes head.
// gen is the new generation. It must differ from head.Generation.
func (a AvailableBucket) Replace(st Start, head Object, gen Generation, req UpdateObjectRequest, deadline time.Time) (Admitted, error) {
	if gen.IsZero() || gen == head.Generation || st.Op.IsZero() {
		return Admitted{}, fmt.Errorf("%w: id", ErrInvalidArgument)
	}
	if !req.HasContents || req.Path != head.Path || head.Path.Bucket != a.b.Name {
		return Admitted{}, fmt.Errorf("%w: object", ErrInvalidArgument)
	}
	work := ObjectUpload{
		Object: ObjectRef{Path: req.Path, Generation: gen}, Replaces: head.Generation,
		Size: req.Size, SHA256: req.SHA256, ContentType: req.ContentType, Labels: req.Labels,
		Deadline: norm(deadline),
	}
	op := newOp(st, work)
	ws := withToken(st, []Write{guardRec(a.b), guardRec(head), putNew(op)})
	return Admitted{Change: writes(ws...), Op: op}, nil
}

// ApplyMeta changes content type and labels without a new generation.
// Equal metadata finishes immediately and leaves the etag unchanged.
func ApplyMeta(st Start, head Object, contentType string, labels Labels) (Admitted, error) {
	if st.Op.IsZero() {
		return Admitted{}, fmt.Errorf("%w: id", ErrInvalidArgument)
	}
	work := ObjectUpload{
		Object:      ObjectRef{Path: head.Path, Generation: head.Generation},
		Size:        head.Size,
		SHA256:      head.SHA256,
		ContentType: contentType,
		Labels:      labels,
	}
	if head.ContentType == contentType && head.Labels == labels {
		return FinishNow(st, work, head)
	}
	next := head
	next.ContentType = contentType
	next.Labels = labels
	next.Meta = bump(head.Meta, st.Now)
	op := newOp(st, work)
	op.State = OpSucceeded{}
	ws := withToken(st, []Write{putUpdate(head, next), putNew(op)})
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
	case head == nil && w.Replaces.IsZero():
		obj := Object{
			Path: w.Object.Path, Generation: w.Object.Generation, Size: obs.Size,
			ETag: obs.ETag, SHA256: obs.SHA256, MD5: obs.MD5, ContentType: w.ContentType, Labels: w.Labels,
			UploadedAt: norm(obs.UploadedAt), Meta: fresh(now),
		}
		return writes(putNew(obj), putUpdate(op.Operation(), done)), nil
	case head == nil:
		return abandonReplaced(op, purge, nil, now)
	case !w.Replaces.IsZero() && head.Generation == w.Replaces && head.Generation != w.Object.Generation:
		next := replacedHead(*head, w, obs, now)
		old := ObjectRef{Path: head.Path, Generation: head.Generation}
		reclaim := internalOp(purge, ObjectPurge{Object: old, Reclaim: true}, now)
		return writes(putUpdate(*head, next), putNew(reclaim), putUpdate(op.Operation(), done)), nil
	case head.Generation == w.Object.Generation:
		return writes(guardRec(*head), putUpdate(op.Operation(), done)), nil
	case !w.Replaces.IsZero():
		return abandonReplaced(op, purge, head, now)
	case !head.UploadedAt.After(obs.UploadedAt):
		next := replacedHead(*head, w, obs, now)
		old := ObjectRef{Path: head.Path, Generation: head.Generation}
		reclaim := internalOp(purge, ObjectPurge{Object: old, Reclaim: true}, now)
		return writes(putUpdate(*head, next), putNew(reclaim), putUpdate(op.Operation(), done)), nil
	default:
		reclaim := internalOp(purge, ObjectPurge{Object: w.Object, Reclaim: true}, now)
		return writes(guardRec(*head), putNew(reclaim), putUpdate(op.Operation(), done)), nil
	}
}

func replacedHead(head Object, w ObjectUpload, obs ObservedObject, now time.Time) Object {
	next := head
	next.Generation = w.Object.Generation
	next.Size = obs.Size
	next.ETag = obs.ETag
	next.SHA256 = obs.SHA256
	next.MD5 = obs.MD5
	next.ContentType = w.ContentType
	next.Labels = w.Labels
	next.UploadedAt = norm(obs.UploadedAt)
	next.Meta = bump(head.Meta, now)
	return next
}

func abandonReplaced(op RunningOp[ObjectUpload], purge OperationID, head *Object, now time.Time) (Change, error) {
	if purge.IsZero() {
		return Change{}, fmt.Errorf("%w: purge", ErrInvalidArgument)
	}
	reclaim := internalOp(purge, ObjectPurge{Object: op.Work().Object, Reclaim: true}, now)
	failed := failOp(op.Operation(), Status{Code: CodeFailedPrecondition, Message: "object generation changed"}, now)
	ws := []Write{putNew(reclaim), putUpdate(op.Operation(), failed)}
	if head != nil {
		ws = append([]Write{guardRec(*head)}, ws...)
	}
	return writes(ws...), nil
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
