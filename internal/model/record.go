package model

import (
	"fmt"
	"time"
)

// Ref names a resource an operation targets or depends on.
type Ref interface {
	String() string
	isRef()
}

func (VolumeID) isRef()   {}
func (SnapshotID) isRef() {}
func (BucketName) isRef() {}
func (ObjectPath) isRef() {}
func (ObjectRef) isRef()  {}

// Key identifies one stored row. String is the store key.
type Key interface {
	String() string
	isKey()
}

func (VolumeID) isKey()    {}
func (SnapshotID) isKey()  {}
func (BucketName) isKey()  {}
func (ObjectPath) isKey()  {}
func (OperationID) isKey() {}

// Lifecycle is Creating, Available, Failed, or Deleting.
// In-flight states name the one operation driving them.
// A failed operation is recorded on the operation. Only a volume whose create
// failed permanently also keeps the failure here, as Failed.
type Lifecycle interface{ isLifecycle() }

// Creating means Op is creating the resource.
type Creating struct{ Op OperationID }

// Available means the backend has the resource.
type Available struct{}

// Failed means the backend gave up creating the resource. It keeps the
// reason and may hold backend leftovers; only a delete leaves this state.
type Failed struct{ Reason Status }

// Deleting means Op is deleting the resource. Failed is the reason the
// resource was in Failed before, zero if it was Available; a failed delete
// returns to that state.
type Deleting struct {
	Op     OperationID
	Failed Status
}

func (Creating) isLifecycle()  {}
func (Available) isLifecycle() {}
func (Failed) isLifecycle()    {}
func (Deleting) isLifecycle()  {}

// Volume is an EBS-like block volume.
type Volume struct {
	ID     VolumeID
	Spec   VolumeSpec
	Labels Labels
	State  Lifecycle
	Meta   Meta
	// Modified is the start times of the modifications that count against
	// the EC2 ModifyVolume limit (#157).
	Modified Modifications
}

// Snapshot is a point-in-time copy of a volume.
type Snapshot struct {
	ID          SnapshotID
	Source      VolumeID
	Region      Region
	Labels      Labels
	Description string
	Size        Bytes
	State       Lifecycle
	Meta        Meta
}

// Bucket is an S3-compatible bucket.
type Bucket struct {
	Name   BucketName
	Region Region
	Labels Labels
	State  Lifecycle
	Meta   Meta
}

// Object is the committed head of an object path.
// A row exists only when that generation's bytes are committed.
type Object struct {
	Path        ObjectPath
	Generation  Generation
	Size        Bytes
	ETag        ETag
	SHA256      Digest
	MD5         MD5
	ContentType string
	Labels      Labels
	UploadedAt  time.Time
	Meta        Meta
}

// Record is one stored row.
type Record interface {
	RecordKey() Key
	RecordMeta() Meta
	isRecord()
}

func (v Volume) RecordKey() Key    { return v.ID }
func (s Snapshot) RecordKey() Key  { return s.ID }
func (b Bucket) RecordKey() Key    { return b.Name }
func (o Object) RecordKey() Key    { return o.Path }
func (o Operation) RecordKey() Key { return o.ID }
func (t TokenRecord) RecordKey() Key {
	return TokenKey{Project: t.Project, Token: t.Claim.Token}
}

func (v Volume) RecordMeta() Meta      { return v.Meta }
func (s Snapshot) RecordMeta() Meta    { return s.Meta }
func (b Bucket) RecordMeta() Meta      { return b.Meta }
func (o Object) RecordMeta() Meta      { return o.Meta }
func (o Operation) RecordMeta() Meta   { return o.Meta }
func (t TokenRecord) RecordMeta() Meta { return t.Meta }

func (Volume) isRecord()      {}
func (Snapshot) isRecord()    {}
func (Bucket) isRecord()      {}
func (Object) isRecord()      {}
func (Operation) isRecord()   {}
func (TokenRecord) isRecord() {}

// Kind is the client-visible operation kind.
type Kind int

const (
	// KindUnspecified is the zero kind.
	KindUnspecified Kind = iota
	// KindCreateVolume creates a volume.
	KindCreateVolume
	// KindDeleteVolume deletes a volume.
	KindDeleteVolume
	// KindCreateSnapshot creates a snapshot.
	KindCreateSnapshot
	// KindDeleteSnapshot deletes a snapshot.
	KindDeleteSnapshot
	// KindCreateBucket creates a bucket.
	KindCreateBucket
	// KindDeleteBucket deletes a bucket.
	KindDeleteBucket
	// KindPutObject uploads an object.
	KindPutObject
	// KindDeleteObject reclaims an object's bytes.
	KindDeleteObject
	// KindExpandVolume grows a volume.
	KindExpandVolume
	// KindModifyVolume changes a volume's labels or type.
	KindModifyVolume
	kindEnd
)

// Kinds returns every valid kind. The list is derived from the sentinel.
func Kinds() []Kind {
	out := make([]Kind, 0, kindEnd-1)
	for k := KindUnspecified + 1; k < kindEnd; k++ {
		out = append(out, k)
	}
	return out
}

// String returns a stable kind name.
func (k Kind) String() string {
	switch k {
	case KindCreateVolume:
		return "create-volume"
	case KindDeleteVolume:
		return "delete-volume"
	case KindCreateSnapshot:
		return "create-snapshot"
	case KindDeleteSnapshot:
		return "delete-snapshot"
	case KindCreateBucket:
		return "create-bucket"
	case KindDeleteBucket:
		return "delete-bucket"
	case KindPutObject:
		return "put-object"
	case KindDeleteObject:
		return "delete-object"
	case KindExpandVolume:
		return "expand-volume"
	case KindModifyVolume:
		return "modify-volume"
	default:
		return ""
	}
}

// Work is what an operation does. Ids inside it are fixed at admission.
type Work interface {
	Kind() Kind
	Target() Ref
	Refs() []Ref
	isWork()
}

// VolumeCreation creates Volume from Spec.
type VolumeCreation struct {
	Volume VolumeID
	Spec   VolumeSpec
}

// VolumeDeletion deletes Volume. Zone is the zone copied at admission so the
// name can be rendered after the row is gone.
type VolumeDeletion struct {
	Volume VolumeID
	Zone   Zone
}

// VolumeExpansion grows Volume to Size. The volume row stays Available and
// takes the new size in the same write that succeeds the operation. Then,
// when set, is the labels, type and performance an UpdateVolume asked for in
// the same mask as the size (#87); they are stored in that same write, so the
// volume never shows one without the other. Nil for a size-only expansion and
// for operations written before #87.
type VolumeExpansion struct {
	Volume VolumeID
	Size   Bytes
	Then   *VolumeModification
}

// VolumeModification sets Volume's Labels, Type, Iops and Throughput. It
// touches no backend object, so it is admitted already succeeded, in the
// write that stores the new values. Iops and Throughput are the values
// stored; they are zero in operations written before #87.
type VolumeModification struct {
	Volume     VolumeID
	Labels     Labels
	Type       VolumeType
	Iops       int64
	Throughput int64
}

// SnapshotCreation snapshots Source into Snapshot.
type SnapshotCreation struct {
	Snapshot SnapshotID
	Source   VolumeID
	Region   Region
}

// SnapshotDeletion deletes Snapshot. Region is copied at admission so the
// name can be rendered after the row is gone.
type SnapshotDeletion struct {
	Snapshot SnapshotID
	Region   Region
}

// BucketCreation creates Bucket.
type BucketCreation struct{ Bucket BucketName }

// BucketDeletion deletes Bucket.
type BucketDeletion struct{ Bucket BucketName }

// ObjectContent is the comparable identity of an object's bytes and metadata.
// Two uploads are the same contents when their ObjectContent values are equal.
type ObjectContent struct {
	Size        Bytes
	SHA256      Digest
	ContentType string
	Labels      Labels
}

// Matches reports whether c is the stored head.
func (c ObjectContent) Matches(o Object) bool {
	return c.Size == o.Size && c.SHA256 == o.SHA256 && c.ContentType == o.ContentType && c.Labels == o.Labels
}

// ObjectUpload waits until Size bytes exist at Object, then commits them.
// Replaces is the head generation this upload supersedes. Zero means the path
// has no head. Replaces equal to Object.Generation is not a byte replacement.
type ObjectUpload struct {
	Object      ObjectRef
	Replaces    Generation
	Size        Bytes
	SHA256      Digest
	ContentType string
	Labels      Labels
	Deadline    time.Time
}

// Content returns the comparable contents of the upload.
func (w ObjectUpload) Content() ObjectContent {
	return ObjectContent{Size: w.Size, SHA256: w.SHA256, ContentType: w.ContentType, Labels: w.Labels}
}

// ObjectPurge deletes one generation of object bytes.
// Reclaim is set when the generation is no longer the head. A reclaim does not
// hold the object path, so a later upload or delete of the head can proceed.
type ObjectPurge struct {
	Object  ObjectRef
	Reclaim bool
}

// Kind implements Work.
func (VolumeCreation) Kind() Kind { return KindCreateVolume }

// Kind implements Work.
func (VolumeDeletion) Kind() Kind { return KindDeleteVolume }

// Kind implements Work.
func (VolumeExpansion) Kind() Kind { return KindExpandVolume }

// Kind implements Work.
func (VolumeModification) Kind() Kind { return KindModifyVolume }

// Kind implements Work.
func (SnapshotCreation) Kind() Kind { return KindCreateSnapshot }

// Kind implements Work.
func (SnapshotDeletion) Kind() Kind { return KindDeleteSnapshot }

// Kind implements Work.
func (BucketCreation) Kind() Kind { return KindCreateBucket }

// Kind implements Work.
func (BucketDeletion) Kind() Kind { return KindDeleteBucket }

// Kind implements Work.
func (ObjectUpload) Kind() Kind { return KindPutObject }

// Kind implements Work.
func (ObjectPurge) Kind() Kind { return KindDeleteObject }

// Target implements Work.
func (w VolumeCreation) Target() Ref { return w.Volume }

// Target implements Work.
func (w VolumeDeletion) Target() Ref { return w.Volume }

// Target implements Work.
func (w VolumeExpansion) Target() Ref { return w.Volume }

// Target implements Work.
func (w VolumeModification) Target() Ref { return w.Volume }

// Target implements Work.
func (w SnapshotCreation) Target() Ref { return w.Snapshot }

// Target implements Work.
func (w SnapshotDeletion) Target() Ref { return w.Snapshot }

// Target implements Work.
func (w BucketCreation) Target() Ref { return w.Bucket }

// Target implements Work.
func (w BucketDeletion) Target() Ref { return w.Bucket }

// Target implements Work.
func (w ObjectUpload) Target() Ref { return w.Object }

// Target implements Work.
func (w ObjectPurge) Target() Ref { return w.Object }

// Refs implements Work. A snapshot source is held while creating.
// An image source adds nothing: ImageName is not a Ref.
func (w VolumeCreation) Refs() []Ref {
	if _, ok := w.Spec.Source.Snapshot(); !ok {
		return []Ref{w.Volume}
	}
	id, _ := w.Spec.Source.Snapshot()
	return []Ref{w.Volume, id}
}

// Refs implements Work.
func (w VolumeDeletion) Refs() []Ref { return []Ref{w.Volume} }

// Refs implements Work.
func (w VolumeExpansion) Refs() []Ref { return []Ref{w.Volume} }

// Refs implements Work.
func (w VolumeModification) Refs() []Ref { return []Ref{w.Volume} }

// Refs implements Work.
func (w SnapshotCreation) Refs() []Ref { return []Ref{w.Snapshot, w.Source} }

// Refs implements Work.
func (w SnapshotDeletion) Refs() []Ref { return []Ref{w.Snapshot} }

// Refs implements Work.
func (w BucketCreation) Refs() []Ref { return []Ref{w.Bucket} }

// Refs implements Work.
func (w BucketDeletion) Refs() []Ref { return []Ref{w.Bucket} }

// Refs implements Work.
func (w ObjectUpload) Refs() []Ref {
	return []Ref{w.Object, w.Object.Path, w.Object.Path.Bucket}
}

// Refs implements Work.
func (w ObjectPurge) Refs() []Ref {
	if w.Reclaim {
		return []Ref{w.Object, w.Object.Path.Bucket}
	}
	return []Ref{w.Object, w.Object.Path, w.Object.Path.Bucket}
}

func (VolumeCreation) isWork()     {}
func (VolumeDeletion) isWork()     {}
func (VolumeExpansion) isWork()    {}
func (VolumeModification) isWork() {}
func (SnapshotCreation) isWork()   {}
func (SnapshotDeletion) isWork()   {}
func (BucketCreation) isWork()     {}
func (BucketDeletion) isWork()     {}
func (ObjectUpload) isWork()       {}
func (ObjectPurge) isWork()        {}

// Check reports whether obs satisfies the upload.
func (w ObjectUpload) Check(obs ObservedObject) error {
	if obs.Size != w.Size {
		return sentinelError("upload size mismatch")
	}
	if obs.ETag == "" || obs.UploadedAt.IsZero() {
		return sentinelError("incomplete upload observation")
	}
	if w.SHA256.IsZero() {
		return nil
	}
	switch {
	case !obs.SHA256.IsZero():
		if obs.SHA256 != w.SHA256 {
			return sentinelError("upload checksum mismatch")
		}
	case obs.MD5 == MD5{}:
		// No SHA-256 and an ETag that is not an MD5, such as a multipart
		// "<hex>-N" ETag. Nothing ties the bytes to the request.
		return sentinelError("upload checksum not verifiable")
	}
	// Some servers (Ceph RGW 19.2) store no SHA-256 to report. The upload
	// carried x-amz-checksum-sha256 in its signed request, the size matches, and
	// the ETag is a single-part MD5. A writer that holds the bytes compares
	// that MD5 (see server.putBytes); ReadObject checks the SHA-256 on read.
	return nil
}

// ObservedObject is what the backend reports about one generation.
type ObservedObject struct {
	Size       Bytes
	ETag       ETag
	SHA256     Digest
	MD5        MD5
	UploadedAt time.Time
}

// Code is a domain error code. It is not a gRPC code.
type Code int

const (
	// CodeUnknown is the zero code.
	CodeUnknown Code = iota
	// CodeInvalidArgument means the client sent a bad field.
	CodeInvalidArgument
	// CodeNotFound means the target does not exist.
	CodeNotFound
	// CodeFailedPrecondition means the resource is in the wrong state.
	CodeFailedPrecondition
	// CodeDeadlineExceeded means an upload missed its deadline.
	CodeDeadlineExceeded
	// CodeUnavailable means a transient dependency failure.
	CodeUnavailable
	// CodeInternal means a broken invariant.
	CodeInternal
)

// Status is a persisted error. The zero value means no error.
type Status struct {
	Code    Code
	Message string
}

// IsZero reports whether s is no error.
func (s Status) IsZero() bool { return s == Status{} }

// OpState is the durable progress of an operation.
// Phase and the client-visible error are derived from it.
type OpState interface{ isOpState() }

// OpRunning has not reached a decision. NotBefore is persisted backoff.
type OpRunning struct {
	Attempts  int
	NotBefore time.Time
	LastError Status
}

// OpCompensating has decided to fail and is undoing backend work.
// Only creation kinds enter this state.
type OpCompensating struct {
	Cause     Status
	Attempts  int
	NotBefore time.Time
}

// OpSucceeded is terminal success.
type OpSucceeded struct{}

// OpFailed is terminal failure. Cause is never zero.
type OpFailed struct{ Cause Status }

func (OpRunning) isOpState()      {}
func (OpCompensating) isOpState() {}
func (OpSucceeded) isOpState()    {}
func (OpFailed) isOpState()       {}

// Phase is the client-visible progress of an operation.
type Phase int

const (
	// PhaseUnspecified is the zero phase.
	PhaseUnspecified Phase = iota
	// PhasePending means the operation has not been attempted.
	PhasePending
	// PhaseRunning means an attempt is in progress or waiting.
	PhaseRunning
	// PhaseSucceeded is terminal success.
	PhaseSucceeded
	// PhaseFailed is terminal failure.
	PhaseFailed
)

// String returns a stable phase name.
func (p Phase) String() string {
	switch p {
	case PhasePending:
		return "pending"
	case PhaseRunning:
		return "running"
	case PhaseSucceeded:
		return "succeeded"
	case PhaseFailed:
		return "failed"
	default:
		return ""
	}
}

// Operation is one durable unit of work.
type Operation struct {
	ID    OperationID
	Work  Work
	State OpState
	Claim TokenClaim
	Meta  Meta
}

// Kind returns the work kind.
func (o Operation) Kind() Kind {
	if o.Work == nil {
		return KindUnspecified
	}
	return o.Work.Kind()
}

// Target returns the resource the work acts on.
func (o Operation) Target() Ref {
	if o.Work == nil {
		return nil
	}
	return o.Work.Target()
}

// Phase derives the client-visible phase.
func (o Operation) Phase() Phase {
	switch s := o.State.(type) {
	case OpRunning:
		if s.Attempts == 0 {
			return PhasePending
		}
		return PhaseRunning
	case OpCompensating:
		return PhaseRunning
	case OpSucceeded:
		return PhaseSucceeded
	case OpFailed:
		return PhaseFailed
	default:
		return PhaseUnspecified
	}
}

// Terminal reports whether the operation will not change again.
func (o Operation) Terminal() bool {
	p := o.Phase()
	return p == PhaseSucceeded || p == PhaseFailed
}

// Err returns the failure cause. It is zero unless the phase is failed.
func (o Operation) Err() Status {
	if f, ok := o.State.(OpFailed); ok {
		return f.Cause
	}
	return Status{}
}

// Schedule reports whether the operation should be driven, and when.
func (o Operation) Schedule() (terminal bool, notBefore time.Time) {
	switch s := o.State.(type) {
	case OpRunning:
		return false, s.NotBefore
	case OpCompensating:
		return false, s.NotBefore
	default:
		return true, time.Time{}
	}
}

// Locator is a short-lived backend address for object bytes.
// It is derived on read and never stored.
type Locator struct {
	Method    string
	URL       string
	Headers   map[string]string
	ExpiresAt time.Time
}

// PageRequest asks for one page. Token is opaque.
type PageRequest struct {
	Size  int
	Token string
}

// Page is one page of results.
type Page[T any] struct {
	Items     []T
	NextToken string
}

// NormalizePage returns the page size. Zero selects 50. Above 1000 is rejected.
func NormalizePage(p PageRequest) (int, error) {
	switch {
	case p.Size < 0 || p.Size > 1000:
		return 0, fmt.Errorf("%w: page size", ErrInvalidArgument)
	case p.Size == 0:
		return 50, nil
	default:
		return p.Size, nil
	}
}
