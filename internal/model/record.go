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
func (ClientToken) isKey() {}

// Lifecycle is Creating, Available, or Deleting.
// In-flight states name the one operation driving them.
// Failure is recorded on the operation, not here.
type Lifecycle interface{ isLifecycle() }

// Creating means Op is creating the resource.
type Creating struct{ Op OperationID }

// Available means the backend has the resource.
type Available struct{}

// Deleting means Op is deleting the resource.
type Deleting struct{ Op OperationID }

func (Creating) isLifecycle()  {}
func (Available) isLifecycle() {}
func (Deleting) isLifecycle()  {}

// Volume is an EBS-like block volume.
type Volume struct {
	ID    VolumeID
	Spec  VolumeSpec
	State Lifecycle
	Meta  Meta
}

// Snapshot is a point-in-time copy of a volume.
type Snapshot struct {
	ID     SnapshotID
	Source VolumeID
	Size   Bytes
	State  Lifecycle
	Meta   Meta
}

// Bucket is an S3-compatible bucket.
type Bucket struct {
	Name  BucketName
	State Lifecycle
	Meta  Meta
}

// Object is the committed head of an object path.
// A row exists only when that generation's bytes are committed.
type Object struct {
	Path        ObjectPath
	Generation  Generation
	Size        Bytes
	ETag        ETag
	SHA256      Digest
	ContentType string
	UploadedAt  time.Time
	Meta        Meta
}

// Record is one stored row.
type Record interface {
	RecordKey() Key
	RecordMeta() Meta
	isRecord()
}

func (v Volume) RecordKey() Key      { return v.ID }
func (s Snapshot) RecordKey() Key    { return s.ID }
func (b Bucket) RecordKey() Key      { return b.Name }
func (o Object) RecordKey() Key      { return o.Path }
func (o Operation) RecordKey() Key   { return o.ID }
func (t TokenRecord) RecordKey() Key { return t.Claim.Token }

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

// VolumeDeletion deletes Volume.
type VolumeDeletion struct{ Volume VolumeID }

// SnapshotCreation snapshots Source into Snapshot.
type SnapshotCreation struct {
	Snapshot SnapshotID
	Source   VolumeID
}

// SnapshotDeletion deletes Snapshot.
type SnapshotDeletion struct{ Snapshot SnapshotID }

// BucketCreation creates Bucket.
type BucketCreation struct{ Bucket BucketName }

// BucketDeletion deletes Bucket.
type BucketDeletion struct{ Bucket BucketName }

// ObjectUpload waits until Size bytes exist at Object, then commits them.
type ObjectUpload struct {
	Object      ObjectRef
	Size        Bytes
	SHA256      Digest
	ContentType string
	Deadline    time.Time
}

// ObjectPurge deletes one generation of object bytes.
type ObjectPurge struct{ Object ObjectRef }

// Kind implements Work.
func (VolumeCreation) Kind() Kind { return KindCreateVolume }

// Kind implements Work.
func (VolumeDeletion) Kind() Kind { return KindDeleteVolume }

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

// Refs implements Work.
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
	return []Ref{w.Object, w.Object.Path, w.Object.Path.Bucket}
}

func (VolumeCreation) isWork()   {}
func (VolumeDeletion) isWork()   {}
func (SnapshotCreation) isWork() {}
func (SnapshotDeletion) isWork() {}
func (BucketCreation) isWork()   {}
func (BucketDeletion) isWork()   {}
func (ObjectUpload) isWork()     {}
func (ObjectPurge) isWork()      {}

// Check reports whether obs satisfies the upload.
func (w ObjectUpload) Check(obs ObservedObject) error {
	if obs.Size != w.Size {
		return errSentinel("upload size mismatch")
	}
	if !w.SHA256.IsZero() && obs.SHA256 != w.SHA256 {
		return errSentinel("upload checksum mismatch")
	}
	if obs.ETag == "" || obs.UploadedAt.IsZero() {
		return errSentinel("incomplete upload observation")
	}
	return nil
}

// ObservedObject is what the backend reports about one generation.
type ObservedObject struct {
	Size       Bytes
	ETag       ETag
	SHA256     Digest
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
