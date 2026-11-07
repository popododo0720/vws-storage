package model

import "time"

// VolumeShape is the values EC2 ModifyVolume changes: size, type, IOPS and
// throughput. Labels are not part of it (EC2 tags are not a modification).
type VolumeShape struct {
	Size       Bytes
	Type       VolumeType
	Iops       int64
	Throughput int64
}

// IsZero reports whether s is unset, as in an operation written before #173.
func (s VolumeShape) IsZero() bool { return s == VolumeShape{} }

// Shape returns the values of s that ModifyVolume changes.
func (s VolumeSpec) Shape() VolumeShape {
	return VolumeShape{Size: s.Size, Type: s.Type, Iops: s.Iops, Throughput: s.Throughput}
}

// Shape returns the shape the volume has once the expansion is done: the new
// size, with the type and performance of Then, or the ones the volume had when
// there is no Then.
func (w VolumeExpansion) Shape() VolumeShape {
	if m := w.Then; m != nil {
		return VolumeShape{Size: w.Size, Type: m.Type, Iops: m.Iops, Throughput: m.Throughput}
	}
	return VolumeShape{Size: w.Size, Type: w.From.Type, Iops: w.From.Iops, Throughput: w.From.Throughput}
}

// Shape returns the shape the modification stores: the size the volume had and
// the new type and performance.
func (w VolumeModification) Shape() VolumeShape {
	return VolumeShape{Size: w.From.Size, Type: w.Type, Iops: w.Iops, Throughput: w.Throughput}
}

// ModificationState is the EC2 modificationState of a volume modification.
type ModificationState int

const (
	// ModificationUnspecified is the zero state.
	ModificationUnspecified ModificationState = iota
	// ModificationModifying means the change is admitted and not finished.
	ModificationModifying
	// ModificationCompleted means the new values are stored.
	ModificationCompleted
	// ModificationFailed means the change was not made.
	ModificationFailed
)

// String returns the EC2 modificationState name.
func (s ModificationState) String() string {
	switch s {
	case ModificationModifying:
		return "modifying"
	case ModificationCompleted:
		return "completed"
	case ModificationFailed:
		return "failed"
	default:
		return ""
	}
}

// VolumeModificationStatus is one volume modification, derived from its
// operation. From is zero for an operation written before #173, and To.Size is
// zero for a modify-volume operation written before it.
type VolumeModificationStatus struct {
	Op     OperationID
	Volume VolumeID
	From   VolumeShape
	To     VolumeShape
	State  ModificationState
	Start  time.Time
	// End is zero while the modification is modifying.
	End time.Time
	// Cause is the failure of a failed modification.
	Cause Status
}

// Modification reports the volume modification the operation records. Only
// expand-volume and modify-volume work is one, and only when it changes a
// value of the shape. Labels-only and no-op updates are not listed. An
// operation written before #173 has no From, so it is listed.
func (o Operation) Modification() (VolumeModificationStatus, bool) {
	out := VolumeModificationStatus{Op: o.ID, Start: o.Meta.CreateTime}
	switch w := o.Work.(type) {
	case VolumeExpansion:
		out.Volume, out.From, out.To = w.Volume, w.From, w.Shape()
	case VolumeModification:
		out.Volume, out.From, out.To = w.Volume, w.From, w.Shape()
	default:
		return VolumeModificationStatus{}, false
	}
	if !out.From.IsZero() && out.From == out.To {
		return VolumeModificationStatus{}, false
	}
	switch o.Phase() {
	case PhasePending, PhaseRunning:
		out.State = ModificationModifying
	case PhaseSucceeded:
		out.State = ModificationCompleted
	case PhaseFailed:
		out.State, out.Cause = ModificationFailed, o.Err()
	default:
		return VolumeModificationStatus{}, false
	}
	if o.Terminal() {
		out.End = o.Meta.UpdateTime
	}
	return out, true
}
