package model

import (
	"fmt"
	"time"
)

// Change is an atomic set of compare-and-swap writes.
// Only transition functions in this package can put writes in a Change.
// The zero Change writes nothing.
type Change struct{ writes []Write }

// Writes returns a copy of the writes.
func (c Change) Writes() []Write { return append([]Write(nil), c.writes...) }

// IsZero reports whether c writes nothing.
func (c Change) IsZero() bool { return len(c.writes) == 0 }

// Write is one row effect. Store code switches on it.
type Write interface{ isWrite() }

// Put inserts Next when Prev is nil, or replaces Prev when the stored row matches it.
// On replace, Next's version must be Prev's version plus one.
type Put struct {
	Prev Record
	Next Record
}

// Remove deletes Prev's row when the stored row matches it.
type Remove struct{ Prev Record }

// Guard fails the change unless the stored row matches Record.
type Guard struct{ Record Record }

func (Put) isWrite()    {}
func (Remove) isWrite() {}
func (Guard) isWrite()  {}

func putNew(r Record) Write             { return Put{Next: r} }
func putUpdate(prev, next Record) Write { return Put{Prev: prev, Next: next} }
func removeRec(r Record) Write          { return Remove{Prev: r} }
func guardRec(r Record) Write           { return Guard{Record: r} }

func writes(ws ...Write) Change { return Change{writes: ws} }

func withToken(st Start, ws []Write) []Write {
	if st.Claim.Token.IsZero() {
		return ws
	}
	rec := TokenRecord{Claim: st.Claim, Op: st.Op, Meta: fresh(st.Now)}
	return append(ws, putNew(rec))
}

func newOp(st Start, w Work) Operation {
	return Operation{
		ID:    st.Op,
		Work:  w,
		State: OpRunning{},
		Claim: st.Claim,
		Meta:  fresh(st.Now),
	}
}

func succeedOp(op Operation, now time.Time) Operation {
	op.State = OpSucceeded{}
	op.Meta = bump(op.Meta, now)
	return op
}

func failOp(op Operation, cause Status, now time.Time) Operation {
	if cause.IsZero() {
		cause = Status{Code: CodeInternal, Message: "unspecified failure"}
	}
	op.State = OpFailed{Cause: cause}
	op.Meta = bump(op.Meta, now)
	return op
}

// Start is the id, claim, and time for one admission.
type Start struct {
	Op    OperationID
	Claim TokenClaim
	Now   time.Time
}

// Admitted is the change that creates an operation, plus the operation itself.
type Admitted struct {
	Change Change
	Op     Operation
}

// NoteExisting binds the caller's token to an operation that already exists.
// The operation row is not rewritten. A zero token writes nothing.
func NoteExisting(st Start, op Operation) (Admitted, error) {
	if op.ID.IsZero() {
		return Admitted{}, fmt.Errorf("%w: id", ErrInvalidArgument)
	}
	if st.Claim.Token.IsZero() {
		return Admitted{Op: op}, nil
	}
	rec := TokenRecord{Claim: st.Claim, Op: op.ID, Meta: fresh(st.Now)}
	return Admitted{Change: writes(putNew(rec)), Op: op}, nil
}

// FinishNow records a succeeded operation for a fact that is already true.
// guard, when non-nil, must still match the stored row. Run does not see it.
func FinishNow(st Start, w Work, guard Record) (Admitted, error) {
	if st.Op.IsZero() || w == nil {
		return Admitted{}, fmt.Errorf("%w: id", ErrInvalidArgument)
	}
	op := newOp(st, w)
	op.State = OpSucceeded{}
	ws := []Write{putNew(op)}
	if guard != nil {
		ws = append([]Write{guardRec(guard)}, ws...)
	}
	return Admitted{Change: writes(withToken(st, ws)...), Op: op}, nil
}

// RunningOp proves an operation was read as OpRunning with work W.
type RunningOp[W Work] struct {
	op Operation
	st OpRunning
	w  W
}

// AsRunning narrows o.
func AsRunning[W Work](o Operation) (RunningOp[W], bool) {
	st, ok := o.State.(OpRunning)
	if !ok {
		return RunningOp[W]{}, false
	}
	w, ok := o.Work.(W)
	if !ok {
		return RunningOp[W]{}, false
	}
	return RunningOp[W]{op: o, st: st, w: w}, true
}

// ID returns the operation id.
func (r RunningOp[W]) ID() OperationID { return r.op.ID }

// Work returns the typed work.
func (r RunningOp[W]) Work() W { return r.w }

// Attempts returns recorded attempts.
func (r RunningOp[W]) Attempts() int { return r.st.Attempts }

// Operation returns the record this handle was narrowed from.
func (r RunningOp[W]) Operation() Operation { return r.op }

// Retry records an inconclusive attempt.
func (r RunningOp[W]) Retry(cause Status, notBefore, now time.Time) Change {
	next := r.op
	next.State = OpRunning{Attempts: r.st.Attempts + 1, NotBefore: norm(notBefore), LastError: cause}
	next.Meta = bump(r.op.Meta, now)
	return writes(putUpdate(r.op, next))
}

// Fail marks the operation failed and does not change any other row.
// Use it when the target is not in the state this attempt expected, so the
// scheduler does not scan the same broken pair forever.
func (r RunningOp[W]) Fail(cause Status, now time.Time) Change {
	return writes(putUpdate(r.op, failOp(r.op, cause, now)))
}

// Creation is the work whose permanent failure must undo backend effects.
type Creation interface {
	VolumeCreation | SnapshotCreation | BucketCreation | ObjectUpload
	Work
}

// Compensate persists the decision to undo a creation before the operation fails.
func Compensate[W Creation](r RunningOp[W], cause Status, now time.Time) Change {
	if cause.IsZero() {
		cause = Status{Code: CodeInternal, Message: "unspecified failure"}
	}
	next := r.op
	next.State = OpCompensating{Cause: cause, Attempts: 0, NotBefore: norm(now)}
	next.Meta = bump(r.op.Meta, now)
	return writes(putUpdate(r.op, next))
}

// CompensatingOp proves an operation was read as OpCompensating with work W.
type CompensatingOp[W Creation] struct {
	op Operation
	st OpCompensating
	w  W
}

// AsCompensating narrows o.
func AsCompensating[W Creation](o Operation) (CompensatingOp[W], bool) {
	st, ok := o.State.(OpCompensating)
	if !ok {
		return CompensatingOp[W]{}, false
	}
	w, ok := o.Work.(W)
	if !ok {
		return CompensatingOp[W]{}, false
	}
	return CompensatingOp[W]{op: o, st: st, w: w}, true
}

// ID returns the operation id.
func (c CompensatingOp[W]) ID() OperationID { return c.op.ID }

// Work returns the typed work.
func (c CompensatingOp[W]) Work() W { return c.w }

// Attempts returns undo attempts.
func (c CompensatingOp[W]) Attempts() int { return c.st.Attempts }

// Cause returns the failure being undone.
func (c CompensatingOp[W]) Cause() Status { return c.st.Cause }

// Operation returns the record this handle was narrowed from.
func (c CompensatingOp[W]) Operation() Operation { return c.op }

// Retry records a failed undo attempt. The undo error is not stored:
// the operation keeps the cause that started compensation.
func (c CompensatingOp[W]) Retry(_ Status, notBefore, now time.Time) Change {
	next := c.op
	next.State = OpCompensating{Cause: c.st.Cause, Attempts: c.st.Attempts + 1, NotBefore: norm(notBefore)}
	next.Meta = bump(c.op.Meta, now)
	return writes(putUpdate(c.op, next))
}

// FinishFailed marks the operation failed and does not change any other row.
func (c CompensatingOp[W]) FinishFailed(now time.Time) Change {
	return writes(putUpdate(c.op, failOp(c.op, c.st.Cause, now)))
}

// FailOperation fails a non-terminal operation without another row change.
// The drive uses it when the work kind has no handler.
func FailOperation(op Operation, cause Status, now time.Time) (Change, error) {
	if op.Terminal() {
		return Change{}, nil
	}
	return writes(putUpdate(op, failOp(op, cause, now))), nil
}
