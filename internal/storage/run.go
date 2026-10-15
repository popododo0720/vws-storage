package storage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/popododo0720/vws-lib/obs"
	"github.com/popododo0720/vws-lib/obs/report"

	"github.com/popododo0720/vws-storage/internal/backend"
	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/storage/internal/store"
)

// Run drives non-terminal operations until ctx is cancelled and the workers
// have returned. Restart recovery is this scan. There is no second path.
func (s *Service) Run(ctx context.Context) error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return errors.New("storage: run already started")
	}
	s.running = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.running = false
		s.mu.Unlock()
	}()

	inflight := map[model.OperationID]struct{}{}
	hold := map[model.OperationID]time.Time{}
	done := make(chan drove, s.workers)
	ticker := time.NewTicker(s.scanInterval)
	defer ticker.Stop()
	s.sampleQueue(ctx)
	for {
		// Sample the context once. A second Err call can observe a cancel that
		// the exit check missed and then drop ctx.Done from the select, which
		// waits forever when no worker is left to send.
		stopping := ctx.Err() != nil
		if !stopping {
			s.fill(ctx, inflight, hold, done)
		}
		if stopping && len(inflight) == 0 {
			return nil
		}
		var wake <-chan model.OperationID
		var tick <-chan time.Time
		var cancel <-chan struct{}
		if !stopping {
			wake = s.wake
			tick = ticker.C
			cancel = ctx.Done()
		}
		select {
		case <-cancel:
		case id := <-wake:
			s.start(ctx, id, inflight, hold, done)
		case d := <-done:
			delete(inflight, d.id)
			if d.err != nil && ctx.Err() == nil {
				hold[d.id] = s.now().Add(s.policy.base)
			}
		case <-tick:
			s.sampleQueue(ctx)
		}
	}
}

func (s *Service) fill(ctx context.Context, inflight map[model.OperationID]struct{}, hold map[model.OperationID]time.Time, done chan drove) {
	ops, err := s.st.Runnable(ctx, s.now(), s.workers+len(inflight))
	if err != nil {
		_ = report.Error(ctx, fmt.Errorf("storage: runnable: %w", err)) //nolint:errcheck // reported here; the next tick retries
		return
	}
	for _, op := range ops {
		if len(inflight) >= s.workers {
			return
		}
		s.start(ctx, op.ID, inflight, hold, done)
	}
}

type drove struct {
	id  model.OperationID
	err error
}

func (s *Service) start(ctx context.Context, id model.OperationID, inflight map[model.OperationID]struct{}, hold map[model.OperationID]time.Time, done chan drove) {
	if ctx.Err() != nil {
		return
	}
	if _, ok := inflight[id]; ok || len(inflight) >= s.workers {
		return
	}
	if until, ok := hold[id]; ok && s.now().Before(until) {
		return
	}
	delete(hold, id)
	inflight[id] = struct{}{}
	go func() {
		err := s.driveRecovered(ctx, id)
		if err != nil && ctx.Err() == nil {
			_ = report.Error(ctx, fmt.Errorf("storage: drive operation: %w", err), slog.String(obs.FieldResourceID, id.String())) //nolint:errcheck // reported here; the operation is held and retried
		}
		done <- drove{id: id, err: err}
	}()
}

// driveRecovered turns a panic in an operation into an error, so one bad
// operation backs off and retries instead of ending the process.
func (s *Service) driveRecovered(ctx context.Context, id model.OperationID) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("storage: panic driving operation: %v", r)
		}
	}()
	return s.drive(ctx, id)
}

func (s *Service) drive(ctx context.Context, id model.OperationID) error {
	op, err := s.st.Operation(ctx, id)
	if err != nil {
		return err
	}
	if op.Terminal() || notDue(op, s.now()) {
		return nil
	}
	switch w := op.Work.(type) {
	case model.VolumeCreation:
		return s.runVolumeCreation(ctx, op, w)
	case model.VolumeDeletion:
		return s.runVolumeDeletion(ctx, op, w)
	case model.VolumeExpansion:
		return s.runVolumeExpansion(ctx, op, w)
	case model.SnapshotCreation:
		return s.runSnapshotCreation(ctx, op, w)
	case model.SnapshotDeletion:
		return s.runSnapshotDeletion(ctx, op, w)
	case model.BucketCreation:
		return s.runBucketCreation(ctx, op, w)
	case model.BucketDeletion:
		return s.runBucketDeletion(ctx, op, w)
	case model.ObjectUpload:
		return s.runObjectUpload(ctx, op, w)
	case model.ObjectPurge:
		return s.runObjectPurge(ctx, op, w)
	default:
		return s.settle(ctx, func(r store.Reader, now time.Time) (model.Change, error) {
			cur, err := r.Operation(ctx, id)
			if err != nil {
				return model.Change{}, err
			}
			return model.FailOperation(cur, model.Status{Code: model.CodeInternal, Message: "unknown work"}, now)
		})
	}
}

func notDue(op model.Operation, now time.Time) bool {
	switch st := op.State.(type) {
	case model.OpRunning:
		return st.NotBefore.After(now)
	case model.OpCompensating:
		return st.NotBefore.After(now)
	default:
		return true
	}
}

func (s *Service) settle(ctx context.Context, decide func(r store.Reader, now time.Time) (model.Change, error)) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("storage: %w", err)
	}
	return s.st.Update(ctx, func(r store.Reader) (model.Change, error) {
		return decide(r, s.now())
	})
}

type retryPolicy struct {
	base, max, poll time.Duration
}

func (p retryPolicy) next(attempts int, now time.Time) time.Time {
	d := p.base
	for i := 0; i < attempts; i++ {
		if d > p.max/2 {
			d = p.max
			break
		}
		d *= 2
	}
	if d > p.max {
		d = p.max
	}
	return now.Add(d)
}

func (p retryPolicy) uploadPoll(now time.Time) time.Time { return now.Add(p.poll) }

// Operation messages for backend errors. Operation.error is client-visible, so
// a backend error's text (API server replies, endpoints, paths) goes only to
// the log. ErrInvalid keeps its text: every ErrInvalid in the backends is a
// sentence this repository writes about the caller's request, such as "volume
// cannot shrink", and the caller needs it to fix the request.
const (
	msgBackendNotFound = "the backend has no such resource"
	msgBackendConflict = "the backend resource conflicts with the request"
	msgBackendBusy     = "the backend is busy; retrying"
	msgBackendFailed   = "the backend failed"
)

func statusOf(ctx context.Context, err error) model.Status {
	if err == nil {
		return model.Status{}
	}
	var missing *backend.MissingImageError
	if errors.As(err, &missing) {
		return model.Status{Code: model.CodeNotFound, Message: missing.Error()}
	}
	if errors.Is(err, backend.ErrInvalid) {
		return model.Status{Code: model.CodeInvalidArgument, Message: err.Error()}
	}
	if errors.Is(err, backend.ErrGaveUp) {
		// The backend wrote the reason for the caller (retry limit or timeout).
		return model.Status{Code: model.CodeDeadlineExceeded, Message: err.Error()}
	}
	// Not a server fault: the operation records a fixed message and retries
	// or fails. The backend text goes to the log only.
	slog.WarnContext(ctx, "backend error", slog.String(obs.FieldErr, err.Error()))
	switch {
	case errors.Is(err, backend.ErrNotFound):
		return model.Status{Code: model.CodeNotFound, Message: msgBackendNotFound}
	case errors.Is(err, backend.ErrConflict):
		return model.Status{Code: model.CodeFailedPrecondition, Message: msgBackendConflict}
	case errors.Is(err, backend.ErrBusy):
		return model.Status{Code: model.CodeUnavailable, Message: msgBackendBusy}
	default:
		return model.Status{Code: model.CodeUnavailable, Message: msgBackendFailed}
	}
}

func finish[W model.Work](
	ctx context.Context,
	s *Service,
	id model.OperationID,
	callErr error,
	onSuccess func(model.RunningOp[W], store.Reader, time.Time) (model.Change, error),
	onPermanent func(model.RunningOp[W], store.Reader, model.Status, time.Time) (model.Change, error),
) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("storage: %w", err)
	}
	return s.settle(ctx, func(r store.Reader, now time.Time) (model.Change, error) {
		cur, err := r.Operation(ctx, id)
		if err != nil {
			return model.Change{}, err
		}
		run, ok := model.AsRunning[W](cur)
		if !ok {
			return model.Change{}, nil
		}
		switch {
		case callErr == nil:
			return onSuccess(run, r, now)
		case backend.IsPermanent(callErr):
			return onPermanent(run, r, statusOf(ctx, callErr), now)
		default:
			return run.Retry(statusOf(ctx, callErr), s.policy.next(run.Attempts(), now), now), nil
		}
	})
}

func undo[W model.Creation](
	ctx context.Context,
	s *Service,
	id model.OperationID,
	undoCall func(context.Context) error,
	abandon func(store.Reader, model.CompensatingOp[W], time.Time) (model.Change, error),
) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("storage: %w", err)
	}
	undoErr := undoCall(ctx)
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("storage: %w", err)
	}
	return s.settle(ctx, func(r store.Reader, now time.Time) (model.Change, error) {
		cur, err := r.Operation(ctx, id)
		if err != nil {
			return model.Change{}, err
		}
		comp, ok := model.AsCompensating[W](cur)
		if !ok {
			return model.Change{}, nil
		}
		if undoErr != nil && !backend.IsPermanent(undoErr) {
			return comp.Retry(statusOf(ctx, undoErr), s.policy.next(comp.Attempts(), now), now), nil
		}
		// A permanent cleanup error still drops the creating row. Leaving it
		// would pin the name to an operation that will never run again.
		return abandon(r, comp, now)
	})
}
