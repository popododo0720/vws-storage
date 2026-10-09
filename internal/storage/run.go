package storage

import (
	"context"
	"errors"
	"log"
	"time"

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
	done := make(chan model.OperationID, s.workers)
	ticker := time.NewTicker(s.scanInterval)
	defer ticker.Stop()
	for {
		if ctx.Err() == nil {
			s.fill(ctx, inflight, done)
		}
		if ctx.Err() != nil && len(inflight) == 0 {
			return nil
		}
		var wake <-chan model.OperationID
		var tick <-chan time.Time
		var cancel <-chan struct{}
		if ctx.Err() == nil {
			wake = s.wake
			tick = ticker.C
			cancel = ctx.Done()
		}
		select {
		case <-cancel:
		case id := <-wake:
			s.start(ctx, id, inflight, done)
		case id := <-done:
			delete(inflight, id)
		case <-tick:
		}
	}
}

func (s *Service) fill(ctx context.Context, inflight map[model.OperationID]struct{}, done chan model.OperationID) {
	ops, err := s.st.Runnable(ctx, s.now(), s.workers+len(inflight))
	if err != nil {
		log.Printf("vws-storage: runnable: %v", err)
		return
	}
	for _, op := range ops {
		if len(inflight) >= s.workers {
			return
		}
		s.start(ctx, op.ID, inflight, done)
	}
}

func (s *Service) start(ctx context.Context, id model.OperationID, inflight map[model.OperationID]struct{}, done chan model.OperationID) {
	if ctx.Err() != nil {
		return
	}
	if _, ok := inflight[id]; ok || len(inflight) >= s.workers {
		return
	}
	inflight[id] = struct{}{}
	go func() {
		if err := s.drive(ctx, id); err != nil && ctx.Err() == nil {
			log.Printf("vws-storage: operation %s: %v", id, err)
		}
		done <- id
	}()
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
		return err
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

func statusOf(err error) model.Status {
	switch {
	case err == nil:
		return model.Status{}
	case errors.Is(err, backend.ErrNotFound):
		return model.Status{Code: model.CodeNotFound, Message: err.Error()}
	case errors.Is(err, backend.ErrConflict):
		return model.Status{Code: model.CodeFailedPrecondition, Message: err.Error()}
	case errors.Is(err, backend.ErrBusy):
		return model.Status{Code: model.CodeUnavailable, Message: err.Error()}
	default:
		return model.Status{Code: model.CodeUnavailable, Message: err.Error()}
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
	if ctx.Err() != nil {
		return ctx.Err()
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
			return onPermanent(run, r, statusOf(callErr), now)
		default:
			return run.Retry(statusOf(callErr), s.policy.next(run.Attempts(), now), now), nil
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
	if ctx.Err() != nil {
		return ctx.Err()
	}
	undoErr := undoCall(ctx)
	if ctx.Err() != nil {
		return ctx.Err()
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
			return comp.Retry(statusOf(undoErr), s.policy.next(comp.Attempts(), now), now), nil
		}
		if undoErr != nil && backend.IsPermanent(undoErr) {
			return comp.FinishFailed(now), nil
		}
		return abandon(r, comp, now)
	})
}
