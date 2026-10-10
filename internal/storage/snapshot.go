package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/storage/internal/store"
)

func (s *Service) admitCreateSnapshot(ctx context.Context, r store.Reader, st model.Start, req model.CreateSnapshotRequest) (model.Admitted, error) {
	if !req.ID.IsZero() {
		snap, err := r.Snapshot(ctx, req.ID)
		switch {
		case err == nil:
			if _, del := snap.State.(model.Deleting); del || snap.Source != req.Source {
				return model.Admitted{}, fmt.Errorf("%w: snapshot", model.ErrAlreadyExists)
			}
			var creating model.OperationID
			if c, ok := snap.State.(model.Creating); ok {
				creating = c.Op
			}
			return replay(ctx, r, st, creating, model.SnapshotCreation{Snapshot: snap.ID, Source: snap.Source}, snap)
		case !errors.Is(err, model.ErrNotFound):
			return model.Admitted{}, err
		}
	}
	vol, err := r.Volume(ctx, req.Source)
	if err != nil {
		return model.Admitted{}, err
	}
	if !req.Zone.IsZero() && vol.Spec.Zone != req.Zone {
		return model.Admitted{}, fmt.Errorf("%w: volume", model.ErrNotFound)
	}
	avail, ok := vol.AsAvailable()
	if !ok {
		return model.Admitted{}, fmt.Errorf("%w: volume is not available", model.ErrFailedPrecondition)
	}
	busy, err := r.InFlight(ctx, req.Source)
	if err != nil {
		return model.Admitted{}, err
	}
	if busy {
		return model.Admitted{}, fmt.Errorf("%w: volume has operations in flight", model.ErrFailedPrecondition)
	}
	id := req.ID
	if id.IsZero() {
		id = model.NewSnapshotID()
	}
	return avail.Snapshot(st, id)
}

func (s *Service) admitDeleteSnapshot(ctx context.Context, r store.Reader, st model.Start, req model.DeleteSnapshotRequest) (model.Admitted, error) {
	snap, err := r.Snapshot(ctx, req.Snapshot)
	if errors.Is(err, model.ErrNotFound) && req.AllowMissing {
		return model.FinishNow(st, model.SnapshotDeletion{Snapshot: req.Snapshot}, nil)
	}
	if err != nil {
		return model.Admitted{}, err
	}
	if del, ok := snap.State.(model.Deleting); ok {
		op, err := r.Operation(ctx, del.Op)
		if err != nil {
			return model.Admitted{}, err
		}
		return model.NoteExisting(st, op)
	}
	if err := expectVersion(snap.Meta.Version, req.Expected); err != nil {
		return model.Admitted{}, err
	}
	avail, ok := snap.AsAvailable()
	if !ok {
		return model.Admitted{}, fmt.Errorf("%w: snapshot is not available", model.ErrFailedPrecondition)
	}
	var after model.VolumeID
	for {
		vols, err := r.ListVolumes(ctx, after, 100)
		if err != nil {
			return model.Admitted{}, err
		}
		for _, vol := range vols {
			if src, ok := vol.Spec.Source.Snapshot(); ok && src == req.Snapshot {
				return model.Admitted{}, fmt.Errorf("%w: volume sources snapshot", model.ErrFailedPrecondition)
			}
			after = vol.ID
		}
		if len(vols) < 100 {
			break
		}
	}
	busy, err := r.InFlight(ctx, req.Snapshot)
	if err != nil {
		return model.Admitted{}, err
	}
	if busy {
		return model.Admitted{}, fmt.Errorf("%w: snapshot has operations in flight", model.ErrFailedPrecondition)
	}
	return avail.Delete(st)
}

func (s *Service) runSnapshotCreation(ctx context.Context, op model.Operation, w model.SnapshotCreation) error {
	if _, ok := model.AsCompensating[model.SnapshotCreation](op); ok {
		return undo(ctx, s, op.ID,
			func(ctx context.Context) error { return s.block.DeleteSnapshot(ctx, w.Snapshot) },
			func(r store.Reader, comp model.CompensatingOp[model.SnapshotCreation], now time.Time) (model.Change, error) {
				snap, err := r.Snapshot(ctx, w.Snapshot)
				if errors.Is(err, model.ErrNotFound) {
					return comp.FinishFailed(now), nil
				}
				if err != nil {
					return model.Change{}, err
				}
				creating, ok := snap.AsCreating()
				if !ok {
					return comp.FinishFailed(now), nil
				}
				return creating.Abandoned(comp, now)
			})
	}
	callErr := s.block.CreateSnapshot(ctx, w.Snapshot, w.Source)
	return finish(ctx, s, op.ID, callErr,
		func(run model.RunningOp[model.SnapshotCreation], r store.Reader, now time.Time) (model.Change, error) {
			snap, err := r.Snapshot(ctx, w.Snapshot)
			if errors.Is(err, model.ErrNotFound) {
				return model.Compensate(run, model.Status{Code: model.CodeInternal, Message: "snapshot row missing"}, now), nil
			}
			if err != nil {
				return model.Change{}, err
			}
			creating, ok := snap.AsCreating()
			if !ok {
				return run.Fail(model.Status{Code: model.CodeInternal, Message: "snapshot is not creating"}, now), nil
			}
			return creating.Created(run, now)
		},
		func(run model.RunningOp[model.SnapshotCreation], _ store.Reader, st model.Status, now time.Time) (model.Change, error) {
			return model.Compensate(run, st, now), nil
		},
	)
}

func (s *Service) runSnapshotDeletion(ctx context.Context, op model.Operation, w model.SnapshotDeletion) error {
	callErr := s.block.DeleteSnapshot(ctx, w.Snapshot)
	return finish(ctx, s, op.ID, callErr,
		func(run model.RunningOp[model.SnapshotDeletion], r store.Reader, now time.Time) (model.Change, error) {
			snap, err := r.Snapshot(ctx, w.Snapshot)
			if errors.Is(err, model.ErrNotFound) {
				return run.Fail(model.Status{Code: model.CodeInternal, Message: "snapshot row missing"}, now), nil
			}
			if err != nil {
				return model.Change{}, err
			}
			deleting, ok := snap.AsDeleting()
			if !ok {
				return run.Fail(model.Status{Code: model.CodeInternal, Message: "snapshot is not deleting"}, now), nil
			}
			return deleting.Deleted(run, now)
		},
		func(run model.RunningOp[model.SnapshotDeletion], r store.Reader, st model.Status, now time.Time) (model.Change, error) {
			snap, err := r.Snapshot(ctx, w.Snapshot)
			if errors.Is(err, model.ErrNotFound) {
				return run.Fail(st, now), nil
			}
			if err != nil {
				return model.Change{}, err
			}
			deleting, ok := snap.AsDeleting()
			if !ok {
				return run.Fail(st, now), nil
			}
			return deleting.DeleteFailed(run, st, now)
		},
	)
}
