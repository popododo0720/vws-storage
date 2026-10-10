package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/storage/internal/store"
)

func (s *Service) admitCreateVolume(ctx context.Context, r store.Reader, st model.Start, req model.CreateVolumeRequest) (model.Admitted, error) {
	if _, ok := s.zones[req.Spec.Zone]; !ok {
		return model.Admitted{}, fmt.Errorf("%w: unknown zone", model.ErrInvalidArgument)
	}
	id := req.ID
	if !id.IsZero() {
		vol, err := r.Volume(ctx, id)
		switch {
		case err == nil:
			if _, del := vol.State.(model.Deleting); del || !sameVolumeSpec(vol.Spec, req.Spec) || vol.Labels != req.Labels {
				return model.Admitted{}, fmt.Errorf("%w: volume", model.ErrAlreadyExists)
			}
			var creating model.OperationID
			if c, ok := vol.State.(model.Creating); ok {
				creating = c.Op
			}
			return replay(ctx, r, st, creating, model.VolumeCreation{Volume: vol.ID, Spec: vol.Spec}, vol)
		case !errors.Is(err, model.ErrNotFound):
			return model.Admitted{}, err
		}
	} else {
		id = model.NewVolumeID().WithProject(req.Project())
	}
	// A blank volume and an image name need no local row. A snapshot does.
	if _, ok := req.Spec.Source.Snapshot(); !ok {
		return model.NewVolume(st, id, req)
	}
	src, _ := req.Spec.Source.Snapshot()
	snap, err := r.Snapshot(ctx, src)
	if err != nil {
		return model.Admitted{}, err
	}
	avail, ok := snap.AsAvailable()
	if !ok {
		return model.Admitted{}, fmt.Errorf("%w: snapshot is not available", model.ErrFailedPrecondition)
	}
	return avail.RestoreVolume(st, id, req)
}

// sameVolumeSpec reports whether a replay asks for the stored spec.
// VolumeSpec is comparable, so a new field or source kind is compared here
// without another edit.
func sameVolumeSpec(a, b model.VolumeSpec) bool { return a == b }

func (s *Service) admitDeleteVolume(ctx context.Context, r store.Reader, st model.Start, req model.DeleteVolumeRequest) (model.Admitted, error) {
	vol, err := r.Volume(ctx, req.Volume)
	if errors.Is(err, model.ErrNotFound) && req.AllowMissing {
		return model.FinishNow(st, model.VolumeDeletion{Volume: req.Volume, Zone: req.Zone}, nil)
	}
	if err != nil {
		return model.Admitted{}, err
	}
	if !req.Zone.IsZero() && vol.Spec.Zone != req.Zone {
		return model.Admitted{}, fmt.Errorf("%w: volume", model.ErrNotFound)
	}
	if del, ok := vol.State.(model.Deleting); ok {
		op, err := r.Operation(ctx, del.Op)
		if err != nil {
			return model.Admitted{}, err
		}
		return model.NoteExisting(st, op)
	}
	if err := expectVersion(vol.Meta.Version, req.Expected); err != nil {
		return model.Admitted{}, err
	}
	avail, ok := vol.AsAvailable()
	if !ok {
		return model.Admitted{}, fmt.Errorf("%w: volume is not available", model.ErrFailedPrecondition)
	}
	busy, err := r.InFlight(ctx, req.Volume)
	if err != nil {
		return model.Admitted{}, err
	}
	if busy {
		return model.Admitted{}, fmt.Errorf("%w: volume has operations in flight", model.ErrFailedPrecondition)
	}
	return avail.Delete(st)
}

func (s *Service) runVolumeCreation(ctx context.Context, op model.Operation, w model.VolumeCreation) error {
	if _, ok := model.AsCompensating[model.VolumeCreation](op); ok {
		return undo(ctx, s, op.ID,
			func(ctx context.Context) error { return s.block.DeleteVolume(ctx, w.Volume) },
			func(r store.Reader, comp model.CompensatingOp[model.VolumeCreation], now time.Time) (model.Change, error) {
				vol, err := r.Volume(ctx, w.Volume)
				if errors.Is(err, model.ErrNotFound) {
					return comp.FinishFailed(now), nil
				}
				if err != nil {
					return model.Change{}, err
				}
				creating, ok := vol.AsCreating()
				if !ok {
					return comp.FinishFailed(now), nil
				}
				return creating.Abandoned(comp, now)
			})
	}
	callErr := s.block.CreateVolume(ctx, w.Volume, w.Spec)
	return finish(ctx, s, op.ID, callErr,
		func(run model.RunningOp[model.VolumeCreation], r store.Reader, now time.Time) (model.Change, error) {
			vol, err := r.Volume(ctx, w.Volume)
			if errors.Is(err, model.ErrNotFound) {
				return model.Compensate(run, model.Status{Code: model.CodeInternal, Message: "volume row missing"}, now), nil
			}
			if err != nil {
				return model.Change{}, err
			}
			creating, ok := vol.AsCreating()
			if !ok {
				return run.Fail(model.Status{Code: model.CodeInternal, Message: "volume is not creating"}, now), nil
			}
			return creating.Created(run, now)
		},
		func(run model.RunningOp[model.VolumeCreation], _ store.Reader, st model.Status, now time.Time) (model.Change, error) {
			return model.Compensate(run, st, now), nil
		},
	)
}

func (s *Service) runVolumeDeletion(ctx context.Context, op model.Operation, w model.VolumeDeletion) error {
	callErr := s.block.DeleteVolume(ctx, w.Volume)
	return finish(ctx, s, op.ID, callErr,
		func(run model.RunningOp[model.VolumeDeletion], r store.Reader, now time.Time) (model.Change, error) {
			vol, err := r.Volume(ctx, w.Volume)
			if errors.Is(err, model.ErrNotFound) {
				return run.Fail(model.Status{Code: model.CodeInternal, Message: "volume row missing"}, now), nil
			}
			if err != nil {
				return model.Change{}, err
			}
			deleting, ok := vol.AsDeleting()
			if !ok {
				return run.Fail(model.Status{Code: model.CodeInternal, Message: "volume is not deleting"}, now), nil
			}
			return deleting.Deleted(run, now)
		},
		func(run model.RunningOp[model.VolumeDeletion], r store.Reader, st model.Status, now time.Time) (model.Change, error) {
			vol, err := r.Volume(ctx, w.Volume)
			if errors.Is(err, model.ErrNotFound) {
				return run.Fail(st, now), nil
			}
			if err != nil {
				return model.Change{}, err
			}
			deleting, ok := vol.AsDeleting()
			if !ok {
				return run.Fail(st, now), nil
			}
			return deleting.DeleteFailed(run, st, now)
		},
	)
}
