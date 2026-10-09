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
	id := model.NewVolumeID()
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

func (s *Service) admitDeleteVolume(ctx context.Context, r store.Reader, st model.Start, req model.DeleteVolumeRequest) (model.Admitted, error) {
	vol, err := r.Volume(ctx, req.Volume)
	if err != nil {
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
