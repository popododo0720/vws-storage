package storage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/popododo0720/vws-lib/obs"

	"github.com/popododo0720/vws-storage/internal/backend"
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
			if _, failed := vol.State.(model.Failed); failed {
				return model.Admitted{}, fmt.Errorf("%w: volume failed to create; delete it", model.ErrFailedPrecondition)
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
	// A type the backend does not serve is refused before anything is
	// written, so it leaves no row, operation or backend object. It carries its
	// own reason because EC2 names this UnknownVolumeType ("The specified
	// volume type is unsupported").
	if ts, ok := s.block.(backend.TypeServer); ok {
		if err := ts.ServeType(req.Spec.Type); err != nil {
			return model.Admitted{}, unsupportedVolumeType(req.Spec.Type, err)
		}
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
	busy, err := r.InFlight(ctx, req.Volume)
	if err != nil {
		return model.Admitted{}, err
	}
	if failed, ok := vol.AsFailed(); ok && !busy {
		if err := s.refuseAttached(ctx, req.Volume); err != nil {
			return model.Admitted{}, err
		}
		return failed.Delete(st)
	}
	avail, ok := vol.AsAvailable()
	if !ok {
		return model.Admitted{}, fmt.Errorf("%w: volume is not available", model.ErrFailedPrecondition)
	}
	if busy {
		return model.Admitted{}, fmt.Errorf("%w: volume has operations in flight", model.ErrFailedPrecondition)
	}
	if err := s.refuseAttached(ctx, req.Volume); err != nil {
		return model.Admitted{}, err
	}
	return avail.Delete(st)
}

// refuseAttached is the DeleteVolume admission half of the attachment rule
// (#152): a volume an instance spec uses is DEPENDENCY_MISSING (EC2
// VolumeInUse), and the store is left as it was. An attachment list the
// backend cannot read refuses as well: a delete must not guess the volume is
// detached. It runs after the cheap row checks, so only a delete that would
// otherwise be admitted asks the backend.
func (s *Service) refuseAttached(ctx context.Context, id model.VolumeID) error {
	inst, err := s.attachedTo(ctx, id)
	if err != nil {
		slog.WarnContext(ctx, "volume delete refused: attachments unavailable",
			slog.String(obs.FieldResourceType, "volume"), slog.String(obs.FieldResourceID, id.String()),
			slog.String(obs.FieldErr, err.Error()))
		return fmt.Errorf("%w: cannot confirm that the volume is detached; retry later", model.ErrFailedPrecondition)
	}
	if inst != "" {
		return fmt.Errorf("%w: volume is attached to instance %s", model.ErrDependencyMissing, inst)
	}
	return nil
}

// attachedTo returns the id of the instance whose spec uses the backend
// object of id, or "" when none does or the backend has no attachments.
// Unlike Attachments it returns the backend error instead of reporting none.
func (s *Service) attachedTo(ctx context.Context, id model.VolumeID) (string, error) {
	a, ok := s.block.(backend.Attacher)
	if !ok {
		return "", nil
	}
	byClaim, err := a.Attachments(ctx)
	if err != nil {
		return "", fmt.Errorf("storage: volume attachments: %w", err)
	}
	return byClaim[s.block.ClaimName(id)], nil
}

// detachedOrBusy is the delete-step half of the attachment rule (#152). The
// attachment is written by vws-compute into the instance spec, outside this
// store, so an attach can land after admission saw none. Admission commits
// Deleting first and this check reads the attachments again right before the
// backend delete; vws-compute refuses a volume that is being deleted. With
// each side writing before it checks, one of them sees the other. An attached
// or unreadable volume is a transient backend.ErrBusy: the operation retries
// and the backend object stays until the instance lets go.
func (s *Service) detachedOrBusy(ctx context.Context, id model.VolumeID) error {
	inst, err := s.attachedTo(ctx, id)
	if err != nil {
		// The cause stays text so a wrapped permanent error never reads as permanent.
		return fmt.Errorf("%w: %v", backend.ErrBusy, err) //nolint:errorlint // see above
	}
	if inst != "" {
		return fmt.Errorf("%w: volume %s is attached to instance %s", backend.ErrBusy, id, inst)
	}
	return nil
}

// admitExpandVolume grows an available volume. A size equal to the stored one
// is already true and records a succeeded operation. A smaller size is invalid.
func (s *Service) admitExpandVolume(ctx context.Context, r store.Reader, st model.Start, req model.ExpandVolumeRequest) (model.Admitted, error) {
	vol, err := r.Volume(ctx, req.Volume)
	if err != nil {
		return model.Admitted{}, err
	}
	if !req.Zone.IsZero() && vol.Spec.Zone != req.Zone {
		return model.Admitted{}, fmt.Errorf("%w: volume", model.ErrNotFound)
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
	switch {
	case req.Size < vol.Spec.Size:
		return model.Admitted{}, fmt.Errorf("%w: volume cannot shrink", model.ErrInvalidArgument)
	case req.Size == vol.Spec.Size:
		return model.FinishNow(st, model.VolumeExpansion{Volume: vol.ID, Size: req.Size}, vol)
	}
	return avail.Expand(st, req.Size)
}

// admitModifyVolume sets an available volume's labels and type in one write.
// A type change must be one the backend serves without touching the volume's
// object (backend.Retyper); otherwise it is invalid.
func (s *Service) admitModifyVolume(ctx context.Context, r store.Reader, st model.Start, req model.ModifyVolumeRequest) (model.Admitted, error) {
	vol, err := r.Volume(ctx, req.Volume)
	if err != nil {
		return model.Admitted{}, err
	}
	if !req.Zone.IsZero() && vol.Spec.Zone != req.Zone {
		return model.Admitted{}, fmt.Errorf("%w: volume", model.ErrNotFound)
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
	labels, typ := vol.Labels, vol.Spec.Type
	if req.SetLabels {
		labels = req.Labels
	}
	if req.Type != model.VolumeTypeUnspecified && req.Type != typ {
		if ts, ok := s.block.(backend.TypeServer); ok {
			if err := ts.ServeType(req.Type); err != nil {
				return model.Admitted{}, unsupportedVolumeType(req.Type, err)
			}
		}
		rt, ok := s.block.(backend.Retyper)
		if !ok {
			return model.Admitted{}, fmt.Errorf("%w: this backend cannot change a volume type", model.ErrInvalidArgument)
		}
		if err := rt.Retype(typ, req.Type); err != nil {
			return model.Admitted{}, fmt.Errorf("%w: %w", model.ErrInvalidArgument, err)
		}
		typ = req.Type
	}
	return avail.Modify(st, labels, typ)
}

// unsupportedVolumeType is a type this deployment has no storage mapped for.
func unsupportedVolumeType(t model.VolumeType, cause error) error {
	return fmt.Errorf("%w: volume type %s is unsupported: %w",
		model.ErrUnsupportedVolumeType, t, cause)
}

func (s *Service) runVolumeExpansion(ctx context.Context, op model.Operation, w model.VolumeExpansion) error {
	callErr := s.block.Expand(ctx, w.Volume, w.Size)
	return finish(ctx, s, op.ID, callErr,
		func(run model.RunningOp[model.VolumeExpansion], r store.Reader, now time.Time) (model.Change, error) {
			vol, err := r.Volume(ctx, w.Volume)
			if errors.Is(err, model.ErrNotFound) {
				return run.Fail(model.Status{Code: model.CodeInternal, Message: "volume row missing"}, now), nil
			}
			if err != nil {
				return model.Change{}, err
			}
			avail, ok := vol.AsAvailable()
			if !ok {
				return run.Fail(model.Status{Code: model.CodeInternal, Message: "volume is not available"}, now), nil
			}
			return avail.Expanded(run, now)
		},
		func(run model.RunningOp[model.VolumeExpansion], _ store.Reader, st model.Status, now time.Time) (model.Change, error) {
			return run.Fail(st, now), nil
		},
	)
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
		// A permanent failure keeps the volume in Failed with the reason, so
		// GetVolume and ListVolumes show why. Its backend leftovers stay until
		// DeleteVolume, which removes them as the undo used to.
		func(run model.RunningOp[model.VolumeCreation], r store.Reader, st model.Status, now time.Time) (model.Change, error) {
			vol, err := r.Volume(ctx, w.Volume)
			if errors.Is(err, model.ErrNotFound) {
				return model.Compensate(run, st, now), nil
			}
			if err != nil {
				return model.Change{}, err
			}
			creating, ok := vol.AsCreating()
			if !ok {
				return model.Compensate(run, st, now), nil
			}
			return creating.Failed(run, st, now)
		},
	)
}

func (s *Service) runVolumeDeletion(ctx context.Context, op model.Operation, w model.VolumeDeletion) error {
	callErr := s.detachedOrBusy(ctx, w.Volume)
	if callErr == nil {
		callErr = s.block.DeleteVolume(ctx, w.Volume)
	}
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
