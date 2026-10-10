package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/popododo0720/vws-storage/internal/backend"
	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/storage/internal/store"
)

func (s *Service) admitPutObject(ctx context.Context, r store.Reader, st model.Start, req model.PutObjectRequest) (model.Admitted, error) {
	head, err := r.Object(ctx, req.Path)
	switch {
	case err == nil:
		if head.SHA256 != req.SHA256 || head.ContentType != req.ContentType || head.Size != req.Size {
			return model.Admitted{}, fmt.Errorf("%w: object", model.ErrAlreadyExists)
		}
		op, ok, err := findOpen(ctx, r, model.KindPutObject, func(op model.Operation) bool {
			up, is := op.Work.(model.ObjectUpload)
			return is && up.Object.Path == req.Path
		})
		if err != nil {
			return model.Admitted{}, err
		}
		if ok {
			return model.NoteExisting(st, op)
		}
		busy, err := r.InFlight(ctx, req.Path)
		if err != nil {
			return model.Admitted{}, err
		}
		if busy {
			return model.Admitted{}, fmt.Errorf("%w: object has operations in flight", model.ErrFailedPrecondition)
		}
		work := model.ObjectUpload{
			Object:      model.ObjectRef{Path: head.Path, Generation: head.Generation},
			Size:        head.Size,
			SHA256:      head.SHA256,
			ContentType: head.ContentType,
		}
		return model.FinishNow(st, work, head)
	case !errors.Is(err, model.ErrNotFound):
		return model.Admitted{}, err
	}
	busy, err := r.InFlight(ctx, req.Path)
	if err != nil {
		return model.Admitted{}, err
	}
	if busy {
		op, ok, err := findOpen(ctx, r, model.KindPutObject, func(op model.Operation) bool {
			up, is := op.Work.(model.ObjectUpload)
			return is && up.Object.Path == req.Path && up.SHA256 == req.SHA256 && up.ContentType == req.ContentType && up.Size == req.Size
		})
		if err != nil {
			return model.Admitted{}, err
		}
		if ok {
			return model.NoteExisting(st, op)
		}
		return model.Admitted{}, fmt.Errorf("%w: object has operations in flight", model.ErrFailedPrecondition)
	}
	b, err := r.Bucket(ctx, req.Path.Bucket)
	if err != nil {
		return model.Admitted{}, err
	}
	avail, ok := b.AsAvailable()
	if !ok {
		return model.Admitted{}, fmt.Errorf("%w: bucket is not available", model.ErrFailedPrecondition)
	}
	return avail.Upload(st, model.NewGeneration(), req, st.Now.Add(s.window))
}

func (s *Service) admitDeleteObject(ctx context.Context, r store.Reader, st model.Start, req model.DeleteObjectRequest) (model.Admitted, error) {
	obj, err := r.Object(ctx, req.Path)
	if errors.Is(err, model.ErrNotFound) && req.AllowMissing {
		return model.FinishNow(st, model.ObjectPurge{Object: model.ObjectRef{Path: req.Path}}, nil)
	}
	if err != nil {
		return model.Admitted{}, err
	}
	if err := expectVersion(obj.Meta.Version, req.Expected); err != nil {
		return model.Admitted{}, err
	}
	busy, err := r.InFlight(ctx, req.Path)
	if err != nil {
		return model.Admitted{}, err
	}
	if busy {
		op, ok, err := findOpen(ctx, r, model.KindDeleteObject, func(op model.Operation) bool {
			purge, is := op.Work.(model.ObjectPurge)
			return is && purge.Object.Path == req.Path
		})
		if err != nil {
			return model.Admitted{}, err
		}
		if ok {
			return model.NoteExisting(st, op)
		}
		return model.Admitted{}, fmt.Errorf("%w: object has operations in flight", model.ErrFailedPrecondition)
	}
	return obj.Delete(st)
}

func (s *Service) runObjectUpload(ctx context.Context, op model.Operation, w model.ObjectUpload) error {
	if _, ok := model.AsCompensating[model.ObjectUpload](op); ok {
		return undo(ctx, s, op.ID,
			func(ctx context.Context) error { return s.object.DeleteObject(ctx, w.Object) },
			func(_ store.Reader, comp model.CompensatingOp[model.ObjectUpload], now time.Time) (model.Change, error) {
				return model.AbandonUpload(comp, now)
			})
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	obs, statErr := s.object.Stat(ctx, w.Object)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	purgeID := model.NewOperationID()
	return s.settle(ctx, func(r store.Reader, now time.Time) (model.Change, error) {
		cur, err := r.Operation(ctx, op.ID)
		if err != nil {
			return model.Change{}, err
		}
		run, ok := model.AsRunning[model.ObjectUpload](cur)
		if !ok {
			return model.Change{}, nil
		}
		switch {
		case errors.Is(statErr, backend.ErrNotFound) && now.Before(w.Deadline):
			return run.Retry(model.Status{}, s.policy.uploadPoll(now), now), nil
		case errors.Is(statErr, backend.ErrNotFound):
			return model.Compensate(run, model.Status{Code: model.CodeDeadlineExceeded, Message: "upload missed its deadline"}, now), nil
		case statErr != nil && backend.IsPermanent(statErr):
			return model.Compensate(run, statusOf(statErr), now), nil
		case statErr != nil:
			return run.Retry(statusOf(statErr), s.policy.next(run.Attempts(), now), now), nil
		}
		if err := w.Check(obs); err != nil {
			return model.Compensate(run, model.Status{Code: model.CodeFailedPrecondition, Message: err.Error()}, now), nil
		}
		var head *model.Object
		switch h, err := r.Object(ctx, w.Object.Path); {
		case err == nil:
			head = &h
		case !errors.Is(err, model.ErrNotFound):
			return model.Change{}, err
		}
		return model.CommitUpload(run, head, obs, purgeID, now)
	})
}

func (s *Service) runObjectPurge(ctx context.Context, op model.Operation, w model.ObjectPurge) error {
	callErr := s.object.DeleteObject(ctx, w.Object)
	return finish(ctx, s, op.ID, callErr,
		func(run model.RunningOp[model.ObjectPurge], r store.Reader, now time.Time) (model.Change, error) {
			var head *model.Object
			switch h, err := r.Object(ctx, w.Object.Path); {
			case err == nil:
				head = &h
			case !errors.Is(err, model.ErrNotFound):
				return model.Change{}, err
			}
			return model.Purged(run, head, now)
		},
		func(run model.RunningOp[model.ObjectPurge], _ store.Reader, st model.Status, now time.Time) (model.Change, error) {
			return model.PurgeFailed(run, st, now)
		},
	)
}
