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

// admitPutObject is a strict create (S3 If-None-Match: *). A live object at
// the path is ALREADY_EXISTS, and so is another create of the path still in
// flight, whatever the bytes. The only replay is the same request_id with the
// same request, which Submit answers from the token before admit runs.
func (s *Service) admitPutObject(ctx context.Context, r store.Reader, st model.Start, req model.PutObjectRequest) (model.Admitted, error) {
	_, err := r.Object(ctx, req.Path)
	switch {
	case err == nil:
		return model.Admitted{}, fmt.Errorf("%w: object", model.ErrAlreadyExists)
	case !errors.Is(err, model.ErrNotFound):
		return model.Admitted{}, err
	}
	return s.admitFirstUpload(ctx, r, st, req, false)
}

func (s *Service) admitUpdateObject(ctx context.Context, r store.Reader, st model.Start, req model.UpdateObjectRequest) (model.Admitted, error) {
	head, err := r.Object(ctx, req.Path)
	switch {
	case errors.Is(err, model.ErrNotFound):
		if !req.AllowMissing {
			return model.Admitted{}, err
		}
		if !req.HasContents {
			return model.Admitted{}, fmt.Errorf("%w: contents", model.ErrInvalidArgument)
		}
		return s.admitFirstUpload(ctx, r, st, req.AsPut(), true)
	case err != nil:
		return model.Admitted{}, err
	}
	if err := expectVersion(head.Meta.Version, req.Expected); err != nil {
		return model.Admitted{}, err
	}
	busy, err := r.InFlight(ctx, req.Path)
	if err != nil {
		return model.Admitted{}, err
	}
	if !req.HasContents {
		if busy {
			return model.Admitted{}, fmt.Errorf("%w: object has operations in flight", model.ErrFailedPrecondition)
		}
		return model.ApplyMeta(st, head, req.ContentType, req.Labels)
	}
	match := func(op model.Operation) bool {
		up, is := op.Work.(model.ObjectUpload)
		return is && up.Object.Path == req.Path && up.Replaces == head.Generation && up.Content() == req.Content()
	}
	if req.Content().Matches(head) {
		op, ok, err := findOpen(ctx, r, model.KindPutObject, match)
		if err != nil {
			return model.Admitted{}, err
		}
		if ok {
			return model.NoteExisting(st, op)
		}
		if busy {
			return model.Admitted{}, fmt.Errorf("%w: object has operations in flight", model.ErrFailedPrecondition)
		}
		work := model.ObjectUpload{
			Object:      model.ObjectRef{Path: head.Path, Generation: head.Generation},
			Size:        head.Size,
			SHA256:      head.SHA256,
			ContentType: head.ContentType,
			Labels:      head.Labels,
		}
		return model.FinishNow(st, work, head)
	}
	if busy {
		op, ok, err := findOpen(ctx, r, model.KindPutObject, match)
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
	return avail.Replace(st, head, model.NewGeneration(), req, st.Now.Add(s.window))
}

// admitFirstUpload admits the upload of a path that has no object. join lets
// an upsert (UpdateObject allow_missing) join an in-flight first upload of the
// same content. A create never joins: the other upload is another writer, so
// the path is taken (ALREADY_EXISTS).
func (s *Service) admitFirstUpload(ctx context.Context, r store.Reader, st model.Start, req model.PutObjectRequest, join bool) (model.Admitted, error) {
	busy, err := r.InFlight(ctx, req.Path)
	if err != nil {
		return model.Admitted{}, err
	}
	if busy && !join {
		_, creating, err := findOpen(ctx, r, model.KindPutObject, func(op model.Operation) bool {
			up, is := op.Work.(model.ObjectUpload)
			return is && up.Replaces.IsZero() && up.Object.Path == req.Path
		})
		if err != nil {
			return model.Admitted{}, err
		}
		if creating {
			return model.Admitted{}, fmt.Errorf("%w: object is being created", model.ErrAlreadyExists)
		}
		return model.Admitted{}, fmt.Errorf("%w: object has operations in flight", model.ErrFailedPrecondition)
	}
	if busy {
		op, ok, err := findOpen(ctx, r, model.KindPutObject, func(op model.Operation) bool {
			up, is := op.Work.(model.ObjectUpload)
			return is && up.Replaces.IsZero() && up.Object.Path == req.Path && up.Content() == req.Content()
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

// admitWriteObject replaces the payload of an existing object. It is an
// overwrite that keeps the stored content type and labels, so it shares every
// rule of UpdateObject: the etag check, the identical-bytes no-op, and the
// in-flight guard.
func (s *Service) admitWriteObject(ctx context.Context, r store.Reader, st model.Start, req model.WriteObjectRequest) (model.Admitted, error) {
	head, err := r.Object(ctx, req.Path)
	if err != nil {
		return model.Admitted{}, err
	}
	return s.admitUpdateObject(ctx, r, st, req.AsUpdate(head))
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
			return is && !purge.Reclaim && purge.Object.Path == req.Path && purge.Object.Generation == obj.Generation
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
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("storage: %w", err)
	}
	obs, statErr := s.object.Stat(ctx, w.Object)
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("storage: %w", err)
	}
	purgeID := model.NewOperationID().WithProject(op.ID.Project())
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
			return model.Compensate(run, statusOf(ctx, statErr), now), nil
		case statErr != nil:
			return run.Retry(statusOf(ctx, statErr), s.policy.next(run.Attempts(), now), now), nil //nolint:nilerr // the backend error is recorded on the retry change and the engine retries
		}
		if err := w.Check(obs); err != nil {
			return model.Compensate(run, model.Status{Code: model.CodeFailedPrecondition, Message: err.Error()}, now), nil //nolint:nilerr // a failed check is recorded on the compensating change
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
