package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/storage/internal/store"
)

func (s *Service) admitCreateBucket(ctx context.Context, r store.Reader, st model.Start, req model.CreateBucketRequest) (model.Admitted, error) {
	b, err := r.Bucket(ctx, req.Bucket)
	switch {
	case err == nil:
		if _, del := b.State.(model.Deleting); del || b.Region != req.Region || b.Labels != req.Labels {
			return model.Admitted{}, fmt.Errorf("%w: bucket", model.ErrAlreadyExists)
		}
		var creating model.OperationID
		if c, ok := b.State.(model.Creating); ok {
			creating = c.Op
		}
		return replay(ctx, r, st, creating, model.BucketCreation{Bucket: b.Name}, b)
	case !errors.Is(err, model.ErrNotFound):
		return model.Admitted{}, err
	}
	return model.NewBucket(st, req)
}

func (s *Service) admitDeleteBucket(ctx context.Context, r store.Reader, st model.Start, req model.DeleteBucketRequest) (model.Admitted, error) {
	b, err := r.Bucket(ctx, req.Bucket)
	if errors.Is(err, model.ErrNotFound) && req.AllowMissing {
		return model.FinishNow(st, model.BucketDeletion{Bucket: req.Bucket}, nil)
	}
	if err != nil {
		return model.Admitted{}, err
	}
	if del, ok := b.State.(model.Deleting); ok {
		op, err := r.Operation(ctx, del.Op)
		if err != nil {
			return model.Admitted{}, err
		}
		return model.NoteExisting(st, op)
	}
	if err := expectVersion(b.Meta.Version, req.Expected); err != nil {
		return model.Admitted{}, err
	}
	avail, ok := b.AsAvailable()
	if !ok {
		return model.Admitted{}, fmt.Errorf("%w: bucket is not available", model.ErrFailedPrecondition)
	}
	has, err := r.HasObjects(ctx, req.Bucket)
	if err != nil {
		return model.Admitted{}, err
	}
	if has {
		return model.Admitted{}, fmt.Errorf("%w: bucket is not empty", model.ErrDependencyMissing)
	}
	busy, err := r.InFlight(ctx, req.Bucket)
	if err != nil {
		return model.Admitted{}, err
	}
	if busy {
		return model.Admitted{}, fmt.Errorf("%w: bucket has operations in flight", model.ErrFailedPrecondition)
	}
	return avail.Delete(st)
}

func (s *Service) runBucketCreation(ctx context.Context, op model.Operation, w model.BucketCreation) error {
	if _, ok := model.AsCompensating[model.BucketCreation](op); ok {
		return undo(ctx, s, op.ID,
			func(ctx context.Context) error { return s.object.DeleteBucket(ctx, w.Bucket) },
			func(r store.Reader, comp model.CompensatingOp[model.BucketCreation], now time.Time) (model.Change, error) {
				b, err := r.Bucket(ctx, w.Bucket)
				if errors.Is(err, model.ErrNotFound) {
					return comp.FinishFailed(now), nil
				}
				if err != nil {
					return model.Change{}, err
				}
				creating, ok := b.AsCreating()
				if !ok {
					return comp.FinishFailed(now), nil
				}
				return creating.Abandoned(comp, now)
			})
	}
	callErr := s.object.CreateBucket(ctx, w.Bucket)
	return finish(ctx, s, op.ID, callErr,
		func(run model.RunningOp[model.BucketCreation], r store.Reader, now time.Time) (model.Change, error) {
			b, err := r.Bucket(ctx, w.Bucket)
			if errors.Is(err, model.ErrNotFound) {
				return model.Compensate(run, model.Status{Code: model.CodeInternal, Message: "bucket row missing"}, now), nil
			}
			if err != nil {
				return model.Change{}, err
			}
			creating, ok := b.AsCreating()
			if !ok {
				return run.Fail(model.Status{Code: model.CodeInternal, Message: "bucket is not creating"}, now), nil
			}
			return creating.Created(run, now)
		},
		func(run model.RunningOp[model.BucketCreation], _ store.Reader, st model.Status, now time.Time) (model.Change, error) {
			return model.Compensate(run, st, now), nil
		},
	)
}

func (s *Service) runBucketDeletion(ctx context.Context, op model.Operation, w model.BucketDeletion) error {
	callErr := s.object.DeleteBucket(ctx, w.Bucket)
	return finish(ctx, s, op.ID, callErr,
		func(run model.RunningOp[model.BucketDeletion], r store.Reader, now time.Time) (model.Change, error) {
			b, err := r.Bucket(ctx, w.Bucket)
			if errors.Is(err, model.ErrNotFound) {
				return run.Fail(model.Status{Code: model.CodeInternal, Message: "bucket row missing"}, now), nil
			}
			if err != nil {
				return model.Change{}, err
			}
			deleting, ok := b.AsDeleting()
			if !ok {
				return run.Fail(model.Status{Code: model.CodeInternal, Message: "bucket is not deleting"}, now), nil
			}
			return deleting.Deleted(run, now)
		},
		func(run model.RunningOp[model.BucketDeletion], r store.Reader, st model.Status, now time.Time) (model.Change, error) {
			b, err := r.Bucket(ctx, w.Bucket)
			if errors.Is(err, model.ErrNotFound) {
				return run.Fail(st, now), nil
			}
			if err != nil {
				return model.Change{}, err
			}
			deleting, ok := b.AsDeleting()
			if !ok {
				return run.Fail(st, now), nil
			}
			return deleting.DeleteFailed(run, st, now)
		},
	)
}
