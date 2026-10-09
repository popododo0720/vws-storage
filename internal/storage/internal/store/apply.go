package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/popododo0720/vws-storage/internal/model"
)

// Apply checks each write against the current row and applies all of them.
// A failed precondition returns model.ErrStale and the caller rolls back.
func Apply(ctx context.Context, t Table, c model.Change) error {
	for _, w := range c.Writes() {
		switch w := w.(type) {
		case model.Put:
			if err := applyPut(ctx, t, w); err != nil {
				return err
			}
		case model.Remove:
			got, err := t.Get(ctx, w.Prev.RecordKey())
			if err != nil {
				return err
			}
			if !model.SameRecord(got, w.Prev) {
				return model.ErrStale
			}
			if err := t.Delete(ctx, w.Prev.RecordKey()); err != nil {
				return err
			}
			if op, ok := w.Prev.(model.Operation); ok {
				if err := t.ClearRefs(ctx, op.ID); err != nil {
					return err
				}
			}
		case model.Guard:
			got, err := t.Get(ctx, w.Record.RecordKey())
			if err != nil {
				return err
			}
			if !model.SameRecord(got, w.Record) {
				return model.ErrStale
			}
		default:
			return fmt.Errorf("store: unknown write %T", w)
		}
	}
	return nil
}

func applyPut(ctx context.Context, t Table, w model.Put) error {
	if w.Next == nil {
		return fmt.Errorf("store: put without next")
	}
	if w.Prev == nil {
		_, err := t.Get(ctx, w.Next.RecordKey())
		switch {
		case err == nil:
			return model.ErrStale
		case !errors.Is(err, model.ErrNotFound):
			return err
		}
		if w.Next.RecordMeta().Version != 1 {
			return model.ErrStale
		}
	} else {
		if keyString(w.Prev.RecordKey()) != keyString(w.Next.RecordKey()) {
			return model.ErrStale
		}
		got, err := t.Get(ctx, w.Prev.RecordKey())
		if err != nil {
			return err
		}
		if !model.SameRecord(got, w.Prev) {
			return model.ErrStale
		}
		if w.Next.RecordMeta().Version != w.Prev.RecordMeta().Version+1 {
			return model.ErrStale
		}
	}
	if err := t.Put(ctx, w.Next); err != nil {
		return err
	}
	if op, ok := w.Next.(model.Operation); ok && op.Work != nil {
		return t.SetRefs(ctx, op.ID, op.Work.Refs())
	}
	return nil
}

func keyString(k model.Key) string {
	type stringer interface{ String() string }
	if s, ok := k.(stringer); ok {
		return s.String()
	}
	return ""
}
