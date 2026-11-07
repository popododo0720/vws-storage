// Package storetest is the conformance suite for Store implementations.
package storetest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/storage/internal/store"
)

// OpenFunc opens a fresh store.
type OpenFunc func(t *testing.T) store.Store

// Run checks the store contract.
func Run(t *testing.T, open OpenFunc) {
	t.Helper()
	t.Run("round trip and stale", func(t *testing.T) { roundTrip(t, open) })
	t.Run("runnable and inflight", func(t *testing.T) { runnable(t, open) })
	t.Run("import", func(t *testing.T) { importRecords(t, open) })
}

// importRecords: model.Import keeps a record's version, an imported
// operation is in flight and runnable, and a second import of a key is stale.
func importRecords(t *testing.T, open OpenFunc) {
	ctx := context.Background()
	st := open(t)
	defer func() {
		if err := st.Close(); err != nil {
			t.Error(err)
		}
	}()
	now := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	admitted := admitVolume(t, now)
	creating, ok := volumeFrom(t, admitted.Change).AsCreating()
	if !ok {
		t.Fatal("not creating")
	}
	run, ok := model.AsRunning[model.VolumeCreation](admitted.Op)
	if !ok {
		t.Fatal("not running")
	}
	created, err := creating.Created(run, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	vol := volumeFrom(t, created)
	if vol.Meta.Version == 1 {
		t.Fatal("want a record past its first version")
	}
	for _, rec := range []model.Record{vol, admitted.Op} {
		ch, err := model.Import(rec)
		if err != nil {
			t.Fatal(err)
		}
		apply := func(store.Reader) (model.Change, error) { return ch, nil }
		if err := st.Update(ctx, apply); err != nil {
			t.Fatal(err)
		}
		if err := st.Update(ctx, apply); !errors.Is(err, model.ErrStale) {
			t.Fatalf("second import of %s: %v, want ErrStale", rec.RecordKey(), err)
		}
	}
	got, err := st.Volume(ctx, vol.ID)
	if err != nil || !model.SameRecord(got, vol) {
		t.Fatalf("imported volume %+v %v, want %+v", got, err, vol)
	}
	op, err := st.Operation(ctx, admitted.Op.ID)
	if err != nil || !model.SameRecord(op, admitted.Op) {
		t.Fatalf("imported operation %+v %v", op, err)
	}
	busy, err := st.InFlight(ctx, vol.ID)
	if err != nil || !busy {
		t.Fatalf("imported operation in flight %v %v, want true", busy, err)
	}
	due, err := st.Runnable(ctx, now.Add(time.Hour), 10)
	if err != nil || len(due) != 1 || due[0].ID != admitted.Op.ID {
		t.Fatalf("runnable %v %v, want the imported operation", due, err)
	}
}

func roundTrip(t *testing.T, open OpenFunc) {
	ctx := context.Background()
	st := open(t)
	defer func() {
		if err := st.Close(); err != nil {
			t.Error(err)
		}
	}()
	now := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	admitted := admitVolume(t, now)
	if err := st.Update(ctx, func(store.Reader) (model.Change, error) {
		return admitted.Change, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.Update(ctx, func(store.Reader) (model.Change, error) {
		return admitted.Change, nil
	}); !errors.Is(err, model.ErrStale) {
		t.Fatalf("second insert err = %v", err)
	}
	got, err := st.Volume(ctx, creationVolume(t, admitted))
	if err != nil {
		t.Fatal(err)
	}
	want := volumeFrom(t, admitted.Change)
	if !model.SameRecord(got, want) {
		t.Fatal("volume changed across the store")
	}
	op, err := st.Operation(ctx, admitted.Op.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !model.SameRecord(op, admitted.Op) {
		t.Fatal("operation changed across the store")
	}

	var created model.Change
	if err := st.Update(ctx, func(r store.Reader) (model.Change, error) {
		vol, err := r.Volume(ctx, got.ID)
		if err != nil {
			return model.Change{}, err
		}
		creating, ok := vol.AsCreating()
		if !ok {
			t.Fatal("not creating")
		}
		cur, err := r.Operation(ctx, admitted.Op.ID)
		if err != nil {
			return model.Change{}, err
		}
		run, ok := model.AsRunning[model.VolumeCreation](cur)
		if !ok {
			t.Fatal("not running")
		}
		created, err = creating.Created(run, now.Add(time.Second))
		if err != nil {
			return model.Change{}, err
		}
		return run.Retry(model.Status{Code: model.CodeUnavailable, Message: "later"}, now.Add(time.Minute), now), nil
	}); err != nil {
		t.Fatal(err)
	}
	keptVol, err := st.Volume(ctx, got.ID)
	if err != nil {
		t.Fatal(err)
	}
	keptOp, err := st.Operation(ctx, admitted.Op.ID)
	if err != nil {
		t.Fatal(err)
	}
	err = st.Update(ctx, func(store.Reader) (model.Change, error) { return created, nil })
	if !errors.Is(err, model.ErrStale) {
		t.Fatalf("edited-record err = %v", err)
	}
	got, err = st.Volume(ctx, got.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !model.SameRecord(got, keptVol) {
		t.Fatal("stale change mutated the volume")
	}
	op, err = st.Operation(ctx, admitted.Op.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !model.SameRecord(op, keptOp) {
		t.Fatal("stale change mutated the operation")
	}
}

func runnable(t *testing.T, open OpenFunc) {
	ctx := context.Background()
	st := open(t)
	defer func() {
		if err := st.Close(); err != nil {
			t.Error(err)
		}
	}()
	now := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	admitted := admitVolume(t, now)
	if err := st.Update(ctx, func(store.Reader) (model.Change, error) { return admitted.Change, nil }); err != nil {
		t.Fatal(err)
	}
	id := creationVolume(t, admitted)
	busy, err := st.InFlight(ctx, id)
	if err != nil || !busy {
		t.Fatalf("inflight %v %v", busy, err)
	}
	due, err := st.Runnable(ctx, now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 1 || due[0].ID != admitted.Op.ID {
		t.Fatalf("runnable %+v", due)
	}
	if err := st.Update(ctx, func(r store.Reader) (model.Change, error) {
		vol, err := r.Volume(ctx, id)
		if err != nil {
			return model.Change{}, err
		}
		creating, _ := vol.AsCreating()
		cur, err := r.Operation(ctx, admitted.Op.ID)
		if err != nil {
			return model.Change{}, err
		}
		run, _ := model.AsRunning[model.VolumeCreation](cur)
		return creating.Created(run, now.Add(time.Second))
	}); err != nil {
		t.Fatal(err)
	}
	busy, err = st.InFlight(ctx, id)
	if err != nil || busy {
		t.Fatalf("inflight after settle %v %v", busy, err)
	}
	due, err = st.Runnable(ctx, now.Add(time.Hour), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 0 {
		t.Fatalf("runnable after settle %+v", due)
	}
	vol, err := st.Volume(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := vol.AsAvailable(); !ok {
		t.Fatalf("state %v", vol.State)
	}
}

func admitVolume(t *testing.T, now time.Time) model.Admitted {
	t.Helper()
	zone, err := model.ParseZone("az-1")
	if err != nil {
		t.Fatal(err)
	}
	tok, err := model.ParseClientToken("store-test")
	if err != nil {
		t.Fatal(err)
	}
	req := model.CreateVolumeRequest{Spec: model.VolumeSpec{
		Size: model.GiB, Zone: zone, Type: model.VolumeTypeGP2, Source: model.NoSource(),
	}}
	claim, err := model.Claim(tok, req)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := model.NewVolume(model.Start{Op: model.NewOperationID(), Claim: claim, Now: now}, model.NewVolumeID(), req)
	if err != nil {
		t.Fatal(err)
	}
	return admitted
}

func volumeFrom(t *testing.T, ch model.Change) model.Volume {
	t.Helper()
	for _, w := range ch.Writes() {
		put, ok := w.(model.Put)
		if !ok {
			continue
		}
		if vol, ok := put.Next.(model.Volume); ok {
			return vol
		}
	}
	t.Fatal("no volume")
	return model.Volume{}
}

func creationVolume(t *testing.T, a model.Admitted) model.VolumeID {
	t.Helper()
	w, ok := a.Op.Work.(model.VolumeCreation)
	if !ok {
		t.Fatalf("work is %T, want VolumeCreation", a.Op.Work)
	}
	return w.Volume
}
