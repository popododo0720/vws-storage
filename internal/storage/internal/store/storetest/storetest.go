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
}

func roundTrip(t *testing.T, open OpenFunc) {
	ctx := context.Background()
	st := open(t)
	defer st.Close()
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
	got, err := st.Volume(ctx, admitted.Op.Work.(model.VolumeCreation).Volume)
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
	err = st.Update(ctx, func(store.Reader) (model.Change, error) { return created, nil })
	if !errors.Is(err, model.ErrStale) {
		t.Fatalf("edited-record err = %v", err)
	}
}

func runnable(t *testing.T, open OpenFunc) {
	ctx := context.Background()
	st := open(t)
	defer st.Close()
	now := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	admitted := admitVolume(t, now)
	if err := st.Update(ctx, func(store.Reader) (model.Change, error) { return admitted.Change, nil }); err != nil {
		t.Fatal(err)
	}
	id := admitted.Op.Work.(model.VolumeCreation).Volume
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
	req := model.CreateVolumeRequest{Spec: model.VolumeSpec{Size: model.GiB, Zone: zone, Type: model.VolumeTypeSSD, Source: model.NoSource()}}
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
