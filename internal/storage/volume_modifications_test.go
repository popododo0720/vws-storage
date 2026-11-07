package storage_test

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/popododo0720/vws-storage/internal/backend"
	"github.com/popododo0720/vws-storage/internal/backend/fake"
	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/storage"
)

func volumeShape(t *testing.T, svc *storage.Service, id model.VolumeID) model.VolumeShape {
	t.Helper()
	v, err := svc.Volume(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return v.Spec.Shape()
}

func listModifications(
	t *testing.T, svc *storage.Service, id model.VolumeID, size int,
) []model.VolumeModificationStatus {
	t.Helper()
	var out []model.VolumeModificationStatus
	tok := ""
	for range 20 {
		page, err := svc.ListVolumeModifications(context.Background(), id, model.PageRequest{Size: size, Token: tok})
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, page.Items...)
		if tok = page.NextToken; tok == "" {
			return byStart(out)
		}
	}
	t.Fatal("listing does not end")
	return nil
}

// byStart orders a listing by start time: the listing itself is in
// operation id order, and ids are random.
func byStart(ms []model.VolumeModificationStatus) []model.VolumeModificationStatus {
	slices.SortFunc(ms, func(a, b model.VolumeModificationStatus) int { return a.Start.Compare(b.Start) })
	return ms
}

// A volume lists the changes of its size, type, IOPS and throughput with the
// shapes before and after, the state and the times (#173). Labels-only and
// no-op updates are not modifications. A growth that is still waiting for the
// runner is modifying, a failed one is failed with its cause, a resent token
// adds no row, and the listing is the same after a restart and across pages.
func TestListVolumeModificationsRecordsOriginalAndTarget(t *testing.T) {
	blk := fake.NewBlock()
	obj := fake.NewObjects(fake.ObjectOptions{})
	path := filepath.Join(t.TempDir(), "vws.db")
	t0 := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	clk := &clock{t: t0}
	o := opts(storage.SQLite(path), blk, obj)
	o.Now = clk.now
	ctx := context.Background()
	svc, stop := openWith(t, o)
	volOp, err := svc.Submit(ctx, token(t, "vol"), volumeReq(t))
	if err != nil {
		t.Fatal(err)
	}
	id := waitDone(t, svc, volOp.ID).Target().(model.VolumeID)
	other, err := svc.Submit(ctx, token(t, "other"), volumeReq(t))
	if err != nil {
		t.Fatal(err)
	}
	otherID := waitDone(t, svc, other.ID).Target().(model.VolumeID)
	if got := listModifications(t, svc, id, 0); len(got) != 0 {
		t.Fatalf("a new volume lists %d modifications", len(got))
	}
	submit := func(tok string, req model.Request) storage.OperationStatus {
		t.Helper()
		op, err := svc.Submit(context.Background(), token(t, tok), req)
		if err != nil {
			t.Fatalf("%s: %v", tok, err)
		}
		return waitDone(t, svc, op.ID)
	}
	labels, err := model.ParseLabels(map[string]string{"team": "storage"})
	if err != nil {
		t.Fatal(err)
	}

	shape0 := volumeShape(t, svc, id)
	clk.set(t0.Add(time.Hour))
	retype := submit("retype", model.ModifyVolumeRequest{Volume: id, Type: model.VolumeTypeGP3})
	shape1 := volumeShape(t, svc, id)
	clk.set(t0.Add(2 * time.Hour))
	submit("labels-only", model.ModifyVolumeRequest{Volume: id, SetLabels: true, Labels: labels})
	submit("no-op", model.ModifyVolumeRequest{Volume: id, Type: model.VolumeTypeGP3, SetLabels: true, Labels: labels})
	clk.set(t0.Add(3 * time.Hour))
	grow := submit("grow", model.ExpandVolumeRequest{Volume: id, Size: 2 * model.GiB})
	shape2 := volumeShape(t, svc, id)
	clk.set(t0.Add(4 * time.Hour))
	blk.FailNext("Expand", backend.Permanent(backend.ErrInvalid))
	failed := submit("grow-fails", model.ExpandVolumeRequest{Volume: id, Size: 3 * model.GiB})
	if failed.Phase() != model.PhaseFailed {
		t.Fatalf("failed growth phase %v", failed.Phase())
	}
	submit("other-grow", model.ExpandVolumeRequest{Volume: otherID, Size: 2 * model.GiB})
	stop()
	if err := svc.Close(); err != nil {
		t.Fatal(err)
	}

	// No runner: the growth with a retype stays admitted.
	svc, err = storage.Open(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	clk.set(t0.Add(5 * time.Hour))
	req := model.ModifyVolumeRequest{
		Volume: id, Type: model.VolumeTypeIO2, Size: 8 * model.GiB, SetIops: true, Iops: 8000,
	}
	pending, err := svc.Submit(ctx, token(t, "grow-retype"), req)
	if err != nil {
		t.Fatal(err)
	}
	got := listModifications(t, svc, id, 0)
	if len(got) != 4 {
		t.Fatalf("waiting growth: %+v", got)
	}
	last := got[len(got)-1]
	if last.Op != pending.ID || last.State != model.ModificationModifying || !last.End.IsZero() {
		t.Fatalf("waiting growth: %+v", got)
	}
	if last.From != shape2 || last.To.Size != 8*model.GiB || last.To.Type != model.VolumeTypeIO2 || last.To.Iops != 8000 {
		t.Fatalf("waiting growth shapes: from %+v to %+v", last.From, last.To)
	}
	ctx2, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		_ = svc.Run(ctx2)
		close(done)
	}()
	clk.set(t0.Add(6 * time.Hour))
	finished := waitDone(t, svc, pending.ID)
	if finished.Phase() != model.PhaseSucceeded {
		t.Fatalf("growth phase %v", finished.Phase())
	}
	again, err := svc.Submit(ctx, token(t, "grow-retype"), req)
	if err != nil || again.ID != pending.ID {
		t.Fatalf("resent %s err %v, want %s", again.ID, err, pending.ID)
	}
	cancel()
	<-done
	if err := svc.Close(); err != nil {
		t.Fatal(err)
	}

	svc, err = storage.Open(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	shape3 := volumeShape(t, svc, id)
	got = listModifications(t, svc, id, 0)
	if len(got) != 4 {
		t.Fatalf("listed %d modifications after restart: %+v", len(got), got)
	}
	failedTo := shape2
	failedTo.Size = 3 * model.GiB
	want := []model.VolumeModificationStatus{
		{
			Op: retype.ID, Volume: id, From: shape0, To: shape1, State: model.ModificationCompleted,
			Start: t0.Add(time.Hour), End: t0.Add(time.Hour),
		},
		{
			Op: grow.ID, Volume: id, From: shape1, To: shape2, State: model.ModificationCompleted,
			Start: t0.Add(3 * time.Hour), End: t0.Add(3 * time.Hour),
		},
		{
			Op: failed.ID, Volume: id, From: shape2, To: failedTo, State: model.ModificationFailed,
			Start: t0.Add(4 * time.Hour), End: t0.Add(4 * time.Hour), Cause: failed.Err(),
		},
		{
			Op: pending.ID, Volume: id, From: shape2, To: shape3, State: model.ModificationCompleted,
			Start: t0.Add(5 * time.Hour), End: t0.Add(6 * time.Hour),
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("modifications\n got  %+v\n want %+v", got, want)
	}
	if shape1.Type != model.VolumeTypeGP3 || shape2.Size != 2*model.GiB ||
		shape3.Size != 8*model.GiB || shape3.Type != model.VolumeTypeIO2 || shape3.Iops != 8000 {
		t.Fatalf("shapes %+v %+v %+v", shape1, shape2, shape3)
	}
	if failed.Err().IsZero() {
		t.Fatal("failed growth has no cause")
	}

	var paged []model.VolumeModificationStatus
	tok := ""
	for range 10 {
		page, err := svc.ListVolumeModifications(ctx, id, model.PageRequest{Size: 3, Token: tok})
		if err != nil {
			t.Fatal(err)
		}
		if tok != "" && len(page.Items) != 1 || tok == "" && len(page.Items) != 3 {
			t.Fatalf("page of %d at token %q", len(page.Items), tok)
		}
		paged = append(paged, page.Items...)
		if tok = page.NextToken; tok == "" {
			break
		}
	}
	if !reflect.DeepEqual(byStart(paged), want) {
		t.Fatalf("paged %+v", paged)
	}
	if one := listModifications(t, svc, id, 1); !reflect.DeepEqual(one, want) {
		t.Fatalf("pages of one %+v", one)
	}
	if got := listModifications(t, svc, otherID, 0); len(got) != 1 || got[0].Volume != otherID {
		t.Fatalf("other volume lists %+v", got)
	}
	_, err = svc.ListVolumeModifications(ctx, model.NewVolumeID(), model.PageRequest{})
	if !errors.Is(err, model.ErrNotFound) {
		t.Fatalf("missing volume: %v", err)
	}
	_, err = svc.ListVolumeModifications(ctx, id, model.PageRequest{Size: 1001})
	if !errors.Is(err, model.ErrInvalidArgument) {
		t.Fatalf("page size 1001: %v", err)
	}
	if _, err := svc.ListVolumeModifications(ctx, id, model.PageRequest{Token: "not a token"}); err == nil {
		t.Fatal("garbage page token accepted")
	}
}
