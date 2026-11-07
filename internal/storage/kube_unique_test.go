package storage_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/popododo0720/vws-storage/internal/model"
)

// race submits n requests of one kind at once, alternating between the replicas, and
// returns the operations that were admitted. Every refusal must be
// ALREADY_EXISTS.
func race(t *testing.T, rs []*replica, kind string, n int, req func(i int) model.Request) []model.OperationID {
	t.Helper()
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		won   []model.OperationID
		other []error
	)
	start := make(chan struct{})
	for i := range n {
		svc, tok, r := rs[i%len(rs)].svc, token(t, fmt.Sprintf("%s-%d", kind, i)), req(i)
		wg.Go(func() {
			<-start
			op, err := svc.Submit(context.Background(), tok, r)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				won = append(won, op.ID)
			case !errors.Is(err, model.ErrAlreadyExists):
				other = append(other, err)
			}
		})
	}
	close(start)
	wg.Wait()
	if len(other) != 0 {
		t.Fatalf("refusals other than ALREADY_EXISTS: %v", other)
	}
	return won
}

// Two replicas on one Kubernetes store never give two resources the same
// volume ID, snapshot ID or bucket name: concurrent creates of one name are
// admitted exactly once (#141 AC 3). The admitted count is the proof: the
// head compare-and-swap serializes admissions, and the record count alone
// would hold even without it, because ConfigMap names are unique.
func TestKubeStoreGivesAnIDOrBucketNameToOneResource(t *testing.T) {
	path, ns := kubeEnv(t)
	rs := []*replica{startReplica(t, path, ns, "a"), startReplica(t, path, ns, "b")}
	const n = 12

	id, err := model.ParseVolumeID("projects/a111111111111/volumes/race")
	if err != nil {
		t.Fatal(err)
	}
	vols := race(t, rs, "volume", n, func(i int) model.Request {
		req := volumeReq(t)
		req.ID = id
		req.Spec.Size = model.Bytes(i+1) * model.GiB
		return req
	})
	if len(vols) != 1 {
		t.Fatalf("volume %s admitted %d times, want 1", id, len(vols))
	}
	if got := records(t, ns, "volume"); got != 1 {
		t.Fatalf("volume ConfigMaps %d, want 1", got)
	}

	source := volumeReq(t)
	if source.ID, err = model.ParseVolumeID("projects/a111111111111/volumes/source"); err != nil {
		t.Fatal(err)
	}
	src := submitDone(t, rs[0].svc, "source", source).Target().(model.VolumeID)
	snapID, err := model.ParseSnapshotID("projects/a111111111111/snapshots/race")
	if err != nil {
		t.Fatal(err)
	}
	snaps := race(t, rs, "snapshot", n, func(i int) model.Request {
		return model.CreateSnapshotRequest{Source: src, ID: snapID, Description: fmt.Sprintf("race-%d", i)}
	})
	if len(snaps) != 1 {
		t.Fatalf("snapshot %s admitted %d times, want 1", snapID, len(snaps))
	}
	if got := records(t, ns, "snapshot"); got != 1 {
		t.Fatalf("snapshot ConfigMaps %d, want 1", got)
	}

	bkts := race(t, rs, "bucket", n, func(i int) model.Request {
		return model.CreateBucketRequest{Bucket: bucketIn(t, fmt.Sprintf("a%012d", i+1), "race")}
	})
	if len(bkts) != 1 {
		t.Fatalf("bucket race admitted %d times, want 1", len(bkts))
	}
	if got := records(t, ns, "bucket"); got != 1 {
		t.Fatalf("bucket ConfigMaps %d, want 1", got)
	}

	for _, op := range append(append(vols, snaps...), bkts...) {
		if got := waitDone(t, rs[0].svc, op); got.Phase() != model.PhaseSucceeded {
			t.Fatalf("winner %s: %s %+v", op, got.Phase(), got.Err())
		}
	}
}
