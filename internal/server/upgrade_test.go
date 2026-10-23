package server_test

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	commonv1 "github.com/popododo0720/vws-proto/gen/go/vws/common/v1"
	storagev1 "github.com/popododo0720/vws-proto/gen/go/vws/storage/v1"

	"github.com/popododo0720/vws-storage/internal/backend/fake"
	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/server"
	"github.com/popododo0720/vws-storage/internal/storage"
)

// fixture010 is the SQLite file the 0.1.0 build wrote
// (testdata/upgrade/0.1.0/README.md). The requests below are the ones
// testdata/upgrade/0.1.0/gen/gen_test.go sent to that build.
const (
	fixture010   = "../../testdata/upgrade/0.1.0/storage.db"
	upgradeZone  = "projects/a123456789012/zones/vws-1a"
	upgradeRgn   = "projects/a123456789012/regions/vws-1"
	upgradeOps   = "projects/a123456789012/operations/"
	upgradeVol1  = upgradeZone + "/volumes/vol-0000000000000001a"
	upgradeVol2  = upgradeZone + "/volumes/vol-0000000000000002b"
	upgradeVol3  = upgradeZone + "/volumes/vol-0000000000000003c"
	upgradeSnap1 = upgradeRgn + "/snapshots/snap-0000000000000001a"
	standard     = storagev1.VolumeType_VOLUME_TYPE_STANDARD
)

var teamLabel = map[string]string{"team": "storage"}

// openFixture010 serves a copy of the 0.1.0 file with the current code. The
// copy lives in t.TempDir, so the fixture is never written.
func openFixture010(t *testing.T) (
	*storage.Service, storagev1.VolumeServiceClient, storagev1.SnapshotServiceClient, longrunningpb.OperationsClient,
) {
	t.Helper()
	raw, err := os.ReadFile(fixture010)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "storage.db")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	zone, err := model.ParseZone("vws-1a")
	if err != nil {
		t.Fatal(err)
	}
	objs := fake.NewObjects(fake.ObjectOptions{})
	svc, err := storage.Open(context.Background(), storage.Options{
		Store: storage.SQLite(path), Block: fake.NewBlock(), Object: objs, Signer: objs,
		Zones: []model.Zone{zone},
	})
	if err != nil {
		t.Fatalf("open 0.1.0 file: %v", err)
	}
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer(server.Options()...)
	server.Register(gs, svc, server.Config{})
	go func() { _ = gs.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		conn.Close()
		gs.Stop()
		svc.Close()
	})
	return svc, storagev1.NewVolumeServiceClient(conn), storagev1.NewSnapshotServiceClient(conn),
		longrunningpb.NewOperationsClient(conn)
}

// Records written by 0.1.0 decode with the current code, keep their values,
// and are served by Get and List (#129). Renaming or re-encoding a stored
// field breaks one of these checks.
func TestUpgradeFrom010DecodesStoredRecords(t *testing.T) {
	svc, vols, snaps, ops := openFixture010(t)
	ctx := context.Background()
	want := []struct {
		name   string
		size   int64
		typ    storagev1.VolumeType
		mtype  model.VolumeType
		labels map[string]string
		op     string
	}{
		{upgradeVol1, 8, standard, model.VolumeTypeStandard, teamLabel, "op-bfe297985c75b4c3a531"},
		// Stored as "ssd" by 0.1.0; it reads as gp2 (#123).
		{upgradeVol2, 16, storagev1.VolumeType_VOLUME_TYPE_GP2, model.VolumeTypeGP2, nil, "op-ee409fb8ed4722576195"},
		{upgradeVol3, 2000, standard, model.VolumeTypeStandard, nil, "op-395b8bb42cb203f7e45e"},
	}
	for _, w := range want {
		v, err := vols.GetVolume(ctx, &storagev1.GetVolumeRequest{Name: w.name})
		if err != nil {
			t.Fatalf("get %s: %v", w.name, err)
		}
		spec := v.GetSpec()
		if spec.GetSizeGib() != w.size || spec.GetType() != w.typ || spec.GetIops() != 0 || spec.GetThroughputMibps() != 0 {
			t.Errorf("%s spec %+v", w.name, spec)
		}
		if v.GetStatus().GetObserved().GetState() != commonv1.ReconciliationState_RECONCILIATION_STATE_READY {
			t.Errorf("%s status %+v", w.name, v.GetStatus())
		}
		if len(v.GetLabels()) != len(w.labels) || v.GetLabels()["team"] != w.labels["team"] {
			t.Errorf("%s labels %v want %v", w.name, v.GetLabels(), w.labels)
		}
		id, err := model.ParseVolumeID(filepath.Base(w.name))
		if err != nil {
			t.Fatal(err)
		}
		project, err := model.ParseProject("a123456789012")
		if err != nil {
			t.Fatal(err)
		}
		mv, err := svc.Volume(ctx, id.WithProject(project))
		if err != nil {
			t.Fatal(err)
		}
		// 0.1.0 stored no IOPS or throughput; standard and gp2 take none.
		ms := mv.Spec
		if ms.Size != model.Bytes(w.size)*model.GiB || ms.Type != w.mtype || ms.Iops != 0 || ms.Throughput != 0 ||
			ms.WithDefaultPerformance() != ms || ms.Zone.String() != "vws-1a" || ms.Source != model.NoSource() {
			t.Errorf("%s model spec %+v", w.name, mv.Spec)
		}
		op, err := ops.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: upgradeOps + w.op})
		if err != nil || !op.GetDone() || op.GetError() != nil {
			t.Errorf("%s create operation %+v err %v", w.name, op, err)
		}
	}
	list, err := vols.ListVolumes(ctx, &storagev1.ListVolumesRequest{Parent: upgradeZone})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, v := range list.GetVolumes() {
		got = append(got, v.GetName())
	}
	if len(got) != len(want) || got[0] != upgradeVol1 || got[1] != upgradeVol2 || got[2] != upgradeVol3 {
		t.Errorf("list volumes %v", got)
	}

	sn, err := snaps.GetSnapshot(ctx, &storagev1.GetSnapshotRequest{Name: upgradeSnap1})
	if err != nil {
		t.Fatal(err)
	}
	if sn.GetSpec().GetVolume() != upgradeVol1 || sn.GetSpec().GetDescription() != "fixture" ||
		sn.GetStatus().GetObserved().GetState() != commonv1.ReconciliationState_RECONCILIATION_STATE_READY {
		t.Errorf("snapshot %+v", sn)
	}
	slist, err := snaps.ListSnapshots(ctx, &storagev1.ListSnapshotsRequest{Parent: upgradeRgn})
	if err != nil {
		t.Fatal(err)
	}
	if len(slist.GetSnapshots()) != 1 || slist.GetSnapshots()[0].GetName() != upgradeSnap1 {
		t.Errorf("list snapshots %v", slist.GetSnapshots())
	}
	op, err := ops.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: upgradeOps + "op-7195b0929250b1cfcd89"})
	if err != nil || !op.GetDone() || op.GetError() != nil {
		t.Errorf("snapshot operation %+v err %v", op, err)
	}
}

// A create sent to 0.1.0 and sent again with the same request_id after the
// upgrade is the same operation, not a new one or a conflict. The gp2 volume
// replays both as the old SSD enum and as GP2, since "ssd" is gp2 (#123).
// The over-1024 GiB standard volume is #126 and is not replayed here.
func TestUpgradeFrom010ReplaysStoredTokens(t *testing.T) {
	_, vols, snaps, _ := openFixture010(t)
	ctx := context.Background()
	const (
		vol1, tok1 = "vol-0000000000000001a", "11111111-1111-4111-8111-111111111111"
		vol2, tok2 = "vol-0000000000000002b", "22222222-2222-4222-8222-222222222222"
	)
	// The 0.1.0 client sent the now-deprecated SSD value; its replay does too.
	ssd := storagev1.VolumeType_VOLUME_TYPE_SSD //nolint:staticcheck // the replay must send what 0.1.0 got
	type createReq = storagev1.CreateVolumeRequest
	create := func(id, tok string, size int64, typ storagev1.VolumeType, labels map[string]string) *createReq {
		return &createReq{
			Parent: upgradeZone, VolumeId: id, RequestId: tok,
			Volume: &storagev1.Volume{Labels: labels, Spec: &storagev1.VolumeSpec{SizeGib: size, Type: typ}},
		}
	}
	cases := []struct {
		name string
		req  *storagev1.CreateVolumeRequest
		op   string
	}{
		{"standard", create(vol1, tok1, 8, standard, teamLabel), "op-bfe297985c75b4c3a531"},
		{"ssd", create(vol2, tok2, 16, ssd, nil), "op-ee409fb8ed4722576195"},
		{"gp2", create(vol2, tok2, 16, storagev1.VolumeType_VOLUME_TYPE_GP2, nil), "op-ee409fb8ed4722576195"},
	}
	for _, c := range cases {
		op, err := vols.CreateVolume(ctx, c.req)
		if err != nil {
			t.Errorf("%s replay: %v", c.name, err)
			continue
		}
		if op.GetName() != upgradeOps+c.op || !op.GetDone() || op.GetError() != nil {
			t.Errorf("%s replay %s done %v err %v, want %s", c.name, op.GetName(), op.GetDone(), op.GetError(), c.op)
		}
	}
	op, err := snaps.CreateSnapshot(ctx, &storagev1.CreateSnapshotRequest{
		Parent: upgradeRgn, SnapshotId: "snap-0000000000000001a", RequestId: "44444444-4444-4444-8444-444444444444",
		Snapshot: &storagev1.Snapshot{Spec: &storagev1.SnapshotSpec{Volume: upgradeVol1, Description: "fixture"}},
	})
	if err != nil {
		t.Fatalf("snapshot replay: %v", err)
	}
	if op.GetName() != upgradeOps+"op-7195b0929250b1cfcd89" || !op.GetDone() {
		t.Errorf("snapshot replay %s done %v", op.GetName(), op.GetDone())
	}
}
