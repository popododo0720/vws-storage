package server_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	storagev1 "github.com/popododo0720/vws-proto/gen/go/vws/storage/v1"

	"github.com/popododo0720/vws-storage/internal/backend/fake"
	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/server"
)

// mappedOnly is a fake block backend whose admin mapped only some types.
type mappedOnly struct {
	*fake.Block
	types map[model.VolumeType]bool
}

func (m mappedOnly) ServeType(t model.VolumeType) error {
	if !m.types[t] {
		return fmt.Errorf("no StorageClass is mapped to volume type %s", t)
	}
	return nil
}

// A type with no admin mapping is refused at admission with a clear reason:
// no volume row, no operation and no backend call (vws-storage#123).
func TestUnmappedVolumeTypeIsRefusedAndCreatesNothing(t *testing.T) {
	gp2, err := model.ParseVolumeType("gp2")
	if err != nil {
		t.Fatal(err)
	}
	block := mappedOnly{
		Block: fake.NewBlock(), types: map[model.VolumeType]bool{model.VolumeTypeStandard: true, gp2: true},
	}
	vols, snaps, _, _, ops := dialBlock(t, "", server.Config{}, block)
	ctx := context.Background()
	src, err := vols.CreateVolume(ctx, &storagev1.CreateVolumeRequest{
		Parent: "projects/local/zones/az-1", VolumeId: "mapped",
		Volume: &storagev1.Volume{Spec: &storagev1.VolumeSpec{SizeGib: 1, Type: storagev1.VolumeType_VOLUME_TYPE_GP2}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if done := waitOp(t, ops, src.GetName()); done.GetError() != nil {
		t.Fatal(done.GetError())
	}
	sop, err := snaps.CreateSnapshot(ctx, &storagev1.CreateSnapshotRequest{
		Parent: "projects/local/regions/local", SnapshotId: "snap-1",
		Snapshot: &storagev1.Snapshot{Spec: &storagev1.SnapshotSpec{Volume: "projects/local/zones/az-1/volumes/mapped"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if done := waitOp(t, ops, sop.GetName()); done.GetError() != nil {
		t.Fatal(done.GetError())
	}
	calls := len(block.Calls())
	for name, spec := range map[string]*storagev1.VolumeSpec{
		"blank": {SizeGib: 1, Type: storagev1.VolumeType_VOLUME_TYPE_GP3},
		"io1":   {SizeGib: 4, Type: storagev1.VolumeType_VOLUME_TYPE_IO1, Iops: 200},
		"restore": {
			SizeGib: 125, Type: storagev1.VolumeType_VOLUME_TYPE_SC1,
			Source: &storagev1.VolumeSpec_Snapshot{Snapshot: "projects/local/regions/local/snapshots/snap-1"},
		},
	} {
		_, err := vols.CreateVolume(ctx, &storagev1.CreateVolumeRequest{
			Parent: "projects/local/zones/az-1", VolumeId: "unmapped-" + name,
			RequestId: "33333333-3333-4333-8333-33333333333" + fmt.Sprint(len(name)%10),
			Volume:    &storagev1.Volume{Spec: spec},
		})
		st, _ := status.FromError(err)
		if st.Code() != codes.InvalidArgument || reasonOf(st) != "INVALID_SPEC" ||
			!strings.Contains(st.Message(), "unsupported") || !strings.Contains(st.Message(), "no StorageClass is mapped") {
			t.Errorf("%s: code %v reason %q message %q", name, st.Code(), reasonOf(st), st.Message())
		}
		_, err = vols.GetVolume(ctx, &storagev1.GetVolumeRequest{Name: "projects/local/zones/az-1/volumes/unmapped-" + name})
		if status.Code(err) != codes.NotFound {
			t.Errorf("%s: volume row: %v", name, err)
		}
	}
	if got := len(block.Calls()); got != calls {
		t.Fatalf("backend calls %d want %d", got, calls)
	}
	listed, err := vols.ListVolumes(ctx, &storagev1.ListVolumesRequest{Parent: "projects/local/zones/az-1"})
	if err != nil || len(listed.GetVolumes()) != 1 {
		t.Fatalf("volumes %v err %v", listed.GetVolumes(), err)
	}
}

// During the rollout a pre-#123 console-api still sends the deprecated SSD
// value. It is gp2, so it is served by the gp2 mapping (the legacy
// -kube-class-ssd flag maps gp2) and refused like gp2 when gp2 is unmapped.
func TestDeprecatedSSDUsesTheGP2Mapping(t *testing.T) {
	ssd := storagev1.VolumeType_VOLUME_TYPE_SSD //nolint:staticcheck // a pre-#123 client sends it
	create := func(block mappedOnly) error {
		vols, _, _, _, ops := dialBlock(t, "", server.Config{}, block)
		op, err := vols.CreateVolume(context.Background(), &storagev1.CreateVolumeRequest{
			Parent: "projects/local/zones/az-1", VolumeId: "old-client",
			Volume: &storagev1.Volume{Spec: &storagev1.VolumeSpec{SizeGib: 1, Type: ssd}},
		})
		if err != nil {
			return err //nolint:wrapcheck // the test reads the gRPC status
		}
		if done := waitOp(t, ops, op.GetName()); done.GetError() != nil {
			t.Fatal(done.GetError())
		}
		return nil
	}
	gp2 := mappedOnly{Block: fake.NewBlock(), types: map[model.VolumeType]bool{model.VolumeTypeGP2: true}}
	if err := create(gp2); err != nil {
		t.Fatalf("gp2 mapped: %v", err)
	}
	calls := gp2.Calls()
	if len(calls) != 1 || calls[0].Method != "CreateVolume" {
		t.Fatalf("calls %v", calls)
	}
	stdOnly := mappedOnly{Block: fake.NewBlock(), types: map[model.VolumeType]bool{model.VolumeTypeStandard: true}}
	st, _ := status.FromError(create(stdOnly))
	if st.Code() != codes.InvalidArgument || !strings.Contains(st.Message(), "volume type gp2 is unsupported") {
		t.Fatalf("gp2 unmapped: code %v message %q", st.Code(), st.Message())
	}
	if len(stdOnly.Calls()) != 0 {
		t.Fatalf("calls %v", stdOnly.Calls())
	}
}
