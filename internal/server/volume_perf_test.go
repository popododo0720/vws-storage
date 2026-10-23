package server_test

import (
	"context"
	"fmt"
	"testing"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	storagev1 "github.com/popododo0720/vws-proto/gen/go/vws/storage/v1"
)

// Every EBS type of vws-proto v0.5.0 is created, stored and read back under
// its own value, with gp3's omitted IOPS and throughput filled as EC2 fills
// them (vws-storage#123).
func TestEveryEBSVolumeTypeIsCreatedAndReadBack(t *testing.T) {
	vols, _, _, _, ops := dial(t, "")
	ctx := context.Background()
	for i, tc := range []struct {
		typ              storagev1.VolumeType
		iops, mibs       int64
		wantIops, wantMb int64
	}{
		{storagev1.VolumeType_VOLUME_TYPE_STANDARD, 0, 0, 0, 0},
		{storagev1.VolumeType_VOLUME_TYPE_GP2, 0, 0, 0, 0},
		{storagev1.VolumeType_VOLUME_TYPE_GP3, 0, 0, 3000, 125},
		{storagev1.VolumeType_VOLUME_TYPE_GP3, 16000, 1000, 16000, 1000},
		{storagev1.VolumeType_VOLUME_TYPE_IO1, 6250, 0, 6250, 0},     // 50 per GiB of 125
		{storagev1.VolumeType_VOLUME_TYPE_IO2, 125000, 0, 125000, 0}, // 1000 per GiB of 125
		{storagev1.VolumeType_VOLUME_TYPE_ST1, 0, 0, 0, 0},
		{storagev1.VolumeType_VOLUME_TYPE_SC1, 0, 0, 0, 0},
	} {
		id := fmt.Sprintf("v-%d", i)
		op, err := vols.CreateVolume(ctx, &storagev1.CreateVolumeRequest{
			Parent: "projects/local/zones/az-1", VolumeId: id,
			Volume: &storagev1.Volume{Spec: &storagev1.VolumeSpec{
				SizeGib: 125, Type: tc.typ, Iops: tc.iops, ThroughputMibps: tc.mibs,
			}},
		})
		if err != nil {
			t.Fatalf("%s: %v", tc.typ, err)
		}
		if done := waitOp(t, ops, op.GetName()); done.GetError() != nil {
			t.Fatalf("%s: %v", tc.typ, done.GetError())
		}
		vol, err := vols.GetVolume(ctx, &storagev1.GetVolumeRequest{Name: "projects/local/zones/az-1/volumes/" + id})
		if err != nil {
			t.Fatal(err)
		}
		s := vol.GetSpec()
		if s.GetType() != tc.typ || s.GetIops() != tc.wantIops || s.GetThroughputMibps() != tc.wantMb {
			t.Errorf("%s: read back %v iops %d throughput %d", tc.typ, s.GetType(), s.GetIops(), s.GetThroughputMibps())
		}
	}
}

// IOPS and throughput outside the per-type rules are InvalidArgument and
// create nothing.
func TestVolumePerformanceOutsideTheTypeRulesIsInvalid(t *testing.T) {
	vols, _, _, _, _ := dial(t, "")
	ctx := context.Background()
	for i, tc := range []struct {
		typ        storagev1.VolumeType
		iops, mibs int64
	}{
		{storagev1.VolumeType_VOLUME_TYPE_GP3, 2999, 0},
		{storagev1.VolumeType_VOLUME_TYPE_GP3, 80001, 0},
		{storagev1.VolumeType_VOLUME_TYPE_GP3, 0, 124},
		{storagev1.VolumeType_VOLUME_TYPE_GP3, 0, 2001},
		{storagev1.VolumeType_VOLUME_TYPE_GP3, -1, 0},
		{storagev1.VolumeType_VOLUME_TYPE_GP3, 3000, 751}, // over 0.25 MiB/s per IOPS
		{storagev1.VolumeType_VOLUME_TYPE_GP3, 0, 751},    // the same after the 3000 IOPS default
		{storagev1.VolumeType_VOLUME_TYPE_GP3, 62501, 0},  // over 500 IOPS per GiB of 125 GiB
		{storagev1.VolumeType_VOLUME_TYPE_IO1, 6251, 0},   // over 50 IOPS per GiB
		{storagev1.VolumeType_VOLUME_TYPE_IO2, 125001, 0}, // over 1000 IOPS per GiB
		{storagev1.VolumeType_VOLUME_TYPE_IO1, 0, 0},
		{storagev1.VolumeType_VOLUME_TYPE_IO1, 99, 0},
		{storagev1.VolumeType_VOLUME_TYPE_IO1, 64001, 0},
		{storagev1.VolumeType_VOLUME_TYPE_IO1, 1000, 125},
		{storagev1.VolumeType_VOLUME_TYPE_IO2, 0, 0},
		{storagev1.VolumeType_VOLUME_TYPE_IO2, 256001, 0},
		{storagev1.VolumeType_VOLUME_TYPE_GP2, 100, 0},
		{storagev1.VolumeType_VOLUME_TYPE_GP2, 0, 125},
		{storagev1.VolumeType_VOLUME_TYPE_SSD, 100, 0}, //nolint:staticcheck // a pre-#123 client
		{storagev1.VolumeType_VOLUME_TYPE_STANDARD, 100, 0},
		{storagev1.VolumeType_VOLUME_TYPE_ST1, 0, 125},
		{storagev1.VolumeType_VOLUME_TYPE_SC1, 100, 0},
	} {
		_, err := vols.CreateVolume(ctx, &storagev1.CreateVolumeRequest{
			Parent: "projects/local/zones/az-1", VolumeId: fmt.Sprintf("bad-%d", i),
			Volume: &storagev1.Volume{Spec: &storagev1.VolumeSpec{
				SizeGib: 125, Type: tc.typ, Iops: tc.iops, ThroughputMibps: tc.mibs,
			}},
		})
		st, _ := status.FromError(err)
		if st.Code() != codes.InvalidArgument || reasonOf(st) != "INVALID_SPEC" {
			t.Errorf("%s iops=%d throughput=%d: code %v reason %q err %v",
				tc.typ, tc.iops, tc.mibs, st.Code(), reasonOf(st), err)
		}
	}
	listed, err := vols.ListVolumes(ctx, &storagev1.ListVolumesRequest{Parent: "projects/local/zones/az-1"})
	if err != nil {
		t.Fatal(err)
	}
	if n := len(listed.GetVolumes()); n != 0 {
		t.Fatalf("%d volumes created", n)
	}
}

// UpdateVolume spec.type re-derives IOPS and throughput for the new type.
// Moving to io1 or io2 without an in-range stored IOPS is InvalidArgument,
// because UpdateVolume carries no IOPS.
func TestUpdateVolumeTypeRederivesPerformance(t *testing.T) {
	vols, _, _, _, ops := dial(t, "")
	name := createGP2(t, vols, ops, "retype", 200)
	retype := func(typ storagev1.VolumeType) (*storagev1.Volume, error) {
		return retypeVolume(t, vols, ops, name, typ)
	}
	want := func(step string, vol *storagev1.Volume, err error, iops, mibs int64) {
		t.Helper()
		if err != nil || vol.GetSpec().GetIops() != iops || vol.GetSpec().GetThroughputMibps() != mibs {
			t.Fatalf("%s: %v err %v", step, vol.GetSpec(), err)
		}
	}
	vol, err := retype(storagev1.VolumeType_VOLUME_TYPE_IO1)
	want("gp2 to io1", vol, err, 3000, 0)
	vol, err = retype(storagev1.VolumeType_VOLUME_TYPE_GP3)
	want("io1 to gp3", vol, err, 3000, 125)
	vol, err = retype(storagev1.VolumeType_VOLUME_TYPE_IO2)
	want("gp3 to io2", vol, err, 3000, 0)
	vol, err = retype(storagev1.VolumeType_VOLUME_TYPE_ST1)
	want("io2 to st1", vol, err, 0, 0)
	vol, err = retype(storagev1.VolumeType_VOLUME_TYPE_IO1)
	want("st1 to io1", vol, err, 3000, 0)

	// 3000 IOPS is over io1's 50 per GiB of 10 GiB: EC2 refuses the default.
	small := createGP2(t, vols, ops, "small", 10)
	_, err = retypeVolume(t, vols, ops, small, storagev1.VolumeType_VOLUME_TYPE_IO1)
	if st, _ := status.FromError(err); st.Code() != codes.InvalidArgument || reasonOf(st) != "INVALID_SPEC" {
		t.Fatalf("10 GiB gp2 to io1: %v", err)
	}
}

// createGP2 creates a gp2 volume of gib GiB, waits for it and returns its name.
func createGP2(t *testing.T, vols storagev1.VolumeServiceClient, ops longrunningpb.OperationsClient,
	id string, gib int64,
) string {
	t.Helper()
	op, err := vols.CreateVolume(context.Background(), &storagev1.CreateVolumeRequest{
		Parent: "projects/local/zones/az-1", VolumeId: id,
		Volume: &storagev1.Volume{Spec: &storagev1.VolumeSpec{SizeGib: gib, Type: storagev1.VolumeType_VOLUME_TYPE_GP2}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if done := waitOp(t, ops, op.GetName()); done.GetError() != nil {
		t.Fatal(done.GetError())
	}
	return "projects/local/zones/az-1/volumes/" + id
}

// retypeVolume sets the volume's type with UpdateVolume spec.type, waits for
// the operation and returns the volume read back.
func retypeVolume(t *testing.T, vols storagev1.VolumeServiceClient, ops longrunningpb.OperationsClient,
	name string, typ storagev1.VolumeType,
) (*storagev1.Volume, error) {
	t.Helper()
	ctx := context.Background()
	cur, err := vols.GetVolume(ctx, &storagev1.GetVolumeRequest{Name: name})
	if err != nil {
		t.Fatal(err)
	}
	uop, err := vols.UpdateVolume(ctx, &storagev1.UpdateVolumeRequest{
		Volume:     &storagev1.Volume{Name: name, Etag: cur.GetEtag(), Spec: &storagev1.VolumeSpec{Type: typ}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"spec.type"}},
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // the test reads the gRPC status
	}
	if done := waitOp(t, ops, uop.GetName()); done.GetError() != nil {
		t.Fatal(done.GetError())
	}
	return vols.GetVolume(ctx, &storagev1.GetVolumeRequest{Name: name}) //nolint:wrapcheck // as above
}
