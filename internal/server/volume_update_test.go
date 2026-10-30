package server_test

import (
	"context"
	"testing"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	storagev1 "github.com/popododo0720/vws-proto/gen/go/vws/storage/v1"
)

// updateVolume sends one UpdateVolume with the given mask, waits for the
// operation and returns the volume read back (#87).
func updateVolume(t *testing.T, vols storagev1.VolumeServiceClient, ops longrunningpb.OperationsClient,
	vol *storagev1.Volume, paths ...string,
) (*storagev1.Volume, error) {
	t.Helper()
	ctx := context.Background()
	uop, err := vols.UpdateVolume(ctx, &storagev1.UpdateVolumeRequest{
		Volume: vol, UpdateMask: &fieldmaskpb.FieldMask{Paths: paths},
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // the test reads the gRPC status
	}
	if done := waitOp(t, ops, uop.GetName()); done.GetError() != nil {
		t.Fatalf("operation error %v", done.GetError())
	}
	return vols.GetVolume(ctx, &storagev1.GetVolumeRequest{Name: vol.GetName()}) //nolint:wrapcheck // as above
}

func wantSpec(t *testing.T, step string, vol *storagev1.Volume, err error,
	gib int64, typ storagev1.VolumeType, iops, mibs int64,
) {
	t.Helper()
	s := vol.GetSpec()
	if err != nil || s.GetSizeGib() != gib || s.GetType() != typ || s.GetIops() != iops || s.GetThroughputMibps() != mibs {
		t.Fatalf("%s: spec %v err %v", step, s, err)
	}
}

// Size, type, IOPS, throughput and labels change in one UpdateVolume, as
// console-api maps one EC2 ModifyVolume (#87, console-api#324 pick A).
func TestUpdateVolumeAppliesSizeTypeAndPerformanceInOneMask(t *testing.T) {
	vols, _, _, _, ops := dial(t, "")
	name := createGP2(t, vols, ops, "combined", 100)
	vol, err := updateVolume(t, vols, ops, &storagev1.Volume{
		Name: name, Labels: map[string]string{"team": "storage"},
		Spec: &storagev1.VolumeSpec{
			SizeGib: 200, Type: storagev1.VolumeType_VOLUME_TYPE_GP3, Iops: 16000, ThroughputMibps: 1000,
		},
	}, "spec.size_gib", "spec.type", "spec.iops", "spec.throughput_mibps", "labels")
	wantSpec(t, "gp2 100 to gp3 200", vol, err, 200, storagev1.VolumeType_VOLUME_TYPE_GP3, 16000, 1000)
	if vol.GetLabels()["team"] != "storage" || vol.GetStatus().GetSizeGib() != 200 && vol.GetStatus().GetSizeGib() != 0 {
		t.Fatalf("labels %v status %v", vol.GetLabels(), vol.GetStatus())
	}

	// st1 needs 125 GiB: a 10 GiB volume takes it only with the size in the
	// same mask, which is checked against the new size.
	small := createGP2(t, vols, ops, "small", 10)
	_, err = updateVolume(t, vols, ops, &storagev1.Volume{
		Name: small, Spec: &storagev1.VolumeSpec{Type: storagev1.VolumeType_VOLUME_TYPE_ST1},
	}, "spec.type")
	if st, _ := status.FromError(err); st.Code() != codes.InvalidArgument {
		t.Fatalf("10 GiB to st1: %v", err)
	}
	vol, err = updateVolume(t, vols, ops, &storagev1.Volume{
		Name: small, Spec: &storagev1.VolumeSpec{SizeGib: 125, Type: storagev1.VolumeType_VOLUME_TYPE_ST1},
	}, "spec.size_gib", "spec.type")
	wantSpec(t, "gp2 10 to st1 125", vol, err, 125, storagev1.VolumeType_VOLUME_TYPE_ST1, 0, 0)

	// io1 at 5000 IOPS needs 100 GiB (50 per GiB): size and IOPS together.
	vol, err = updateVolume(t, vols, ops, &storagev1.Volume{
		Name: small, Spec: &storagev1.VolumeSpec{SizeGib: 130, Type: storagev1.VolumeType_VOLUME_TYPE_IO1, Iops: 5000},
	}, "spec.size_gib", "spec.type", "spec.iops")
	wantSpec(t, "st1 to io1 5000", vol, err, 130, storagev1.VolumeType_VOLUME_TYPE_IO1, 5000, 0)
}

// spec.iops and spec.throughput_mibps change performance alone and keep the
// rest; a zero value in the mask takes the type's EC2 default (#87).
func TestUpdateVolumeSetsIopsAndThroughput(t *testing.T) {
	vols, _, _, _, ops := dial(t, "")
	name := createGP2(t, vols, ops, "perf", 200)
	vol, err := updateVolume(t, vols, ops, &storagev1.Volume{
		Name: name, Spec: &storagev1.VolumeSpec{Type: storagev1.VolumeType_VOLUME_TYPE_GP3},
	}, "spec.type")
	wantSpec(t, "gp2 to gp3", vol, err, 200, storagev1.VolumeType_VOLUME_TYPE_GP3, 3000, 125)
	vol, err = updateVolume(t, vols, ops, &storagev1.Volume{
		Name: name, Spec: &storagev1.VolumeSpec{Iops: 12000},
	}, "spec.iops")
	wantSpec(t, "iops 12000", vol, err, 200, storagev1.VolumeType_VOLUME_TYPE_GP3, 12000, 125)
	vol, err = updateVolume(t, vols, ops, &storagev1.Volume{
		Name: name, Spec: &storagev1.VolumeSpec{ThroughputMibps: 750},
	}, "spec.throughput_mibps")
	wantSpec(t, "throughput 750", vol, err, 200, storagev1.VolumeType_VOLUME_TYPE_GP3, 12000, 750)
	vol, err = updateVolume(t, vols, ops, &storagev1.Volume{
		Name: name, Spec: &storagev1.VolumeSpec{Iops: 4000, ThroughputMibps: 0},
	}, "spec.iops", "spec.throughput_mibps")
	wantSpec(t, "iops 4000 and default throughput", vol, err, 200, storagev1.VolumeType_VOLUME_TYPE_GP3, 4000, 125)

	// The same values again change nothing and keep the etag.
	before, err := vols.GetVolume(context.Background(), &storagev1.GetVolumeRequest{Name: name})
	if err != nil {
		t.Fatal(err)
	}
	vol, err = updateVolume(t, vols, ops, &storagev1.Volume{
		Name: name, Spec: &storagev1.VolumeSpec{Iops: 4000, ThroughputMibps: 125},
	}, "spec.iops", "spec.throughput_mibps")
	if err != nil || vol.GetEtag() != before.GetEtag() {
		t.Fatalf("repeat changed the volume: etag %q to %q err %v", before.GetEtag(), vol.GetEtag(), err)
	}
}

// A request_id replays its operation; validate_only changes nothing.
func TestUpdateVolumeCombinedIsIdempotentAndValidates(t *testing.T) {
	vols, _, _, _, ops := dial(t, "")
	ctx := context.Background()
	name := createGP2(t, vols, ops, "idem", 100)
	req := &storagev1.UpdateVolumeRequest{
		Volume: &storagev1.Volume{Name: name, Spec: &storagev1.VolumeSpec{
			SizeGib: 150, Type: storagev1.VolumeType_VOLUME_TYPE_GP3, Iops: 6000,
		}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"spec.size_gib", "spec.type", "spec.iops"}},
		RequestId:  "87878787-8787-4787-8787-878787878787",
	}
	check := &storagev1.UpdateVolumeRequest{Volume: req.GetVolume(), UpdateMask: req.GetUpdateMask(), ValidateOnly: true}
	if _, err := vols.UpdateVolume(ctx, check); err != nil {
		t.Fatal(err)
	}
	vol, err := vols.GetVolume(ctx, &storagev1.GetVolumeRequest{Name: name})
	wantSpec(t, "validate_only", vol, err, 100, storagev1.VolumeType_VOLUME_TYPE_GP2, 0, 0)
	first, err := vols.UpdateVolume(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	waitOp(t, ops, first.GetName())
	again, err := vols.UpdateVolume(ctx, req)
	if err != nil || again.GetName() != first.GetName() {
		t.Fatalf("replay %q want %q err %v", again.GetName(), first.GetName(), err)
	}
	vol, err = vols.GetVolume(ctx, &storagev1.GetVolumeRequest{Name: name})
	wantSpec(t, "after replay", vol, err, 150, storagev1.VolumeType_VOLUME_TYPE_GP3, 6000, 125)
}

// Combinations EC2 ModifyVolume refuses are InvalidArgument and leave the
// volume as it was.
func TestUpdateVolumeRefusesIllegalTypeAndPerformance(t *testing.T) {
	vols, _, _, _, ops := dial(t, "")
	ctx := context.Background()
	name := createGP2(t, vols, ops, "bad", 100)
	const (
		io1  = storagev1.VolumeType_VOLUME_TYPE_IO1
		gp3  = storagev1.VolumeType_VOLUME_TYPE_GP3
		std  = storagev1.VolumeType_VOLUME_TYPE_STANDARD
		typ  = "spec.type"
		iops = "spec.iops"
		mibs = "spec.throughput_mibps"
		size = "spec.size_gib"
	)
	type spec = storagev1.VolumeSpec
	for _, tc := range []struct {
		step  string
		spec  *spec
		paths []string
	}{
		{"iops on gp2", &spec{Iops: 3000}, []string{iops}},
		{"throughput on gp2", &spec{ThroughputMibps: 125}, []string{mibs}},
		{"io1 with zero iops", &spec{Type: io1}, []string{typ, iops}},
		{"io1 with throughput", &spec{Type: io1, Iops: 3000, ThroughputMibps: 125}, []string{typ, iops, mibs}},
		{"io1 over 50 per GiB", &spec{Type: io1, Iops: 5001}, []string{typ, iops}},
		{"gp3 under 3000", &spec{Type: gp3, Iops: 2999}, []string{typ, iops}},
		{"gp3 over 0.25 MiB/s per iops", &spec{Type: gp3, ThroughputMibps: 751}, []string{typ, mibs}},
		{"negative iops", &spec{Type: gp3, Iops: -1}, []string{typ, iops}},
		{"shrink with a type", &spec{SizeGib: 50, Type: gp3}, []string{size, typ}},
		{"standard over 1024 GiB", &spec{SizeGib: 1025, Type: std}, []string{size, typ}},
		{"unknown path", &spec{}, []string{iops, "spec.zone"}},
	} {
		_, err := vols.UpdateVolume(ctx, &storagev1.UpdateVolumeRequest{
			Volume:     &storagev1.Volume{Name: name, Spec: tc.spec},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: tc.paths},
		})
		st, _ := status.FromError(err)
		want := codes.InvalidArgument
		if tc.step == "unknown path" {
			want = codes.Unimplemented
		}
		if st.Code() != want {
			t.Errorf("%s: code %v err %v", tc.step, st.Code(), err)
		}
	}
	vol, err := vols.GetVolume(ctx, &storagev1.GetVolumeRequest{Name: name})
	wantSpec(t, "after refusals", vol, err, 100, storagev1.VolumeType_VOLUME_TYPE_GP2, 0, 0)
}
