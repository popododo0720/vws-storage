package server_test

import (
	"context"
	"testing"

	storagev1 "github.com/popododo0720/vws-proto/gen/go/vws/storage/v1"
)

// A pre-#123 client sends the deprecated SSD value. It is the gp2 volume it
// always was, and a replay of the request as GP2 is the same request.
func TestDeprecatedSSDCreatesGP2(t *testing.T) {
	vols, _, _, _, ops := dial(t, "")
	ctx := context.Background()
	req := &storagev1.CreateVolumeRequest{
		Parent: "projects/local/zones/az-1", VolumeId: "old-client",
		RequestId: "22222222-2222-4222-8222-222222222222",
		Volume: &storagev1.Volume{Spec: &storagev1.VolumeSpec{
			SizeGib: 1,
			Type:    storagev1.VolumeType_VOLUME_TYPE_SSD, //nolint:staticcheck // a pre-#123 client sends it
		}},
	}
	first, err := vols.CreateVolume(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if done := waitOp(t, ops, first.GetName()); done.GetError() != nil {
		t.Fatal(done.GetError())
	}
	req.Volume.Spec.Type = storagev1.VolumeType_VOLUME_TYPE_GP2
	again, err := vols.CreateVolume(ctx, req)
	if err != nil || again.GetName() != first.GetName() {
		t.Fatalf("replay as gp2: %v name %s want %s", err, again.GetName(), first.GetName())
	}
	vol, err := vols.GetVolume(ctx, &storagev1.GetVolumeRequest{Name: "projects/local/zones/az-1/volumes/old-client"})
	if err != nil {
		t.Fatal(err)
	}
	if vol.GetSpec().GetType() != storagev1.VolumeType_VOLUME_TYPE_GP2 {
		t.Fatalf("type %v", vol.GetSpec().GetType())
	}
}
