package server_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	commonv1 "github.com/popododo0720/vws-proto/gen/go/vws/common/v1"
	storagev1 "github.com/popododo0720/vws-proto/gen/go/vws/storage/v1"

	"github.com/popododo0720/vws-storage/internal/backend"
	"github.com/popododo0720/vws-storage/internal/backend/fake"
	"github.com/popododo0720/vws-storage/internal/server"
)

// GetVolume and ListVolumes show a volume whose create failed permanently as
// RECONCILIATION_STATE_ERROR with the reason in observed.message.
func TestVolumeInErrorShowsStateAndReason(t *testing.T) {
	blk := fake.NewBlock()
	blk.FailNext("CreateVolume", backend.Permanent(fmt.Errorf("%w: the CDI DataVolume failed 4 times (retry limit 3)", backend.ErrGaveUp)))
	vols, _, _, _, ops := dialBlock(t, "", server.Config{}, blk)
	ctx := context.Background()
	op, err := vols.CreateVolume(ctx, &storagev1.CreateVolumeRequest{
		Parent: "projects/local/zones/az-1", VolumeId: "data-err",
		Volume: &storagev1.Volume{Spec: &storagev1.VolumeSpec{SizeGib: 1, Type: storagev1.VolumeType_VOLUME_TYPE_SSD}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if done := waitOp(t, ops, op.GetName()); done.GetError() == nil {
		t.Fatal("operation succeeded")
	}
	check := func(where string, v *storagev1.Volume) {
		t.Helper()
		obs := v.GetStatus().GetObserved()
		if obs.GetState() != commonv1.ReconciliationState_RECONCILIATION_STATE_ERROR || !strings.Contains(obs.GetMessage(), "failed 4 times") {
			t.Fatalf("%s: observed %+v", where, obs)
		}
		if v.GetStatus().GetClaimName() != "" {
			t.Fatalf("%s: claim name %q on a volume in error", where, v.GetStatus().GetClaimName())
		}
	}
	vol, err := vols.GetVolume(ctx, &storagev1.GetVolumeRequest{Name: "projects/local/zones/az-1/volumes/data-err"})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	check("get", vol)
	page, err := vols.ListVolumes(ctx, &storagev1.ListVolumesRequest{Parent: "projects/local/zones/az-1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.GetVolumes()) != 1 {
		t.Fatalf("list %d volumes", len(page.GetVolumes()))
	}
	check("list", page.GetVolumes()[0])
}
