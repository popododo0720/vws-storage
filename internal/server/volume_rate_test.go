package server_test

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	storagev1 "github.com/popododo0720/vws-proto/gen/go/vws/storage/v1"
)

// The fifth size, type or performance change of a volume within 24 hours is
// RESOURCE_EXHAUSTED with reason VOLUME_MODIFICATION_RATE_EXCEEDED, which
// console-api maps to EC2 VolumeModificationRateExceeded; the message names
// when the next modification is allowed. A labels-only update (EC2 tags) is
// still accepted (#157).
func TestUpdateVolumeFifthModificationIsRateExceeded(t *testing.T) {
	vols, _, _, _, ops := dial(t, "")
	name := createGP2(t, vols, ops, "rate", 200)
	for _, typ := range []storagev1.VolumeType{
		storagev1.VolumeType_VOLUME_TYPE_GP3, storagev1.VolumeType_VOLUME_TYPE_GP2,
		storagev1.VolumeType_VOLUME_TYPE_GP3, storagev1.VolumeType_VOLUME_TYPE_GP2,
	} {
		if _, err := retypeVolume(t, vols, ops, name, typ); err != nil {
			t.Fatalf("retype to %v: %v", typ, err)
		}
	}
	_, err := retypeVolume(t, vols, ops, name, storagev1.VolumeType_VOLUME_TYPE_GP3)
	st, _ := status.FromError(err)
	if st.Code() != codes.ResourceExhausted || reasonOf(st) != "VOLUME_MODIFICATION_RATE_EXCEEDED" ||
		!strings.Contains(st.Message(), "wait until ") {
		t.Fatalf("fifth: code %v reason %q err %v", st.Code(), reasonOf(st), err)
	}
	cur, err := vols.GetVolume(context.Background(), &storagev1.GetVolumeRequest{Name: name})
	if err != nil {
		t.Fatal(err)
	}
	labeled := &storagev1.Volume{Name: name, Etag: cur.GetEtag(), Labels: map[string]string{"team": "storage"}}
	vol, err := updateVolume(t, vols, ops, labeled, "labels")
	gp2 := storagev1.VolumeType_VOLUME_TYPE_GP2
	if err != nil || vol.GetLabels()["team"] != "storage" || vol.GetSpec().GetType() != gp2 {
		t.Fatalf("labels after the limit: %v err %v", vol, err)
	}
}
