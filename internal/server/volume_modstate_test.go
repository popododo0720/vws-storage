package server_test

import (
	"context"
	"testing"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	commonv1 "github.com/popododo0720/vws-proto/gen/go/vws/common/v1"
	storagev1 "github.com/popododo0720/vws-proto/gen/go/vws/storage/v1"

	"github.com/popododo0720/vws-storage/internal/backend"
	"github.com/popododo0720/vws-storage/internal/backend/fake"
	"github.com/popododo0720/vws-storage/internal/server"
)

// modificationState is the EC2 VolumeModification.modificationState that
// console-api derives from the operation UpdateVolume returns (#157):
// not done is modifying, done without error is completed, done with an
// error is failed. VWS has no optimizing phase.
func modificationState(op *longrunningpb.Operation) string {
	switch {
	case !op.GetDone():
		return "modifying"
	case op.GetError() != nil:
		return "failed"
	default:
		return "completed"
	}
}

// The operation of an UpdateVolume exposes the EC2 modification states:
// a change without growth is completed at once, a growth is modifying until
// the claim grows and then completed, a failed growth is failed and changes
// nothing. The metadata carries the UPDATE verb, start and end times (#157).
func TestUpdateVolumeOperationExposesModificationState(t *testing.T) {
	blk := fake.NewBlock()
	vols, _, _, _, ops := dialBlock(t, "", server.Config{}, blk)
	ctx := context.Background()
	name := createGP2(t, vols, ops, "modstate", 10)
	update := func(spec *storagev1.VolumeSpec, paths ...string) *longrunningpb.Operation {
		t.Helper()
		cur, err := vols.GetVolume(ctx, &storagev1.GetVolumeRequest{Name: name})
		if err != nil {
			t.Fatal(err)
		}
		op, err := vols.UpdateVolume(ctx, &storagev1.UpdateVolumeRequest{
			Volume:     &storagev1.Volume{Name: name, Etag: cur.GetEtag(), Spec: spec},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: paths},
		})
		if err != nil {
			t.Fatal(err)
		}
		return op
	}
	meta := func(step string, op *longrunningpb.Operation) {
		t.Helper()
		var md commonv1.OperationMetadata
		if err := op.GetMetadata().UnmarshalTo(&md); err != nil {
			t.Fatal(err)
		}
		if md.GetVerb() != commonv1.OperationVerb_OPERATION_VERB_UPDATE || md.GetCreateTime() == nil ||
			(op.GetDone() != (md.GetEndTime() != nil)) {
			t.Fatalf("%s: metadata %v done %v", step, &md, op.GetDone())
		}
	}

	retype := update(&storagev1.VolumeSpec{Type: storagev1.VolumeType_VOLUME_TYPE_GP3}, "spec.type")
	meta("retype", retype)
	if got := modificationState(retype); got != "completed" {
		t.Fatalf("retype without growth: %s", got)
	}

	grow := update(&storagev1.VolumeSpec{SizeGib: 20}, "spec.size_gib")
	meta("grow", grow)
	if got := modificationState(grow); got != "modifying" {
		t.Fatalf("growth as admitted: %s", got)
	}
	done := waitOp(t, ops, grow.GetName())
	meta("grow done", done)
	if got := modificationState(done); got != "completed" {
		t.Fatalf("growth finished: %s (%v)", got, done.GetError())
	}

	blk.FailNext("Expand", backend.Permanent(backend.ErrInvalid))
	failing := update(&storagev1.VolumeSpec{SizeGib: 30}, "spec.size_gib")
	failed := waitOp(t, ops, failing.GetName())
	meta("failed growth", failed)
	if got := modificationState(failed); got != "failed" {
		t.Fatalf("failed growth: %s", got)
	}
	vol, err := vols.GetVolume(ctx, &storagev1.GetVolumeRequest{Name: name})
	if err != nil {
		t.Fatal(err)
	}
	if vol.GetSpec().GetSizeGib() != 20 || vol.GetSpec().GetType() != storagev1.VolumeType_VOLUME_TYPE_GP3 {
		t.Fatalf("after failed growth: %v", vol.GetSpec())
	}
}
