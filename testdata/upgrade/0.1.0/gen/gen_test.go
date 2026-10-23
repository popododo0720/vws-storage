// Package gen writes the 0.1.0 upgrade fixtures. TestGenerate010 is a gRPC
// client for a running 0.1.0 server, so every stored byte comes from that
// server. It lives under testdata, so go test ./... never runs it; see the
// README next to this directory for the command.
package gen

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	storagev1 "github.com/popododo0720/vws-proto/gen/go/vws/storage/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	project = "projects/a123456789012"
	zone    = project + "/zones/vws-1a"
	region  = project + "/regions/vws-1"
)

// TestGenerate010 needs VWS_FIXTURE_ADDR, the address of the 0.1.0 server.
func TestGenerate010(t *testing.T) {
	addr := os.Getenv("VWS_FIXTURE_ADDR")
	if addr == "" {
		t.Fatal("VWS_FIXTURE_ADDR is not set")
	}
	if err := run(addr); err != nil {
		t.Fatal(err)
	}
}

func run(addr string) error {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	vols := storagev1.NewVolumeServiceClient(conn)
	snaps := storagev1.NewSnapshotServiceClient(conn)
	ops := longrunningpb.NewOperationsClient(conn)

	volumes := []struct {
		id, token string
		size      int64
		typ       storagev1.VolumeType
		labels    map[string]string
	}{
		{"vol-0000000000000001a", "11111111-1111-4111-8111-111111111111", 8, storagev1.VolumeType_VOLUME_TYPE_STANDARD, map[string]string{"team": "storage"}},
		{"vol-0000000000000002b", "22222222-2222-4222-8222-222222222222", 16, storagev1.VolumeType_VOLUME_TYPE_SSD, nil},
		// Over the 1024 GiB standard limit added by #124 (#126).
		{"vol-0000000000000003c", "33333333-3333-4333-8333-333333333333", 2000, storagev1.VolumeType_VOLUME_TYPE_STANDARD, nil},
	}
	for _, v := range volumes {
		op, err := vols.CreateVolume(ctx, &storagev1.CreateVolumeRequest{
			Parent:    zone,
			VolumeId:  v.id,
			RequestId: v.token,
			Volume: &storagev1.Volume{
				Labels: v.labels,
				Spec:   &storagev1.VolumeSpec{SizeGib: v.size, Type: v.typ},
			},
		})
		if err != nil {
			return fmt.Errorf("create %s: %w", v.id, err)
		}
		if err := wait(ctx, ops, op); err != nil {
			return err
		}
	}
	op, err := snaps.CreateSnapshot(ctx, &storagev1.CreateSnapshotRequest{
		Parent:     region,
		SnapshotId: "snap-0000000000000001a",
		RequestId:  "44444444-4444-4444-8444-444444444444",
		Snapshot: &storagev1.Snapshot{
			Spec: &storagev1.SnapshotSpec{Volume: zone + "/volumes/" + volumes[0].id, Description: "fixture"},
		},
	})
	if err != nil {
		return fmt.Errorf("create snapshot: %w", err)
	}
	return wait(ctx, ops, op)
}

func wait(ctx context.Context, ops longrunningpb.OperationsClient, op *longrunningpb.Operation) error {
	for !op.GetDone() {
		time.Sleep(100 * time.Millisecond)
		var err error
		if op, err = ops.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: op.GetName()}); err != nil {
			return fmt.Errorf("get %s: %w", op.GetName(), err)
		}
	}
	if e := op.GetError(); e != nil {
		return fmt.Errorf("%s: %s", op.GetName(), e.GetMessage())
	}
	return nil
}
