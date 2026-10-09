package server_test

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/popododo0720/vws-storage/internal/backend/fake"
	storagev1 "github.com/popododo0720/vws-storage/internal/gen/vws/storage/v1"
	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/server"
	"github.com/popododo0720/vws-storage/internal/storage"
)

func TestCreateVolumeAndTokenReuse(t *testing.T) {
	block, ops := dial(t)
	ctx := context.Background()
	first, err := block.CreateVolume(ctx, &storagev1.CreateVolumeRequest{
		ClientToken: "vol-tok-1",
		SizeBytes:   int64(model.GiB),
		Zone:        "az-1",
		Type:        "ssd",
	})
	if err != nil {
		t.Fatal(err)
	}
	again, err := block.CreateVolume(ctx, &storagev1.CreateVolumeRequest{
		ClientToken: "vol-tok-1",
		SizeBytes:   int64(model.GiB),
		Zone:        "az-1",
		Type:        "ssd",
	})
	if err != nil {
		t.Fatal(err)
	}
	if again.GetId() != first.GetId() {
		t.Fatalf("replay id %s want %s", again.GetId(), first.GetId())
	}
	_, err = block.CreateVolume(ctx, &storagev1.CreateVolumeRequest{
		ClientToken: "vol-tok-1",
		SizeBytes:   int64(2 * model.GiB),
		Zone:        "az-1",
		Type:        "ssd",
	})
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.AlreadyExists {
		t.Fatalf("code %v err %v", st.Code(), err)
	}
	var reason string
	for _, d := range st.Details() {
		info, ok := d.(*errdetails.ErrorInfo)
		if ok {
			reason = info.GetReason()
		}
	}
	if reason != "CLIENT_TOKEN_REUSED" {
		t.Fatalf("reason %q", reason)
	}
	deadline := time.Now().Add(3 * time.Second)
	var done *storagev1.Operation
	for time.Now().Before(deadline) {
		got, err := ops.GetOperation(ctx, &storagev1.GetOperationRequest{Id: first.GetId()})
		if err != nil {
			t.Fatal(err)
		}
		if got.GetDone() {
			done = got
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if done == nil || done.GetPhase() != "succeeded" {
		t.Fatalf("operation %+v", done)
	}
	vol, err := block.GetVolume(ctx, &storagev1.GetVolumeRequest{Id: done.GetTarget()})
	if err != nil {
		t.Fatal(err)
	}
	if vol.GetState() != "available" || vol.GetSizeBytes() != int64(model.GiB) {
		t.Fatalf("volume %+v", vol)
	}
}

func dial(t *testing.T) (storagev1.BlockServiceClient, storagev1.OperationServiceClient) {
	t.Helper()
	zone, err := model.ParseZone("az-1")
	if err != nil {
		t.Fatal(err)
	}
	obj := fake.NewObjects(fake.ObjectOptions{})
	svc, err := storage.Open(context.Background(), storage.Options{
		Store:        storage.Memory(),
		Block:        fake.NewBlock(),
		Object:       obj,
		Signer:       obj,
		Zones:        []model.Zone{zone},
		Workers:      2,
		ScanInterval: 5 * time.Millisecond,
		RetryBase:    time.Millisecond,
		RetryMax:     5 * time.Millisecond,
		UploadPoll:   5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		_ = svc.Run(ctx)
		close(runDone)
	}()
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	server.Register(gs, svc)
	go func() { _ = gs.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		<-runDone
		gs.Stop()
		conn.Close()
		svc.Close()
	})
	return storagev1.NewBlockServiceClient(conn), storagev1.NewOperationServiceClient(conn)
}
