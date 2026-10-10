package server_test

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	commonv1 "github.com/popododo0720/vws-proto/gen/go/vws/common/v1"
	storagev1 "github.com/popododo0720/vws-proto/gen/go/vws/storage/v1"

	"github.com/popododo0720/vws-storage/internal/backend/fake"
	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/server"
	"github.com/popododo0720/vws-storage/internal/storage"
)

func TestCreateVolumeAndTokenReuse(t *testing.T) {
	vols, _, _, _, ops := dial(t, "")
	ctx := context.Background()
	req := &storagev1.CreateVolumeRequest{
		Parent:    "projects/local/zones/az-1",
		VolumeId:  "data-1",
		RequestId: "11111111-1111-4111-8111-111111111111",
		Volume: &storagev1.Volume{Spec: &storagev1.VolumeSpec{
			SizeGib: 1,
			Type:    storagev1.VolumeType_VOLUME_TYPE_SSD,
		}},
	}
	first, err := vols.CreateVolume(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	again, err := vols.CreateVolume(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if again.GetName() != first.GetName() {
		t.Fatalf("replay name %s want %s", again.GetName(), first.GetName())
	}
	req.Volume.Spec.SizeGib = 2
	_, err = vols.CreateVolume(ctx, req)
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Aborted {
		t.Fatalf("code %v err %v", st.Code(), err)
	}
	if reasonOf(st) != "IDEMPOTENCY_CONFLICT" {
		t.Fatalf("reason %q", reasonOf(st))
	}
	done := waitOp(t, ops, first.GetName())
	if done.GetError() != nil {
		t.Fatalf("operation error %+v", done.GetError())
	}
	vol, err := vols.GetVolume(ctx, &storagev1.GetVolumeRequest{Name: "projects/local/zones/az-1/volumes/data-1"})
	if err != nil {
		t.Fatal(err)
	}
	if vol.GetSpec().GetSizeGib() != 1 || vol.GetStatus().GetObserved().GetState() != commonv1.ReconciliationState_RECONCILIATION_STATE_READY {
		t.Fatalf("volume %+v", vol)
	}
}

func TestCreateObjectStoresBytes(t *testing.T) {
	_, _, buckets, objects, ops := dial(t, "http://127.0.0.1")
	ctx := context.Background()
	bop, err := buckets.CreateBucket(ctx, &storagev1.CreateBucketRequest{
		Parent: "projects/local", BucketId: "logs",
		Bucket: &storagev1.Bucket{Spec: &storagev1.BucketSpec{Region: "local"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if done := waitOp(t, ops, bop.GetName()); done.GetError() != nil {
		t.Fatal(done.GetError())
	}
	body := []byte("hello")
	oop, err := objects.CreateObject(ctx, &storagev1.CreateObjectRequest{
		Parent: "projects/local/buckets/logs", ObjectId: "day.bin",
		Object:   &storagev1.Object{Spec: &storagev1.ObjectSpec{ContentType: "text/plain"}},
		Contents: body,
	})
	if err != nil {
		t.Fatal(err)
	}
	if done := waitOp(t, ops, oop.GetName()); done.GetError() != nil {
		t.Fatal(done.GetError())
	}
	obj, err := objects.GetObject(ctx, &storagev1.GetObjectRequest{Name: "projects/local/buckets/logs/objects/day.bin"})
	if err != nil {
		t.Fatal(err)
	}
	if obj.GetSpec().GetContentType() != "text/plain" || obj.GetStatus().GetSizeBytes() != int64(len(body)) {
		t.Fatalf("object %+v", obj)
	}
}

func TestListVolumesPages(t *testing.T) {
	vols, _, _, _, _ := dial(t, "")
	ctx := context.Background()
	for _, id := range []string{"data-a", "data-b", "data-c"} {
		_, err := vols.CreateVolume(ctx, &storagev1.CreateVolumeRequest{
			Parent: "projects/local/zones/az-1", VolumeId: id,
			Volume: &storagev1.Volume{Spec: &storagev1.VolumeSpec{SizeGib: 1, Type: storagev1.VolumeType_VOLUME_TYPE_STANDARD}},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	var got []string
	token := ""
	for {
		page, err := vols.ListVolumes(ctx, &storagev1.ListVolumesRequest{
			Parent: "projects/local/zones/az-1", PageSize: 1, PageToken: token,
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(page.GetVolumes()) != 1 {
			t.Fatalf("page %d", len(page.GetVolumes()))
		}
		got = append(got, page.GetVolumes()[0].GetName())
		token = page.GetNextPageToken()
		if token == "" {
			break
		}
		if len(got) > 3 {
			t.Fatal("page did not end")
		}
	}
	want := []string{
		"projects/local/zones/az-1/volumes/data-a",
		"projects/local/zones/az-1/volumes/data-b",
		"projects/local/zones/az-1/volumes/data-c",
	}
	if len(got) != len(want) {
		t.Fatalf("names %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("names %v", got)
		}
	}
}

func TestDeleteReplayAndRecreate(t *testing.T) {
	_, _, buckets, _, ops := dial(t, "")
	ctx := context.Background()
	const id = "22222222-2222-4222-8222-222222222222"
	req := &storagev1.CreateBucketRequest{
		Parent: "projects/local", BucketId: "logs", RequestId: id,
		Bucket: &storagev1.Bucket{Spec: &storagev1.BucketSpec{Region: "local"}},
	}
	created, err := buckets.CreateBucket(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if done := waitOp(t, ops, created.GetName()); done.GetError() != nil {
		t.Fatal(done.GetError())
	}
	del := &storagev1.DeleteBucketRequest{
		Name: "projects/local/buckets/logs", RequestId: "33333333-3333-4333-8333-333333333333",
	}
	first, err := buckets.DeleteBucket(ctx, del)
	if err != nil {
		t.Fatal(err)
	}
	if done := waitOp(t, ops, first.GetName()); done.GetError() != nil {
		t.Fatal(done.GetError())
	}
	again, err := buckets.DeleteBucket(ctx, del)
	if err != nil {
		t.Fatal(err)
	}
	if again.GetName() != first.GetName() || !again.GetDone() {
		t.Fatalf("replay %+v", again)
	}
	got, err := ops.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: created.GetName()})
	if err != nil {
		t.Fatal(err)
	}
	if !got.GetDone() || got.GetError() != nil {
		t.Fatalf("create after delete %+v", got)
	}
	page, err := ops.ListOperations(ctx, &longrunningpb.ListOperationsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.GetOperations()) < 2 {
		t.Fatalf("operations %d", len(page.GetOperations()))
	}
	re, err := buckets.CreateBucket(ctx, &storagev1.CreateBucketRequest{
		Parent: "projects/local", BucketId: "logs",
		Bucket: &storagev1.Bucket{Spec: &storagev1.BucketSpec{Region: "local"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if re.GetName() == created.GetName() {
		t.Fatal("recreate reused the finished create")
	}
}

func TestVolumeSizeRejectsOverflow(t *testing.T) {
	vols, _, _, _, _ := dial(t, "")
	_, err := vols.CreateVolume(context.Background(), &storagev1.CreateVolumeRequest{
		Parent: "projects/local/zones/az-1", VolumeId: "data-1",
		Volume: &storagev1.Volume{Spec: &storagev1.VolumeSpec{SizeGib: 1<<34 + 1, Type: storagev1.VolumeType_VOLUME_TYPE_SSD}},
	})
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument || reasonOf(st) != "INVALID_SPEC" {
		t.Fatalf("code %v reason %q err %v", st.Code(), reasonOf(st), err)
	}
}

func TestObjectNameRoundTrip(t *testing.T) {
	_, _, buckets, objects, ops := dial(t, "http://127.0.0.1")
	ctx := context.Background()
	bop, err := buckets.CreateBucket(ctx, &storagev1.CreateBucketRequest{
		Parent: "projects/local", BucketId: "logs",
		Bucket: &storagev1.Bucket{Spec: &storagev1.BucketSpec{Region: "local"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if done := waitOp(t, ops, bop.GetName()); done.GetError() != nil {
		t.Fatal(done.GetError())
	}
	oop, err := objects.CreateObject(ctx, &storagev1.CreateObjectRequest{
		Parent: "projects/local/buckets/logs", ObjectId: "a:b",
		Object:   &storagev1.Object{Spec: &storagev1.ObjectSpec{ContentType: "text/plain"}},
		Contents: []byte("x"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if done := waitOp(t, ops, oop.GetName()); done.GetError() != nil {
		t.Fatal(done.GetError())
	}
	const name = "projects/local/buckets/logs/objects/a%3Ab"
	obj, err := objects.GetObject(ctx, &storagev1.GetObjectRequest{Name: name})
	if err != nil {
		t.Fatal(err)
	}
	if obj.GetName() != name {
		t.Fatalf("name %s", obj.GetName())
	}
	_, err = objects.CreateObject(ctx, &storagev1.CreateObjectRequest{
		Parent: "projects/local/buckets/logs", ObjectId: "a:b",
		Object:   &storagev1.Object{Spec: &storagev1.ObjectSpec{ContentType: "text/plain"}},
		Contents: []byte("y"),
	})
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.AlreadyExists {
		t.Fatalf("code %v err %v", st.Code(), err)
	}
}

func reasonOf(st *status.Status) string {
	for _, d := range st.Details() {
		info, ok := d.(*errdetails.ErrorInfo)
		if ok {
			return info.GetReason()
		}
	}
	return ""
}

func waitOp(t *testing.T, ops longrunningpb.OperationsClient, name string) *longrunningpb.Operation {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		got, err := ops.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: name})
		if err != nil {
			t.Fatal(err)
		}
		if got.GetDone() {
			return got
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("operation not done")
	return nil
}

func dial(t *testing.T, objectBase string) (
	storagev1.VolumeServiceClient,
	storagev1.SnapshotServiceClient,
	storagev1.BucketServiceClient,
	storagev1.ObjectServiceClient,
	longrunningpb.OperationsClient,
) {
	t.Helper()
	zone, err := model.ParseZone("az-1")
	if err != nil {
		t.Fatal(err)
	}
	objs := fake.NewObjects(fake.ObjectOptions{})
	var httpSrv *http.Server
	if objectBase != "" {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		objs = fake.NewObjects(fake.ObjectOptions{BaseURL: "http://" + ln.Addr().String()})
		httpSrv = &http.Server{Handler: objs.Handler(), ReadHeaderTimeout: 5 * time.Second}
		go func() { _ = httpSrv.Serve(ln) }()
	}
	svc, err := storage.Open(context.Background(), storage.Options{
		Store: storage.Memory(), Block: fake.NewBlock(), Object: objs, Signer: objs,
		Zones: []model.Zone{zone}, Workers: 2, ScanInterval: 5 * time.Millisecond,
		RetryBase: time.Millisecond, RetryMax: 5 * time.Millisecond, UploadPoll: 5 * time.Millisecond,
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
	gs := grpc.NewServer(grpc.MaxRecvMsgSize(server.MaxRecvMsgSize))
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
		if httpSrv != nil {
			shut, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = httpSrv.Shutdown(shut)
		}
		svc.Close()
	})
	return storagev1.NewVolumeServiceClient(conn),
		storagev1.NewSnapshotServiceClient(conn),
		storagev1.NewBucketServiceClient(conn),
		storagev1.NewObjectServiceClient(conn),
		longrunningpb.NewOperationsClient(conn)
}
