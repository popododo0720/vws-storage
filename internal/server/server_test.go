package server_test

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

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
	sum := md5.Sum(body)
	if want := hex.EncodeToString(sum[:]); obj.GetStatus().GetMd5() != want {
		t.Fatalf("md5 %q want %q", obj.GetStatus().GetMd5(), want)
	}
	if obj.GetEtag() == obj.GetStatus().GetMd5() || obj.GetEtag() == obj.GetSpec().GetSha256() {
		t.Fatalf("etag %q is a digest", obj.GetEtag())
	}
}

func TestProjectScopedVolumeAndBucket(t *testing.T) {
	vols, _, buckets, _, ops := dial(t, "")
	ctx := context.Background()
	created, err := vols.CreateVolume(ctx, &storagev1.CreateVolumeRequest{
		Parent: "projects/a123/zones/az-1", VolumeId: "data-1",
		Volume: &storagev1.Volume{Spec: &storagev1.VolumeSpec{SizeGib: 1, Type: storagev1.VolumeType_VOLUME_TYPE_SSD}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if done := waitOp(t, ops, created.GetName()); done.GetError() != nil {
		t.Fatal(done.GetError())
	}
	if created.GetName() == "" || !strings.HasPrefix(created.GetName(), "projects/a123/operations/") {
		t.Fatalf("operation %s", created.GetName())
	}
	vol, err := vols.GetVolume(ctx, &storagev1.GetVolumeRequest{Name: "projects/a123/zones/az-1/volumes/data-1"})
	if err != nil {
		t.Fatal(err)
	}
	if vol.GetName() != "projects/a123/zones/az-1/volumes/data-1" {
		t.Fatalf("name %s", vol.GetName())
	}
	_, err = vols.GetVolume(ctx, &storagev1.GetVolumeRequest{Name: "projects/a999/zones/az-1/volumes/data-1"})
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.NotFound {
		t.Fatalf("other project code %v err %v", st.Code(), err)
	}
	bop, err := buckets.CreateBucket(ctx, &storagev1.CreateBucketRequest{
		Parent: "projects/a123", BucketId: "logs",
		Bucket: &storagev1.Bucket{Spec: &storagev1.BucketSpec{Region: "vws-1"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if done := waitOp(t, ops, bop.GetName()); done.GetError() != nil {
		t.Fatal(done.GetError())
	}
	got, err := buckets.GetBucket(ctx, &storagev1.GetBucketRequest{Name: "projects/a123/buckets/logs"})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetSpec().GetRegion() != "vws-1" {
		t.Fatalf("region %s", got.GetSpec().GetRegion())
	}
	_, err = buckets.GetBucket(ctx, &storagev1.GetBucketRequest{Name: "projects/a999/buckets/logs"})
	st, ok = status.FromError(err)
	if !ok || st.Code() != codes.NotFound {
		t.Fatalf("other bucket code %v err %v", st.Code(), err)
	}
}

func TestCreateVolumeFromImage(t *testing.T) {
	project, err := model.ParseProject("a123")
	if err != nil {
		t.Fatal(err)
	}
	vols, _, _, _, ops := dialConfig(t, "", server.Config{Projects: []model.Project{project}})
	ctx := context.Background()
	const image = "projects/shared/regions/vws-1/images/ubuntu-24-04"
	req := &storagev1.CreateVolumeRequest{
		Parent: "projects/a123/zones/az-1", VolumeId: "boot-1",
		RequestId: "22222222-2222-4222-8222-222222222222",
		Volume: &storagev1.Volume{Spec: &storagev1.VolumeSpec{
			SizeGib: 1,
			Type:    storagev1.VolumeType_VOLUME_TYPE_SSD,
			Source:  &storagev1.VolumeSpec_Image{Image: image},
		}},
	}
	created, err := vols.CreateVolume(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if done := waitOp(t, ops, created.GetName()); done.GetError() != nil {
		t.Fatal(done.GetError())
	}
	vol, err := vols.GetVolume(ctx, &storagev1.GetVolumeRequest{Name: "projects/a123/zones/az-1/volumes/boot-1"})
	if err != nil {
		t.Fatal(err)
	}
	if vol.GetSpec().GetImage() != image {
		t.Fatalf("image %q", vol.GetSpec().GetImage())
	}
	listed, err := vols.ListVolumes(ctx, &storagev1.ListVolumesRequest{Parent: "projects/a123/zones/az-1"})
	if err != nil {
		t.Fatal(err)
	}
	var saw bool
	for _, item := range listed.GetVolumes() {
		if item.GetName() == vol.GetName() && item.GetSpec().GetImage() == image {
			saw = true
		}
	}
	if !saw {
		t.Fatalf("list %+v", listed.GetVolumes())
	}
	req.Volume.Spec.Source = &storagev1.VolumeSpec_Image{Image: "projects/shared/regions/vws-1/images/ubuntu-22-04"}
	_, err = vols.CreateVolume(ctx, req)
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Aborted || reasonOf(st) != "IDEMPOTENCY_CONFLICT" {
		t.Fatalf("conflict code %v reason %q err %v", st.Code(), reasonOf(st), err)
	}
	req.RequestId = "33333333-3333-4333-8333-333333333333"
	_, err = vols.CreateVolume(ctx, req)
	st, ok = status.FromError(err)
	if !ok || st.Code() != codes.AlreadyExists || reasonOf(st) != "ALREADY_EXISTS" {
		t.Fatalf("exists code %v reason %q err %v", st.Code(), reasonOf(st), err)
	}
	_, err = vols.CreateVolume(ctx, &storagev1.CreateVolumeRequest{
		Parent: "projects/a123/zones/az-1", VolumeId: "boot-bad",
		Volume: &storagev1.Volume{Spec: &storagev1.VolumeSpec{
			SizeGib: 1,
			Type:    storagev1.VolumeType_VOLUME_TYPE_SSD,
			Source:  &storagev1.VolumeSpec_Image{Image: "images/debian"},
		}},
	})
	st, ok = status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument || reasonOf(st) != "INVALID_RESOURCE_ID" {
		t.Fatalf("bad name code %v reason %q err %v", st.Code(), reasonOf(st), err)
	}
	_, err = vols.GetVolume(ctx, &storagev1.GetVolumeRequest{Name: "projects/a123/zones/az-1/volumes/boot-bad"})
	st, ok = status.FromError(err)
	if !ok || st.Code() != codes.NotFound {
		t.Fatalf("missing code %v err %v", st.Code(), err)
	}
	_, err = vols.CreateVolume(ctx, &storagev1.CreateVolumeRequest{
		Parent: "projects/a123/zones/az-1", VolumeId: "boot-empty",
		Volume: &storagev1.Volume{Spec: &storagev1.VolumeSpec{
			SizeGib: 1,
			Type:    storagev1.VolumeType_VOLUME_TYPE_SSD,
			Source:  &storagev1.VolumeSpec_Image{Image: ""},
		}},
	})
	st, ok = status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument || reasonOf(st) != "INVALID_RESOURCE_ID" {
		t.Fatalf("empty image code %v reason %q err %v", st.Code(), reasonOf(st), err)
	}
}

func TestProjectAllowListRejected(t *testing.T) {
	project, err := model.ParseProject("a123")
	if err != nil {
		t.Fatal(err)
	}
	vols, _, _, _, _ := dialConfig(t, "", server.Config{Projects: []model.Project{project}})
	_, err = vols.CreateVolume(context.Background(), &storagev1.CreateVolumeRequest{
		Parent: "projects/a999/zones/az-1", VolumeId: "data-1",
		Volume: &storagev1.Volume{Spec: &storagev1.VolumeSpec{SizeGib: 1, Type: storagev1.VolumeType_VOLUME_TYPE_SSD}},
	})
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.NotFound || reasonOf(st) != "RESOURCE_NOT_FOUND" {
		t.Fatalf("code %v reason %q err %v", st.Code(), reasonOf(st), err)
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
	page, err := ops.ListOperations(ctx, &longrunningpb.ListOperationsRequest{Name: "projects/local"})
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

func TestUpdateObjectOverwrites(t *testing.T) {
	_, _, buckets, objects, ops := dial(t, "http://127.0.0.1")
	ctx := context.Background()
	_, err := objects.UpdateObject(ctx, &storagev1.UpdateObjectRequest{
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"uid"}},
	})
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Unimplemented {
		t.Fatalf("mask code %v err %v", st.Code(), err)
	}
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
	const name = "projects/local/buckets/logs/objects/day.bin"
	oop, err := objects.CreateObject(ctx, &storagev1.CreateObjectRequest{
		Parent: "projects/local/buckets/logs", ObjectId: "day.bin",
		Object:   &storagev1.Object{Spec: &storagev1.ObjectSpec{ContentType: "text/plain"}},
		Contents: []byte("hello"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if done := waitOp(t, ops, oop.GetName()); done.GetError() != nil {
		t.Fatal(done.GetError())
	}
	_, err = objects.CreateObject(ctx, &storagev1.CreateObjectRequest{
		Parent: "projects/local/buckets/logs", ObjectId: "day.bin",
		Object:   &storagev1.Object{Spec: &storagev1.ObjectSpec{ContentType: "text/plain"}},
		Contents: []byte("world!!"),
	})
	st, ok = status.FromError(err)
	if !ok || st.Code() != codes.AlreadyExists {
		t.Fatalf("create code %v err %v", st.Code(), err)
	}
	before, err := objects.GetObject(ctx, &storagev1.GetObjectRequest{Name: name})
	if err != nil {
		t.Fatal(err)
	}
	req := &storagev1.UpdateObjectRequest{
		RequestId: "22222222-2222-4222-8222-222222222222",
		Object: &storagev1.Object{
			Name: name,
			Spec: &storagev1.ObjectSpec{ContentType: "application/octet-stream"},
		},
		Contents: []byte("world!!"),
	}
	uop, err := objects.UpdateObject(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if done := waitOp(t, ops, uop.GetName()); done.GetError() != nil {
		t.Fatal(done.GetError())
	}
	again, err := objects.UpdateObject(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if again.GetName() != uop.GetName() {
		t.Fatalf("replay %s want %s", again.GetName(), uop.GetName())
	}
	req.Contents = []byte("other!")
	_, err = objects.UpdateObject(ctx, req)
	st, ok = status.FromError(err)
	if !ok || st.Code() != codes.Aborted || reasonOf(st) != "IDEMPOTENCY_CONFLICT" {
		t.Fatalf("token code %v reason %s err %v", st.Code(), reasonOf(st), err)
	}
	got, err := objects.GetObject(ctx, &storagev1.GetObjectRequest{Name: name})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetStatus().GetSizeBytes() != 7 || got.GetEtag() == before.GetEtag() || got.GetSpec().GetContentType() != "application/octet-stream" {
		t.Fatalf("object %+v", got)
	}
	same, err := objects.UpdateObject(ctx, &storagev1.UpdateObjectRequest{
		Object: &storagev1.Object{
			Name: name,
			Spec: &storagev1.ObjectSpec{ContentType: "application/octet-stream"},
		},
		Contents: []byte("world!!"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if done := waitOp(t, ops, same.GetName()); done.GetError() != nil {
		t.Fatal(done.GetError())
	}
	kept, err := objects.GetObject(ctx, &storagev1.GetObjectRequest{Name: name})
	if err != nil {
		t.Fatal(err)
	}
	if kept.GetEtag() != got.GetEtag() {
		t.Fatalf("identical overwrite bumped etag %s -> %s", got.GetEtag(), kept.GetEtag())
	}
	meta, err := objects.UpdateObject(ctx, &storagev1.UpdateObjectRequest{
		Object: &storagev1.Object{
			Name:   name,
			Labels: map[string]string{"env": "prod"},
			Spec:   &storagev1.ObjectSpec{ContentType: "application/octet-stream"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if done := waitOp(t, ops, meta.GetName()); done.GetError() != nil {
		t.Fatal(done.GetError())
	}
	labeled, err := objects.GetObject(ctx, &storagev1.GetObjectRequest{Name: name})
	if err != nil {
		t.Fatal(err)
	}
	if labeled.GetLabels()["env"] != "prod" || labeled.GetStatus().GetSizeBytes() != 7 || labeled.GetEtag() == kept.GetEtag() {
		t.Fatalf("metadata %+v", labeled)
	}
	_, err = objects.UpdateObject(ctx, &storagev1.UpdateObjectRequest{
		AllowMissing: true,
		Object:       &storagev1.Object{Name: "projects/local/buckets/logs/objects/missing", Spec: &storagev1.ObjectSpec{}},
	})
	st, ok = status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument || reasonOf(st) != "INVALID_SPEC" {
		t.Fatalf("allow_missing without contents %v %v", st.Code(), err)
	}
	_, err = objects.UpdateObject(ctx, &storagev1.UpdateObjectRequest{
		Object: &storagev1.Object{Name: "projects/local/buckets/logs/objects/missing", Spec: &storagev1.ObjectSpec{}},
	})
	st, ok = status.FromError(err)
	if !ok || st.Code() != codes.NotFound {
		t.Fatalf("missing code %v err %v", st.Code(), err)
	}
	made, err := objects.UpdateObject(ctx, &storagev1.UpdateObjectRequest{
		AllowMissing: true,
		Object:       &storagev1.Object{Name: "projects/local/buckets/logs/objects/other", Spec: &storagev1.ObjectSpec{}},
		Contents:     []byte("z"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if done := waitOp(t, ops, made.GetName()); done.GetError() != nil {
		t.Fatal(done.GetError())
	}
	other, err := objects.GetObject(ctx, &storagev1.GetObjectRequest{Name: "projects/local/buckets/logs/objects/other"})
	if err != nil || other.GetStatus().GetSizeBytes() != 1 {
		t.Fatalf("created %+v %v", other, err)
	}
}

func TestLabelsRoundTrip(t *testing.T) {
	vols, snaps, buckets, objects, ops := dial(t, "http://127.0.0.1")
	ctx := context.Background()
	_, err := vols.CreateVolume(ctx, &storagev1.CreateVolumeRequest{
		Parent: "projects/local/zones/az-1", VolumeId: "data-1",
		Volume: &storagev1.Volume{
			Labels: map[string]string{"Bad": "x"},
			Spec:   &storagev1.VolumeSpec{SizeGib: 1, Type: storagev1.VolumeType_VOLUME_TYPE_SSD},
		},
	})
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument || reasonOf(st) != "INVALID_SPEC" {
		t.Fatalf("bad label code %v err %v", st.Code(), err)
	}
	created, err := vols.CreateVolume(ctx, &storagev1.CreateVolumeRequest{
		Parent: "projects/local/zones/az-1", VolumeId: "data-1",
		Volume: &storagev1.Volume{
			Labels: map[string]string{"env": "prod"},
			Spec:   &storagev1.VolumeSpec{SizeGib: 1, Type: storagev1.VolumeType_VOLUME_TYPE_SSD},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if done := waitOp(t, ops, created.GetName()); done.GetError() != nil {
		t.Fatal(done.GetError())
	}
	vol, err := vols.GetVolume(ctx, &storagev1.GetVolumeRequest{Name: "projects/local/zones/az-1/volumes/data-1"})
	if err != nil {
		t.Fatal(err)
	}
	if vol.GetLabels()["env"] != "prod" || len(vol.GetLabels()) != 1 {
		t.Fatalf("volume labels %v", vol.GetLabels())
	}
	_, err = snaps.CreateSnapshot(ctx, &storagev1.CreateSnapshotRequest{
		Parent: "projects/local/regions/local", SnapshotId: "snap-1",
		Snapshot: &storagev1.Snapshot{
			Spec: &storagev1.SnapshotSpec{Volume: vol.GetName(), Description: "bad\n"},
		},
	})
	st, ok = status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument || reasonOf(st) != "INVALID_SPEC" {
		t.Fatalf("bad description code %v err %v", st.Code(), err)
	}
	sop, err := snaps.CreateSnapshot(ctx, &storagev1.CreateSnapshotRequest{
		Parent: "projects/local/regions/local", SnapshotId: "snap-1",
		Snapshot: &storagev1.Snapshot{
			Labels: map[string]string{"env": "prod"},
			Spec:   &storagev1.SnapshotSpec{Volume: vol.GetName(), Description: "nightly copy"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if done := waitOp(t, ops, sop.GetName()); done.GetError() != nil {
		t.Fatal(done.GetError())
	}
	snap, err := snaps.GetSnapshot(ctx, &storagev1.GetSnapshotRequest{Name: "projects/local/regions/local/snapshots/snap-1"})
	if err != nil {
		t.Fatal(err)
	}
	if snap.GetSpec().GetDescription() != "nightly copy" || snap.GetLabels()["env"] != "prod" {
		t.Fatalf("snapshot %+v", snap)
	}
	bop, err := buckets.CreateBucket(ctx, &storagev1.CreateBucketRequest{
		Parent: "projects/local", BucketId: "logs",
		Bucket: &storagev1.Bucket{
			Labels: map[string]string{"env": "prod"},
			Spec:   &storagev1.BucketSpec{Region: "local"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if done := waitOp(t, ops, bop.GetName()); done.GetError() != nil {
		t.Fatal(done.GetError())
	}
	bucket, err := buckets.GetBucket(ctx, &storagev1.GetBucketRequest{Name: "projects/local/buckets/logs"})
	if err != nil {
		t.Fatal(err)
	}
	if bucket.GetLabels()["env"] != "prod" {
		t.Fatalf("bucket labels %v", bucket.GetLabels())
	}
	oop, err := objects.CreateObject(ctx, &storagev1.CreateObjectRequest{
		Parent: "projects/local/buckets/logs", ObjectId: "day.bin",
		Object: &storagev1.Object{
			Labels: map[string]string{"env": "prod"},
			Spec:   &storagev1.ObjectSpec{ContentType: "text/plain"},
		},
		Contents: []byte("hello"),
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
	if obj.GetLabels()["env"] != "prod" {
		t.Fatalf("object labels %v", obj.GetLabels())
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
	return dialConfig(t, objectBase, server.Config{})
}

func dialConfig(t *testing.T, objectBase string, cfg server.Config) (
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
	server.Register(gs, svc, cfg)
	go func() { _ = gs.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(server.MaxRecvMsgSize)),
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

const (
	writeBucket = "projects/local/buckets/logs"
	writeName   = writeBucket + "/objects/day.bin"
	reqA        = "33333333-3333-4333-8333-333333333333"
)

func md5hex(b []byte) string {
	sum := md5.Sum(b)
	return hex.EncodeToString(sum[:])
}

func sha256hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// seedObject creates bucket logs and object day.bin with body, content type
// text/plain, and one label.
func seedObject(t *testing.T, body []byte) (storagev1.ObjectServiceClient, longrunningpb.OperationsClient) {
	t.Helper()
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
		Parent: writeBucket, ObjectId: "day.bin",
		Object: &storagev1.Object{
			Labels: map[string]string{"env": "dev"},
			Spec:   &storagev1.ObjectSpec{ContentType: "text/plain"},
		},
		Contents: body,
	})
	if err != nil {
		t.Fatal(err)
	}
	if done := waitOp(t, ops, oop.GetName()); done.GetError() != nil {
		t.Fatal(done.GetError())
	}
	return objects, ops
}

func getObject(t *testing.T, objects storagev1.ObjectServiceClient) *storagev1.Object {
	t.Helper()
	obj, err := objects.GetObject(context.Background(), &storagev1.GetObjectRequest{Name: writeName})
	if err != nil {
		t.Fatal(err)
	}
	return obj
}

func wantStatus(t *testing.T, err error, code codes.Code, reason string) {
	t.Helper()
	st, ok := status.FromError(err)
	if !ok || st.Code() != code || reasonOf(st) != reason {
		t.Fatalf("got code %v reason %q err %v; want %v %q", st.Code(), reasonOf(st), err, code, reason)
	}
}

func TestWriteObjectReplacesPayloadAndKeepsEtagSeparateFromMD5(t *testing.T) {
	objects, ops := seedObject(t, []byte("hello"))
	ctx := context.Background()
	before := getObject(t, objects)
	body := []byte("world!!")
	wop, err := objects.WriteObject(ctx, &storagev1.WriteObjectRequest{Name: writeName, Contents: body, Etag: before.GetEtag()})
	if err != nil {
		t.Fatal(err)
	}
	if done := waitOp(t, ops, wop.GetName()); done.GetError() != nil {
		t.Fatal(done.GetError())
	}
	after := getObject(t, objects)
	if after.GetStatus().GetMd5() != md5hex(body) || after.GetSpec().GetSha256() != sha256hex(body) || after.GetStatus().GetSizeBytes() != 7 {
		t.Fatalf("digests %+v", after)
	}
	if after.GetEtag() == before.GetEtag() || after.GetEtag() == after.GetStatus().GetMd5() || after.GetEtag() == after.GetSpec().GetSha256() {
		t.Fatalf("etag %q before %q md5 %q", after.GetEtag(), before.GetEtag(), after.GetStatus().GetMd5())
	}
	if after.GetSpec().GetContentType() != "text/plain" || after.GetLabels()["env"] != "dev" {
		t.Fatalf("metadata not kept: %+v", after)
	}
	read, err := objects.ReadObject(ctx, &storagev1.ReadObjectRequest{Name: writeName})
	if err != nil || string(read.GetContents()) != string(body) || read.GetObject().GetEtag() != after.GetEtag() {
		t.Fatalf("read %v %v", read, err)
	}
}

func TestWriteObjectIdenticalBytesKeepEtag(t *testing.T) {
	objects, ops := seedObject(t, []byte("hello"))
	before := getObject(t, objects)
	wop, err := objects.WriteObject(context.Background(), &storagev1.WriteObjectRequest{Name: writeName, Contents: []byte("hello")})
	if err != nil {
		t.Fatal(err)
	}
	if done := waitOp(t, ops, wop.GetName()); done.GetError() != nil {
		t.Fatal(done.GetError())
	}
	after := getObject(t, objects)
	if after.GetEtag() != before.GetEtag() || after.GetGeneration() != before.GetGeneration() || after.GetStatus().GetMd5() != md5hex([]byte("hello")) {
		t.Fatalf("before %+v after %+v", before, after)
	}
}

func TestWriteObjectIsIdempotentOnRequestID(t *testing.T) {
	objects, ops := seedObject(t, []byte("hello"))
	ctx := context.Background()
	before := getObject(t, objects)
	req := &storagev1.WriteObjectRequest{Name: writeName, Contents: []byte("first"), Etag: before.GetEtag(), RequestId: reqA}
	first, err := objects.WriteObject(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if done := waitOp(t, ops, first.GetName()); done.GetError() != nil {
		t.Fatal(done.GetError())
	}
	written := getObject(t, objects)
	// The replay carries the etag from before the write. Replay wins over the etag compare.
	again, err := objects.WriteObject(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if again.GetName() != first.GetName() {
		t.Fatalf("replay %s want %s", again.GetName(), first.GetName())
	}
	if got := getObject(t, objects); got.GetEtag() != written.GetEtag() || got.GetGeneration() != written.GetGeneration() {
		t.Fatalf("replay wrote again: %+v -> %+v", written, got)
	}
	req.Contents = []byte("second")
	_, err = objects.WriteObject(ctx, req)
	wantStatus(t, err, codes.Aborted, "IDEMPOTENCY_CONFLICT")
	if got := getObject(t, objects); got.GetStatus().GetMd5() != md5hex([]byte("first")) {
		t.Fatalf("conflict wrote: %+v", got)
	}
	// Without the replay, the stale etag is rejected.
	_, err = objects.WriteObject(ctx, &storagev1.WriteObjectRequest{Name: writeName, Contents: []byte("third"), Etag: before.GetEtag()})
	wantStatus(t, err, codes.Aborted, "ETAG_MISMATCH")
}

func TestWriteObjectSizeLimitBoundary(t *testing.T) {
	objects, ops := seedObject(t, []byte("hello"))
	ctx := context.Background()
	atLimit := make([]byte, 4194304)
	atLimit[len(atLimit)-1] = 1
	wop, err := objects.WriteObject(ctx, &storagev1.WriteObjectRequest{Name: writeName, Contents: atLimit})
	if err != nil {
		t.Fatal(err)
	}
	if done := waitOp(t, ops, wop.GetName()); done.GetError() != nil {
		t.Fatal(done.GetError())
	}
	stored := getObject(t, objects)
	if stored.GetStatus().GetSizeBytes() != 4194304 || stored.GetStatus().GetMd5() != md5hex(atLimit) {
		t.Fatalf("at limit %+v", stored)
	}
	read, err := objects.ReadObject(ctx, &storagev1.ReadObjectRequest{Name: writeName})
	if err != nil || len(read.GetContents()) != 4194304 {
		t.Fatalf("read at limit %d %v", len(read.GetContents()), err)
	}
	_, err = objects.WriteObject(ctx, &storagev1.WriteObjectRequest{Name: writeName, Contents: make([]byte, 4194305)})
	wantStatus(t, err, codes.InvalidArgument, "INVALID_SPEC")
	if got := getObject(t, objects); got.GetEtag() != stored.GetEtag() {
		t.Fatalf("oversize write changed the object: %+v", got)
	}
}

func TestWriteObjectRejects(t *testing.T) {
	objects, ops := seedObject(t, []byte("hello"))
	ctx := context.Background()
	_, err := objects.WriteObject(ctx, &storagev1.WriteObjectRequest{Name: writeName})
	wantStatus(t, err, codes.InvalidArgument, "INVALID_SPEC")
	_, err = objects.WriteObject(ctx, &storagev1.WriteObjectRequest{Name: writeName, Contents: []byte("x"), Sha256: sha256hex([]byte("y"))})
	wantStatus(t, err, codes.InvalidArgument, "INVALID_SPEC")
	_, err = objects.WriteObject(ctx, &storagev1.WriteObjectRequest{Name: writeBucket + "/objects/missing", Contents: []byte("x")})
	wantStatus(t, err, codes.NotFound, "RESOURCE_NOT_FOUND")
	_, err = objects.WriteObject(ctx, &storagev1.WriteObjectRequest{Name: writeName, Contents: []byte("x"), Etag: "999"})
	wantStatus(t, err, codes.Aborted, "ETAG_MISMATCH")
	if got := getObject(t, objects); got.GetStatus().GetMd5() != md5hex([]byte("hello")) {
		t.Fatalf("rejected write changed the object: %+v", got)
	}
	// A matching sha256 is accepted.
	wop, err := objects.WriteObject(ctx, &storagev1.WriteObjectRequest{Name: writeName, Contents: []byte("x"), Sha256: sha256hex([]byte("x"))})
	if err != nil {
		t.Fatal(err)
	}
	if done := waitOp(t, ops, wop.GetName()); done.GetError() != nil {
		t.Fatal(done.GetError())
	}
	// validate_only stores nothing.
	before := getObject(t, objects)
	vop, err := objects.WriteObject(ctx, &storagev1.WriteObjectRequest{Name: writeName, Contents: []byte("zzz"), ValidateOnly: true})
	if err != nil || !vop.GetDone() {
		t.Fatalf("validate_only %v %v", vop, err)
	}
	if got := getObject(t, objects); got.GetEtag() != before.GetEtag() {
		t.Fatalf("validate_only wrote: %+v", got)
	}
}

func TestWriteObjectEmptyPayload(t *testing.T) {
	objects, ops := seedObject(t, []byte("hello"))
	ctx := context.Background()
	wop, err := objects.WriteObject(ctx, &storagev1.WriteObjectRequest{Name: writeName, Contents: []byte{}})
	if err != nil {
		t.Fatal(err)
	}
	if done := waitOp(t, ops, wop.GetName()); done.GetError() != nil {
		t.Fatal(done.GetError())
	}
	got := getObject(t, objects)
	if got.GetStatus().GetSizeBytes() != 0 || got.GetStatus().GetMd5() != "d41d8cd98f00b204e9800998ecf8427e" {
		t.Fatalf("empty %+v", got)
	}
	read, err := objects.ReadObject(ctx, &storagev1.ReadObjectRequest{Name: writeName})
	if err != nil || len(read.GetContents()) != 0 || read.GetObject().GetStatus().GetMd5() != got.GetStatus().GetMd5() {
		t.Fatalf("read %v %v", read, err)
	}
}

func TestReadObject(t *testing.T) {
	objects, _ := seedObject(t, []byte("hello"))
	ctx := context.Background()
	obj := getObject(t, objects)
	read, err := objects.ReadObject(ctx, &storagev1.ReadObjectRequest{Name: writeName, Etag: obj.GetEtag()})
	if err != nil {
		t.Fatal(err)
	}
	if string(read.GetContents()) != "hello" || read.GetObject().GetStatus().GetMd5() != md5hex([]byte("hello")) ||
		read.GetObject().GetSpec().GetSha256() != sha256hex([]byte("hello")) || read.GetObject().GetEtag() != obj.GetEtag() {
		t.Fatalf("read %+v", read)
	}
	_, err = objects.ReadObject(ctx, &storagev1.ReadObjectRequest{Name: writeName, Etag: "999"})
	wantStatus(t, err, codes.Aborted, "ETAG_MISMATCH")
	_, err = objects.ReadObject(ctx, &storagev1.ReadObjectRequest{Name: writeBucket + "/objects/missing"})
	wantStatus(t, err, codes.NotFound, "RESOURCE_NOT_FOUND")
}

func TestUpdateVolumeExpandsAndRejectsShrink(t *testing.T) {
	vols, _, _, _, ops := dial(t, "")
	ctx := context.Background()
	const name = "projects/local/zones/az-1/volumes/data-1"
	created, err := vols.CreateVolume(ctx, &storagev1.CreateVolumeRequest{
		Parent: "projects/local/zones/az-1", VolumeId: "data-1",
		Volume: &storagev1.Volume{Spec: &storagev1.VolumeSpec{SizeGib: 2, Type: storagev1.VolumeType_VOLUME_TYPE_SSD}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if done := waitOp(t, ops, created.GetName()); done.GetError() != nil {
		t.Fatal(done.GetError())
	}
	grow := &storagev1.UpdateVolumeRequest{
		Volume:     &storagev1.Volume{Name: name, Spec: &storagev1.VolumeSpec{SizeGib: 5}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"spec.size_gib"}},
		RequestId:  "44444444-4444-4444-8444-444444444444",
	}
	op, err := vols.UpdateVolume(ctx, grow)
	if err != nil {
		t.Fatal(err)
	}
	again, err := vols.UpdateVolume(ctx, grow)
	if err != nil {
		t.Fatal(err)
	}
	if again.GetName() != op.GetName() {
		t.Fatalf("replay %s want %s", again.GetName(), op.GetName())
	}
	done := waitOp(t, ops, op.GetName())
	if done.GetError() != nil {
		t.Fatalf("operation error %+v", done.GetError())
	}
	vol, err := vols.GetVolume(ctx, &storagev1.GetVolumeRequest{Name: name})
	if err != nil {
		t.Fatal(err)
	}
	if vol.GetSpec().GetSizeGib() != 5 || vol.GetStatus().GetSizeGib() != 5 {
		t.Fatalf("volume %+v", vol)
	}
	for _, tc := range []struct {
		name string
		req  *storagev1.UpdateVolumeRequest
		code codes.Code
	}{
		{"shrink", &storagev1.UpdateVolumeRequest{Volume: &storagev1.Volume{Name: name, Spec: &storagev1.VolumeSpec{SizeGib: 3}}, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"spec"}}}, codes.InvalidArgument},
		{"labels", &storagev1.UpdateVolumeRequest{Volume: &storagev1.Volume{Name: name, Spec: &storagev1.VolumeSpec{SizeGib: 6}}, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}}}, codes.Unimplemented},
		{"no mask", &storagev1.UpdateVolumeRequest{Volume: &storagev1.Volume{Name: name, Spec: &storagev1.VolumeSpec{SizeGib: 6}}}, codes.Unimplemented},
		{"stale etag", &storagev1.UpdateVolumeRequest{Volume: &storagev1.Volume{Name: name, Etag: "1", Spec: &storagev1.VolumeSpec{SizeGib: 6}}, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"spec"}}}, codes.Aborted},
	} {
		_, err := vols.UpdateVolume(ctx, tc.req)
		if st, _ := status.FromError(err); st.Code() != tc.code {
			t.Fatalf("%s: code %v err %v", tc.name, st.Code(), err)
		}
	}
	after, err := vols.GetVolume(ctx, &storagev1.GetVolumeRequest{Name: name})
	if err != nil {
		t.Fatal(err)
	}
	if after.GetSpec().GetSizeGib() != 5 {
		t.Fatalf("size after rejects %d", after.GetSpec().GetSizeGib())
	}
}
