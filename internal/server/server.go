// Package server is the gRPC boundary. It parses vws-proto into model values,
// calls storage.Service, and renders the result. Submit is the only write.
package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net/http"
	"time"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	commonv1 "github.com/popododo0720/vws-proto/gen/go/vws/common/v1"
	storagev1 "github.com/popododo0720/vws-proto/gen/go/vws/storage/v1"

	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/storage"
)

// MaxRecvMsgSize fits a CreateObject payload at the proto limit plus the envelope.
const MaxRecvMsgSize = 8 << 20

// Server implements the storage gRPC services.
// Project and region are fixed for the process. Zones stay on the engine.
type Server struct {
	storagev1.UnimplementedVolumeServiceServer
	storagev1.UnimplementedSnapshotServiceServer
	storagev1.UnimplementedBucketServiceServer
	storagev1.UnimplementedObjectServiceServer
	longrunningpb.UnimplementedOperationsServer
	svc     *storage.Service
	project string
	region  string
}

// New returns a server for project "local" and region "local".
func New(svc *storage.Service) *Server {
	return &Server{svc: svc, project: "local", region: "local"}
}

// Register attaches the storage services and long-running operations to gs.
func Register(gs *grpc.Server, svc *storage.Service) {
	s := New(svc)
	storagev1.RegisterVolumeServiceServer(gs, s)
	storagev1.RegisterSnapshotServiceServer(gs, s)
	storagev1.RegisterBucketServiceServer(gs, s)
	storagev1.RegisterObjectServiceServer(gs, s)
	longrunningpb.RegisterOperationsServer(gs, s)
}

// CreateVolume implements VolumeService.
func (s *Server) CreateVolume(ctx context.Context, in *storagev1.CreateVolumeRequest) (*longrunningpb.Operation, error) {
	req, name, err := s.volumeCreate(in)
	if err != nil {
		return nil, toStatus(err)
	}
	if in.GetValidateOnly() {
		if err := req.Validate(); err != nil {
			return nil, toStatus(err)
		}
		return doneOnly(name, commonv1.OperationVerb_OPERATION_VERB_CREATE)
	}
	return s.submit(ctx, in.GetRequestId(), req)
}

// UpdateVolume is not a storage transition.
func (s *Server) UpdateVolume(context.Context, *storagev1.UpdateVolumeRequest) (*longrunningpb.Operation, error) {
	return nil, status.Error(codes.Unimplemented, "volume update is not a storage transition")
}

// DeleteVolume implements VolumeService.
func (s *Server) DeleteVolume(ctx context.Context, in *storagev1.DeleteVolumeRequest) (*longrunningpb.Operation, error) {
	id, zone, err := s.parseVolumeName(in.GetName())
	if err != nil {
		return nil, toStatus(err)
	}
	ver, err := expectedVersion(in.GetEtag())
	if err != nil {
		return nil, toStatus(err)
	}
	return s.submit(ctx, in.GetRequestId(), model.DeleteVolumeRequest{
		Volume: id, Zone: zone, Expected: ver, AllowMissing: in.GetAllowMissing(),
	})
}

// GetVolume implements VolumeService.
func (s *Server) GetVolume(ctx context.Context, in *storagev1.GetVolumeRequest) (*storagev1.Volume, error) {
	id, zone, err := s.parseVolumeName(in.GetName())
	if err != nil {
		return nil, toStatus(err)
	}
	vol, err := s.svc.Volume(ctx, id)
	if err != nil {
		return nil, toStatus(err)
	}
	if vol.Spec.Zone != zone {
		return nil, toStatus(notFound("volume"))
	}
	return s.volumeMsg(vol), nil
}

// ListVolumes implements VolumeService.
func (s *Server) ListVolumes(ctx context.Context, in *storagev1.ListVolumesRequest) (*storagev1.ListVolumesResponse, error) {
	zone, err := s.parseVolumeParent(in.GetParent())
	if err != nil {
		return nil, toStatus(err)
	}
	page, err := listPage(ctx, in.GetPageSize(), in.GetPageToken(), func(ctx context.Context, p model.PageRequest) (model.Page[model.Volume], error) {
		return s.svc.ListVolumes(ctx, zone, p)
	})
	if err != nil {
		return nil, toStatus(err)
	}
	out := &storagev1.ListVolumesResponse{NextPageToken: page.NextToken}
	for _, vol := range page.Items {
		out.Volumes = append(out.Volumes, s.volumeMsg(vol))
	}
	return out, nil
}

// ReportVolumeStatus is not a write. Run reconciles volumes.
func (s *Server) ReportVolumeStatus(context.Context, *storagev1.ReportVolumeStatusRequest) (*storagev1.ReportVolumeStatusResponse, error) {
	return nil, status.Error(codes.Unimplemented, "volume status is reconciled by Service.Run")
}

// CreateSnapshot implements SnapshotService.
func (s *Server) CreateSnapshot(ctx context.Context, in *storagev1.CreateSnapshotRequest) (*longrunningpb.Operation, error) {
	req, name, err := s.snapshotCreate(in)
	if err != nil {
		return nil, toStatus(err)
	}
	if in.GetValidateOnly() {
		if err := req.Validate(); err != nil {
			return nil, toStatus(err)
		}
		return doneOnly(name, commonv1.OperationVerb_OPERATION_VERB_CREATE)
	}
	return s.submit(ctx, in.GetRequestId(), req)
}

// UpdateSnapshot is not a storage transition.
func (s *Server) UpdateSnapshot(context.Context, *storagev1.UpdateSnapshotRequest) (*longrunningpb.Operation, error) {
	return nil, status.Error(codes.Unimplemented, "snapshot update is not a storage transition")
}

// DeleteSnapshot implements SnapshotService.
func (s *Server) DeleteSnapshot(ctx context.Context, in *storagev1.DeleteSnapshotRequest) (*longrunningpb.Operation, error) {
	id, err := s.parseSnapshotName(in.GetName())
	if err != nil {
		return nil, toStatus(err)
	}
	ver, err := expectedVersion(in.GetEtag())
	if err != nil {
		return nil, toStatus(err)
	}
	return s.submit(ctx, in.GetRequestId(), model.DeleteSnapshotRequest{
		Snapshot: id, Expected: ver, AllowMissing: in.GetAllowMissing(),
	})
}

// GetSnapshot implements SnapshotService.
func (s *Server) GetSnapshot(ctx context.Context, in *storagev1.GetSnapshotRequest) (*storagev1.Snapshot, error) {
	id, err := s.parseSnapshotName(in.GetName())
	if err != nil {
		return nil, toStatus(err)
	}
	snap, err := s.svc.Snapshot(ctx, id)
	if err != nil {
		return nil, toStatus(err)
	}
	return s.snapshotMsg(ctx, snap), nil
}

// ListSnapshots implements SnapshotService.
func (s *Server) ListSnapshots(ctx context.Context, in *storagev1.ListSnapshotsRequest) (*storagev1.ListSnapshotsResponse, error) {
	if err := s.parseSnapshotParent(in.GetParent()); err != nil {
		return nil, toStatus(err)
	}
	page, err := listPage(ctx, in.GetPageSize(), in.GetPageToken(), func(ctx context.Context, p model.PageRequest) (model.Page[model.Snapshot], error) {
		return s.svc.ListSnapshots(ctx, p)
	})
	if err != nil {
		return nil, toStatus(err)
	}
	out := &storagev1.ListSnapshotsResponse{NextPageToken: page.NextToken}
	for _, snap := range page.Items {
		out.Snapshots = append(out.Snapshots, s.snapshotMsg(ctx, snap))
	}
	return out, nil
}

// ReportSnapshotStatus is not a write. Run reconciles snapshots.
func (s *Server) ReportSnapshotStatus(context.Context, *storagev1.ReportSnapshotStatusRequest) (*storagev1.ReportSnapshotStatusResponse, error) {
	return nil, status.Error(codes.Unimplemented, "snapshot status is reconciled by Service.Run")
}

// CreateBucket implements BucketService.
func (s *Server) CreateBucket(ctx context.Context, in *storagev1.CreateBucketRequest) (*longrunningpb.Operation, error) {
	req, name, err := s.bucketCreate(in)
	if err != nil {
		return nil, toStatus(err)
	}
	if in.GetValidateOnly() {
		if err := req.Validate(); err != nil {
			return nil, toStatus(err)
		}
		return doneOnly(name, commonv1.OperationVerb_OPERATION_VERB_CREATE)
	}
	return s.submit(ctx, in.GetRequestId(), req)
}

// UpdateBucket is not a storage transition.
func (s *Server) UpdateBucket(context.Context, *storagev1.UpdateBucketRequest) (*longrunningpb.Operation, error) {
	return nil, status.Error(codes.Unimplemented, "bucket update is not a storage transition")
}

// DeleteBucket implements BucketService.
func (s *Server) DeleteBucket(ctx context.Context, in *storagev1.DeleteBucketRequest) (*longrunningpb.Operation, error) {
	name, err := s.parseBucketName(in.GetName())
	if err != nil {
		return nil, toStatus(err)
	}
	ver, err := expectedVersion(in.GetEtag())
	if err != nil {
		return nil, toStatus(err)
	}
	return s.submit(ctx, in.GetRequestId(), model.DeleteBucketRequest{
		Bucket: name, Expected: ver, AllowMissing: in.GetAllowMissing(),
	})
}

// GetBucket implements BucketService.
func (s *Server) GetBucket(ctx context.Context, in *storagev1.GetBucketRequest) (*storagev1.Bucket, error) {
	name, err := s.parseBucketName(in.GetName())
	if err != nil {
		return nil, toStatus(err)
	}
	b, err := s.svc.Bucket(ctx, name)
	if err != nil {
		return nil, toStatus(err)
	}
	return s.bucketMsg(b), nil
}

// ListBuckets implements BucketService.
func (s *Server) ListBuckets(ctx context.Context, in *storagev1.ListBucketsRequest) (*storagev1.ListBucketsResponse, error) {
	if err := s.parseBucketParent(in.GetParent()); err != nil {
		return nil, toStatus(err)
	}
	page, err := listPage(ctx, in.GetPageSize(), in.GetPageToken(), func(ctx context.Context, p model.PageRequest) (model.Page[model.Bucket], error) {
		return s.svc.ListBuckets(ctx, p)
	})
	if err != nil {
		return nil, toStatus(err)
	}
	out := &storagev1.ListBucketsResponse{NextPageToken: page.NextToken}
	for _, b := range page.Items {
		out.Buckets = append(out.Buckets, s.bucketMsg(b))
	}
	return out, nil
}

// ReportBucketStatus is not a write. Run reconciles buckets.
func (s *Server) ReportBucketStatus(context.Context, *storagev1.ReportBucketStatusRequest) (*storagev1.ReportBucketStatusResponse, error) {
	return nil, status.Error(codes.Unimplemented, "bucket status is reconciled by Service.Run")
}

// CreateObject implements ObjectService. The bytes are written to the signed
// upload URL before the operation is returned.
func (s *Server) CreateObject(ctx context.Context, in *storagev1.CreateObjectRequest) (*longrunningpb.Operation, error) {
	req, path, contents, err := s.objectCreate(in)
	if err != nil {
		return nil, toStatus(err)
	}
	if in.GetValidateOnly() {
		if err := req.Validate(); err != nil {
			return nil, toStatus(err)
		}
		return doneOnly(s.objectName(path), commonv1.OperationVerb_OPERATION_VERB_CREATE)
	}
	tok, err := tokenFor(in.GetRequestId())
	if err != nil {
		return nil, toStatus(err)
	}
	op, err := s.svc.Submit(ctx, tok, req)
	if err != nil {
		return nil, toStatus(err)
	}
	if op.Upload != nil {
		putCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		err := putBytes(putCtx, *op.Upload, contents)
		cancel()
		if err != nil {
			if _, gerr := s.svc.Object(ctx, path); gerr != nil {
				return nil, status.Error(codes.Unavailable, err.Error())
			}
		}
	}
	return s.operation(ctx, op)
}

// UpdateObject is not a storage transition.
func (s *Server) UpdateObject(context.Context, *storagev1.UpdateObjectRequest) (*longrunningpb.Operation, error) {
	return nil, status.Error(codes.Unimplemented, "object update is not a storage transition")
}

// DeleteObject implements ObjectService.
func (s *Server) DeleteObject(ctx context.Context, in *storagev1.DeleteObjectRequest) (*longrunningpb.Operation, error) {
	path, err := s.parseObjectName(in.GetName())
	if err != nil {
		return nil, toStatus(err)
	}
	ver, err := expectedVersion(in.GetEtag())
	if err != nil {
		return nil, toStatus(err)
	}
	return s.submit(ctx, in.GetRequestId(), model.DeleteObjectRequest{
		Path: path, Expected: ver, AllowMissing: in.GetAllowMissing(),
	})
}

// GetObject implements ObjectService. The payload is not returned.
func (s *Server) GetObject(ctx context.Context, in *storagev1.GetObjectRequest) (*storagev1.Object, error) {
	path, err := s.parseObjectName(in.GetName())
	if err != nil {
		return nil, toStatus(err)
	}
	obj, err := s.svc.Object(ctx, path)
	if err != nil {
		return nil, toStatus(err)
	}
	return s.objectMsg(obj), nil
}

// ListObjects implements ObjectService.
func (s *Server) ListObjects(ctx context.Context, in *storagev1.ListObjectsRequest) (*storagev1.ListObjectsResponse, error) {
	bucket, err := s.parseObjectParent(in.GetParent())
	if err != nil {
		return nil, toStatus(err)
	}
	n, err := pageSize(in.GetPageSize())
	if err != nil {
		return nil, toStatus(err)
	}
	page, err := s.svc.ListObjects(ctx, bucket, "", model.PageRequest{Size: n, Token: in.GetPageToken()})
	if err != nil {
		return nil, toStatus(err)
	}
	out := &storagev1.ListObjectsResponse{NextPageToken: page.NextToken}
	for _, obj := range page.Items {
		out.Objects = append(out.Objects, s.objectMsg(obj))
	}
	return out, nil
}

// ReportObjectStatus is not a write. Run reconciles objects.
func (s *Server) ReportObjectStatus(context.Context, *storagev1.ReportObjectStatusRequest) (*storagev1.ReportObjectStatusResponse, error) {
	return nil, status.Error(codes.Unimplemented, "object status is reconciled by Service.Run")
}

// GetOperation implements google.longrunning.Operations.
func (s *Server) GetOperation(ctx context.Context, in *longrunningpb.GetOperationRequest) (*longrunningpb.Operation, error) {
	id, err := s.parseOpName(in.GetName())
	if err != nil {
		return nil, toStatus(err)
	}
	op, err := s.svc.Operation(ctx, id)
	if err != nil {
		return nil, toStatus(err)
	}
	return s.operation(ctx, op)
}

// ListOperations implements google.longrunning.Operations.
func (s *Server) ListOperations(ctx context.Context, in *longrunningpb.ListOperationsRequest) (*longrunningpb.ListOperationsResponse, error) {
	if in.GetName() != "" && in.GetName() != "projects/"+s.project && in.GetName() != "projects/"+s.project+"/operations" {
		return nil, toStatus(invalidID("operation parent"))
	}
	if in.GetFilter() != "" {
		return nil, toStatus(invalidSpec("filter"))
	}
	n, err := pageSize(in.GetPageSize())
	if err != nil {
		return nil, toStatus(err)
	}
	page, err := s.svc.ListOperations(ctx, storage.OperationQuery{}, model.PageRequest{Size: n, Token: in.GetPageToken()})
	if err != nil {
		return nil, toStatus(err)
	}
	out := &longrunningpb.ListOperationsResponse{NextPageToken: page.NextToken}
	for _, op := range page.Items {
		rendered, err := s.operation(ctx, op)
		if err != nil {
			return nil, err
		}
		out.Operations = append(out.Operations, rendered)
	}
	return out, nil
}

// DeleteOperation is not a storage transition.
func (s *Server) DeleteOperation(context.Context, *longrunningpb.DeleteOperationRequest) (*emptypb.Empty, error) {
	return nil, status.Error(codes.Unimplemented, "operation delete is not a storage transition")
}

// CancelOperation is not a storage transition.
func (s *Server) CancelOperation(context.Context, *longrunningpb.CancelOperationRequest) (*emptypb.Empty, error) {
	return nil, status.Error(codes.Unimplemented, "operation cancel is not a storage transition")
}

func (s *Server) submit(ctx context.Context, requestID string, req model.Request) (*longrunningpb.Operation, error) {
	tok, err := tokenFor(requestID)
	if err != nil {
		return nil, toStatus(err)
	}
	op, err := s.svc.Submit(ctx, tok, req)
	if err != nil {
		return nil, toStatus(err)
	}
	return s.operation(ctx, op)
}

func (s *Server) objectCreate(in *storagev1.CreateObjectRequest) (model.PutObjectRequest, model.ObjectPath, []byte, error) {
	bucket, err := s.parseObjectParent(in.GetParent())
	if err != nil {
		return model.PutObjectRequest{}, model.ObjectPath{}, nil, err
	}
	if in.GetObject() == nil || in.GetObject().GetSpec() == nil {
		return model.PutObjectRequest{}, model.ObjectPath{}, nil, invalidSpec("object")
	}
	if err := rejectLabels(in.GetObject().GetLabels()); err != nil {
		return model.PutObjectRequest{}, model.ObjectPath{}, nil, err
	}
	spec := in.GetObject().GetSpec()
	if spec.GetCacheControl() != "" || len(spec.GetMetadata()) > 0 {
		return model.PutObjectRequest{}, model.ObjectPath{}, nil, invalidSpec("object")
	}
	raw := in.GetObjectId()
	path, err := s.parseObjectName("projects/" + s.project + "/buckets/" + bucket.Name() + "/objects/" + raw)
	if err != nil {
		return model.PutObjectRequest{}, model.ObjectPath{}, nil, err
	}
	name := s.objectName(path)
	if got := in.GetObject().GetName(); got != "" && got != name {
		return model.PutObjectRequest{}, model.ObjectPath{}, nil, invalidID("object name")
	}
	body := in.GetContents()
	if len(body) > maxInlineObject {
		return model.PutObjectRequest{}, model.ObjectPath{}, nil, invalidSpec("contents")
	}
	sum := sha256.Sum256(body)
	if spec.GetSha256() != "" {
		parsed, err := model.ParseDigest(spec.GetSha256())
		if err != nil || parsed != sum {
			return model.PutObjectRequest{}, model.ObjectPath{}, nil, invalidSpec("sha256")
		}
	}
	return model.PutObjectRequest{
		Path: path, Size: model.Bytes(len(body)), SHA256: sum, ContentType: spec.GetContentType(),
	}, path, body, nil
}

func listPage[T any](ctx context.Context, size int32, token string, list func(context.Context, model.PageRequest) (model.Page[T], error)) (model.Page[T], error) {
	n, err := pageSize(size)
	if err != nil {
		return model.Page[T]{}, err
	}
	return list(ctx, model.PageRequest{Size: n, Token: token})
}

func putBytes(ctx context.Context, loc model.Locator, body []byte) error {
	method := loc.Method
	if method == "" {
		method = http.MethodPut
	}
	req, err := http.NewRequestWithContext(ctx, method, loc.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	for k, v := range loc.Headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return errors.New(resp.Status + " " + string(bytes.TrimSpace(msg)))
	}
	return nil
}
