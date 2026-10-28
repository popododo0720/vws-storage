// Package server is the gRPC boundary. It parses vws-proto into model values,
// calls storage.Service, and renders the result. Submit is the only write.
package server

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
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

// Config is process policy for which projects this server answers.
// An empty Projects list accepts every syntactically valid project.
type Config struct {
	Projects []model.Project
}

// Server implements the storage gRPC services.
// Project comes from each request. Zones stay on the engine.
type Server struct {
	storagev1.UnimplementedVolumeServiceServer
	storagev1.UnimplementedSnapshotServiceServer
	storagev1.UnimplementedBucketServiceServer
	storagev1.UnimplementedObjectServiceServer
	longrunningpb.UnimplementedOperationsServer
	svc      *storage.Service
	projects map[model.Project]struct{}
}

// New returns a server. A nil or empty project list does not pin a default project.
func New(svc *storage.Service, cfg Config) *Server {
	var projects map[model.Project]struct{}
	if len(cfg.Projects) > 0 {
		projects = make(map[model.Project]struct{}, len(cfg.Projects))
		for _, p := range cfg.Projects {
			projects[p] = struct{}{}
		}
	}
	return &Server{svc: svc, projects: projects}
}

// Register attaches the storage services and long-running operations to gs.
func Register(gs *grpc.Server, svc *storage.Service, cfg Config) {
	s := New(svc, cfg)
	storagev1.RegisterVolumeServiceServer(gs, s)
	storagev1.RegisterSnapshotServiceServer(gs, s)
	storagev1.RegisterBucketServiceServer(gs, s)
	storagev1.RegisterObjectServiceServer(gs, s)
	longrunningpb.RegisterOperationsServer(gs, s)
}

// BlockServices are the registered services that need the block backend.
// ObjectServices are the others, with the operations service.
var (
	BlockServices = []string{
		storagev1.VolumeService_ServiceDesc.ServiceName,
		storagev1.SnapshotService_ServiceDesc.ServiceName,
	}
	ObjectServices = []string{
		storagev1.BucketService_ServiceDesc.ServiceName,
		storagev1.ObjectService_ServiceDesc.ServiceName,
		longrunningpb.Operations_ServiceDesc.ServiceName,
	}
)

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

// UpdateVolume grows a volume (mask spec or spec.size_gib) or sets its labels
// and type (mask labels, spec.type, or both). A size below the stored one is
// INVALID_ARGUMENT, and the same size is a succeeded operation. Labels and
// type are stored at once and the operation is already done; a type the
// backend cannot serve in place is INVALID_ARGUMENT. Size and labels/type in
// one mask is INVALID_ARGUMENT: they are separate operations.
func (s *Server) UpdateVolume(ctx context.Context, in *storagev1.UpdateVolumeRequest) (*longrunningpb.Operation, error) {
	grow, modify, ok := volumeMask(in.GetUpdateMask().GetPaths())
	if !ok {
		return nil, status.Error(codes.Unimplemented, "volume field mask")
	}
	if grow && len(modify) > 0 {
		return nil, toStatus(invalidSpec("update_mask: size and labels or type are separate updates"))
	}
	id, zone, err := s.parseVolumeName(in.GetVolume().GetName())
	if err != nil {
		return nil, toStatus(err)
	}
	if len(modify) > 0 {
		return s.modifyVolume(ctx, in, id, zone, modify)
	}
	size, err := model.VolumeSize(in.GetVolume().GetSpec().GetSizeGib())
	if err != nil {
		return nil, toStatus(invalidSpec("size_gib"))
	}
	ver, err := expectedVersion(in.GetVolume().GetEtag())
	if err != nil {
		return nil, toStatus(err)
	}
	req := model.ExpandVolumeRequest{Volume: id, Zone: zone, Size: size, Expected: ver}
	if in.GetValidateOnly() {
		if err := req.Validate(); err != nil {
			return nil, toStatus(err)
		}
		return doneOnly(s.volumeName(zone, id), commonv1.OperationVerb_OPERATION_VERB_UPDATE)
	}
	return s.submit(ctx, in.GetRequestId(), req)
}

func (s *Server) modifyVolume(ctx context.Context, in *storagev1.UpdateVolumeRequest, id model.VolumeID, zone model.Zone, paths map[string]bool) (*longrunningpb.Operation, error) {
	ver, err := expectedVersion(in.GetVolume().GetEtag())
	if err != nil {
		return nil, toStatus(err)
	}
	req := model.ModifyVolumeRequest{Volume: id, Zone: zone, Expected: ver}
	if paths["labels"] {
		req.SetLabels = true
		if req.Labels, err = model.ParseLabels(in.GetVolume().GetLabels()); err != nil {
			return nil, toStatus(invalidSpec("labels"))
		}
	}
	if paths["spec.type"] {
		if req.Type, err = volumeType(in.GetVolume().GetSpec().GetType()); err != nil {
			return nil, toStatus(err)
		}
	}
	if in.GetValidateOnly() {
		if err := req.Validate(); err != nil {
			return nil, toStatus(err)
		}
		return doneOnly(s.volumeName(zone, id), commonv1.OperationVerb_OPERATION_VERB_UPDATE)
	}
	return s.submit(ctx, in.GetRequestId(), req)
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
	return s.volumeMsg(ctx, vol), nil
}

// ListVolumes implements VolumeService.
func (s *Server) ListVolumes(ctx context.Context, in *storagev1.ListVolumesRequest) (*storagev1.ListVolumesResponse, error) {
	project, zone, err := s.parseVolumeParent(in.GetParent())
	if err != nil {
		return nil, toStatus(err)
	}
	page, err := listPage(ctx, in.GetPageSize(), in.GetPageToken(), func(ctx context.Context, p model.PageRequest) (model.Page[model.Volume], error) {
		return s.svc.ListVolumes(ctx, project, zone, p)
	})
	if err != nil {
		return nil, toStatus(err)
	}
	out := &storagev1.ListVolumesResponse{NextPageToken: page.NextToken}
	attached := s.svc.Attachments(ctx, page.Items)
	for _, vol := range page.Items {
		out.Volumes = append(out.Volumes, s.volumeMsgAttached(ctx, vol, attached))
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
	id, region, err := s.parseSnapshotName(in.GetName())
	if err != nil {
		return nil, toStatus(err)
	}
	ver, err := expectedVersion(in.GetEtag())
	if err != nil {
		return nil, toStatus(err)
	}
	return s.submit(ctx, in.GetRequestId(), model.DeleteSnapshotRequest{
		Snapshot: id, Region: region, Expected: ver, AllowMissing: in.GetAllowMissing(),
	})
}

// GetSnapshot implements SnapshotService.
func (s *Server) GetSnapshot(ctx context.Context, in *storagev1.GetSnapshotRequest) (*storagev1.Snapshot, error) {
	id, region, err := s.parseSnapshotName(in.GetName())
	if err != nil {
		return nil, toStatus(err)
	}
	snap, err := s.svc.Snapshot(ctx, id)
	if err != nil {
		return nil, toStatus(err)
	}
	if snap.Region != region {
		return nil, toStatus(notFound("snapshot"))
	}
	return s.snapshotMsg(ctx, snap), nil
}

// ListSnapshots implements SnapshotService.
func (s *Server) ListSnapshots(ctx context.Context, in *storagev1.ListSnapshotsRequest) (*storagev1.ListSnapshotsResponse, error) {
	project, region, err := s.parseSnapshotParent(in.GetParent())
	if err != nil {
		return nil, toStatus(err)
	}
	page, err := listPage(ctx, in.GetPageSize(), in.GetPageToken(), func(ctx context.Context, p model.PageRequest) (model.Page[model.Snapshot], error) {
		return s.svc.ListSnapshots(ctx, project, region, p)
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
	project, err := s.parseBucketParent(in.GetParent())
	if err != nil {
		return nil, toStatus(err)
	}
	page, err := listPage(ctx, in.GetPageSize(), in.GetPageToken(), func(ctx context.Context, p model.PageRequest) (model.Page[model.Bucket], error) {
		return s.svc.ListBuckets(ctx, project, p)
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
	return s.submitUpload(ctx, in.GetRequestId(), req, contents)
}

// UpdateObject implements ObjectService.
// A payload replaces the object. An empty payload keeps the stored bytes and
// applies content type and labels. Proto3 bytes have no presence, so a
// zero-length replacement is expressed as an empty payload and keeps the bytes.
func (s *Server) UpdateObject(ctx context.Context, in *storagev1.UpdateObjectRequest) (*longrunningpb.Operation, error) {
	if !objectMaskOK(in.GetUpdateMask().GetPaths()) {
		return nil, status.Error(codes.Unimplemented, "object field mask")
	}
	req, contents, err := s.objectUpdate(in)
	if err != nil {
		return nil, toStatus(err)
	}
	if in.GetValidateOnly() {
		if err := req.Validate(); err != nil {
			return nil, toStatus(err)
		}
		return doneOnly(s.objectName(req.Path), commonv1.OperationVerb_OPERATION_VERB_UPDATE)
	}
	return s.submitUpload(ctx, in.GetRequestId(), req, contents)
}

// WriteObject implements ObjectService. It replaces the payload of an existing
// object, including with a zero-length payload, and keeps content type and
// labels. It never creates. Unset contents is INVALID_SPEC; contents above
// maxInlineObject is INVALID_SPEC too, and stores nothing.
func (s *Server) WriteObject(ctx context.Context, in *storagev1.WriteObjectRequest) (*longrunningpb.Operation, error) {
	path, err := s.parseObjectName(in.GetName())
	if err != nil {
		return nil, toStatus(err)
	}
	if in.Contents == nil || len(in.Contents) > maxInlineObject {
		return nil, toStatus(invalidSpec("contents"))
	}
	body := in.GetContents()
	sum := sha256.Sum256(body)
	if in.GetSha256() != "" {
		parsed, err := model.ParseDigest(in.GetSha256())
		if err != nil || parsed != sum {
			return nil, toStatus(invalidSpec("sha256"))
		}
	}
	ver, err := expectedVersion(in.GetEtag())
	if err != nil {
		return nil, toStatus(err)
	}
	req := model.WriteObjectRequest{Path: path, Size: model.Bytes(len(body)), SHA256: sum, Expected: ver}
	if in.GetValidateOnly() {
		if err := req.Validate(); err != nil {
			return nil, toStatus(err)
		}
		return doneOnly(s.objectName(path), commonv1.OperationVerb_OPERATION_VERB_UPDATE)
	}
	return s.submitUpload(ctx, in.GetRequestId(), req, body)
}

// ReadObject implements ObjectService. It returns the whole stored payload and
// the object record it belongs to. A write that commits between the record
// read and the byte fetch moves the head, so the read retries on the new head.
func (s *Server) ReadObject(ctx context.Context, in *storagev1.ReadObjectRequest) (*storagev1.ReadObjectResponse, error) {
	path, err := s.parseObjectName(in.GetName())
	if err != nil {
		return nil, toStatus(err)
	}
	want, err := expectedVersion(in.GetEtag())
	if err != nil {
		return nil, toStatus(err)
	}
	var lastErr error
	for range readAttempts {
		obj, loc, err := s.svc.LocateObject(ctx, path)
		if err != nil {
			return nil, toStatus(err)
		}
		if want != 0 && obj.Meta.Version != want {
			return nil, toStatus(model.ErrETagMismatch)
		}
		if obj.Size > maxInlineObject {
			return nil, status.Error(codes.FailedPrecondition, "stored payload exceeds the read limit")
		}
		body, err := getBytes(ctx, loc)
		if err == nil && storedBytes(obj, body) {
			return &storagev1.ReadObjectResponse{Contents: body, Object: s.objectMsg(obj)}, nil
		}
		if ctx.Err() != nil {
			return nil, toStatus(ctx.Err())
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = errors.New("stored payload changed during read")
	}
	return nil, status.Error(codes.Unavailable, lastErr.Error())
}

// storedBytes reports whether body is the payload obj records. A record
// committed without a SHA-256 (an RGW upload before the backend computed one)
// is checked against its single-part MD5 ETag instead. A record with neither
// is never accepted.
func storedBytes(obj model.Object, body []byte) bool {
	if model.Bytes(len(body)) != obj.Size {
		return false
	}
	if !obj.SHA256.IsZero() {
		return sha256.Sum256(body) == [32]byte(obj.SHA256)
	}
	return obj.MD5 != (model.MD5{}) && md5.Sum(body) == [16]byte(obj.MD5) //nolint:gosec // md5 checks the S3 ETag
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
	project, err := s.parseOperationParent(in.GetName())
	if err != nil {
		return nil, toStatus(err)
	}
	if in.GetFilter() != "" {
		return nil, toStatus(invalidSpec("filter"))
	}
	n, err := pageSize(in.GetPageSize())
	if err != nil {
		return nil, toStatus(err)
	}
	page, err := s.svc.ListOperations(ctx, storage.OperationQuery{Project: project}, model.PageRequest{Size: n, Token: in.GetPageToken()})
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

func (s *Server) submitUpload(ctx context.Context, requestID string, req model.Request, body []byte) (*longrunningpb.Operation, error) {
	if req.Project().IsZero() {
		return nil, toStatus(invalidID("project"))
	}
	tok, err := tokenFor(requestID)
	if err != nil {
		return nil, toStatus(err)
	}
	op, err := s.svc.Submit(ctx, tok, req)
	if err != nil {
		return nil, toStatus(err)
	}
	if op.Upload != nil {
		putCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		err := putBytes(putCtx, *op.Upload, body)
		cancel()
		if err != nil {
			again, gerr := s.svc.Operation(ctx, op.ID)
			if gerr != nil || !again.Terminal() || !again.Err().IsZero() {
				return nil, status.Error(codes.Unavailable, err.Error())
			}
			op = again
		}
	}
	return s.operation(ctx, op)
}

// volumeMask sorts the UpdateVolume mask into a size update and the labels
// and type paths. An empty mask or any other path is not served.
func volumeMask(paths []string) (grow bool, modify map[string]bool, ok bool) {
	if len(paths) == 0 {
		return false, nil, false
	}
	modify = map[string]bool{}
	for _, p := range paths {
		switch p {
		case "spec", "spec.size_gib":
			grow = true
		case "labels", "spec.type":
			modify[p] = true
		default:
			return false, nil, false
		}
	}
	return grow, modify, true
}

func objectMaskOK(paths []string) bool {
	if len(paths) == 0 {
		return true
	}
	var spec, labels bool
	for _, p := range paths {
		switch p {
		case "spec":
			spec = true
		case "labels":
			labels = true
		default:
			return false
		}
	}
	return spec && labels
}

func (s *Server) submit(ctx context.Context, requestID string, req model.Request) (*longrunningpb.Operation, error) {
	if req.Project().IsZero() {
		return nil, toStatus(invalidID("project"))
	}
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
	labels, err := model.ParseLabels(in.GetObject().GetLabels())
	if err != nil {
		return model.PutObjectRequest{}, model.ObjectPath{}, nil, invalidSpec("labels")
	}
	spec := in.GetObject().GetSpec()
	if spec.GetCacheControl() != "" || len(spec.GetMetadata()) > 0 {
		return model.PutObjectRequest{}, model.ObjectPath{}, nil, invalidSpec("object")
	}
	raw := in.GetObjectId()
	path, err := s.parseObjectName("projects/" + bucket.Project().String() + "/buckets/" + bucket.Name() + "/objects/" + raw)
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
		Labels: labels,
	}, path, body, nil
}

func (s *Server) objectUpdate(in *storagev1.UpdateObjectRequest) (model.UpdateObjectRequest, []byte, error) {
	obj := in.GetObject()
	if obj == nil || obj.GetSpec() == nil {
		return model.UpdateObjectRequest{}, nil, invalidSpec("object")
	}
	path, err := s.parseObjectName(obj.GetName())
	if err != nil {
		return model.UpdateObjectRequest{}, nil, err
	}
	spec := obj.GetSpec()
	if spec.GetCacheControl() != "" || len(spec.GetMetadata()) > 0 {
		return model.UpdateObjectRequest{}, nil, invalidSpec("object")
	}
	labels, err := model.ParseLabels(obj.GetLabels())
	if err != nil {
		return model.UpdateObjectRequest{}, nil, invalidSpec("labels")
	}
	ver, err := expectedVersion(obj.GetEtag())
	if err != nil {
		return model.UpdateObjectRequest{}, nil, err
	}
	body := in.GetContents()
	if len(body) > maxInlineObject {
		return model.UpdateObjectRequest{}, nil, invalidSpec("contents")
	}
	req := model.UpdateObjectRequest{
		Path: path, ContentType: spec.GetContentType(), Labels: labels,
		Expected: ver, AllowMissing: in.GetAllowMissing(), HasContents: len(body) > 0,
	}
	if !req.HasContents {
		if req.AllowMissing {
			return model.UpdateObjectRequest{}, nil, invalidSpec("contents")
		}
		return req, nil, nil
	}
	sum := sha256.Sum256(body)
	if spec.GetSha256() != "" {
		parsed, err := model.ParseDigest(spec.GetSha256())
		if err != nil || parsed != sum {
			return model.UpdateObjectRequest{}, nil, invalidSpec("sha256")
		}
	}
	req.Size = model.Bytes(len(body))
	req.SHA256 = sum
	return req, body, nil
}

func listPage[T any](ctx context.Context, size int32, token string, list func(context.Context, model.PageRequest) (model.Page[T], error)) (model.Page[T], error) {
	n, err := pageSize(size)
	if err != nil {
		return model.Page[T]{}, err
	}
	return list(ctx, model.PageRequest{Size: n, Token: token})
}

func getBytes(ctx context.Context, loc model.Locator) (body []byte, err error) {
	method := loc.Method
	if method == "" {
		method = http.MethodGet
	}
	resp, err := doLocator(ctx, method, loc, http.NoBody)
	if err != nil {
		return nil, err
	}
	defer closeBody(resp.Body, &err)
	if resp.StatusCode/100 != 2 {
		return nil, statusErr(resp)
	}
	body, err = io.ReadAll(io.LimitReader(resp.Body, maxInlineObject+1))
	if err != nil {
		return nil, fmt.Errorf("read object bytes: %w", err)
	}
	if len(body) > maxInlineObject {
		return nil, errors.New("payload exceeds the read limit")
	}
	return body, nil
}

func putBytes(ctx context.Context, loc model.Locator, body []byte) (err error) {
	method := loc.Method
	if method == "" {
		method = http.MethodPut
	}
	resp, err := doLocator(ctx, method, loc, bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer closeBody(resp.Body, &err)
	if resp.StatusCode/100 != 2 {
		return statusErr(resp)
	}
	return checkPutETag(resp.Header.Get("ETag"), body)
}

// checkPutETag compares a single-part MD5 ETag from the PUT response with the
// bytes sent. It is the integrity check when the store reports no SHA-256.
// An absent ETag or one that is not an MD5 (multipart) is not compared.
func checkPutETag(etag string, body []byte) error {
	got, err := model.ParseMD5(strings.Trim(etag, `"`))
	if err != nil {
		return nil //nolint:nilerr // not an MD5 ETag, so there is nothing to compare
	}
	if got != model.MD5(md5.Sum(body)) {
		return errors.New("uploaded bytes do not match the PUT response md5")
	}
	return nil
}

func doLocator(ctx context.Context, method string, loc model.Locator, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, loc.URL, body)
	if err != nil {
		return nil, fmt.Errorf("build object request: %w", err)
	}
	for k, v := range loc.Headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("object request: %w", err)
	}
	return resp, nil
}

// statusErr reports a non-2xx object response with the start of its body.
// A body that cannot be read is left out of the message.
func statusErr(resp *http.Response) error {
	msg, err := io.ReadAll(io.LimitReader(resp.Body, 512))
	if err != nil {
		msg = nil
	}
	return errors.New(resp.Status + " " + string(bytes.TrimSpace(msg)))
}

func closeBody(c io.Closer, err *error) {
	if cerr := c.Close(); cerr != nil {
		*err = errors.Join(*err, fmt.Errorf("close object response: %w", cerr))
	}
}
