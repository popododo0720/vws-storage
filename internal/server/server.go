// Package server is the gRPC boundary. It parses proto into model values,
// calls storage.Service, and renders the result.
package server

import (
	"context"
	"errors"
	"net/url"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	storagev1 "github.com/popododo0720/vws-storage/internal/gen/vws/storage/v1"
	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/storage"
)

// Server implements the storage gRPC services.
type Server struct {
	storagev1.UnimplementedBlockServiceServer
	storagev1.UnimplementedObjectServiceServer
	storagev1.UnimplementedOperationServiceServer
	svc *storage.Service
}

// New returns a server over svc.
func New(svc *storage.Service) *Server { return &Server{svc: svc} }

// Register attaches the storage services to gs.
func Register(gs *grpc.Server, svc *storage.Service) {
	s := New(svc)
	storagev1.RegisterBlockServiceServer(gs, s)
	storagev1.RegisterObjectServiceServer(gs, s)
	storagev1.RegisterOperationServiceServer(gs, s)
}

// CreateVolume implements BlockService.
func (s *Server) CreateVolume(ctx context.Context, in *storagev1.CreateVolumeRequest) (*storagev1.Operation, error) {
	tok, err := model.ParseClientToken(in.GetClientToken())
	if err != nil {
		return nil, toStatus(err)
	}
	zone, err := model.ParseZone(in.GetZone())
	if err != nil {
		return nil, toStatus(err)
	}
	typ, err := model.ParseVolumeType(in.GetType())
	if err != nil {
		return nil, toStatus(err)
	}
	src := model.NoSource()
	if in.GetSourceSnapshotId() != "" {
		id, err := model.ParseSnapshotID(in.GetSourceSnapshotId())
		if err != nil {
			return nil, toStatus(err)
		}
		src, err = model.FromSnapshot(id)
		if err != nil {
			return nil, toStatus(err)
		}
	}
	op, err := s.svc.Submit(ctx, tok, model.CreateVolumeRequest{Spec: model.VolumeSpec{
		Size: model.Bytes(in.GetSizeBytes()), Zone: zone, Type: typ, Source: src,
	}})
	if err != nil {
		return nil, toStatus(err)
	}
	return toOp(op), nil
}

// DeleteVolume implements BlockService.
func (s *Server) DeleteVolume(ctx context.Context, in *storagev1.DeleteVolumeRequest) (*storagev1.Operation, error) {
	tok, id, err := tokenAnd(in.GetClientToken(), in.GetId(), model.ParseVolumeID)
	if err != nil {
		return nil, toStatus(err)
	}
	op, err := s.svc.Submit(ctx, tok, model.DeleteVolumeRequest{Volume: id})
	if err != nil {
		return nil, toStatus(err)
	}
	return toOp(op), nil
}

// GetVolume implements BlockService.
func (s *Server) GetVolume(ctx context.Context, in *storagev1.GetVolumeRequest) (*storagev1.Volume, error) {
	id, err := model.ParseVolumeID(in.GetId())
	if err != nil {
		return nil, toStatus(err)
	}
	vol, err := s.svc.Volume(ctx, id)
	if err != nil {
		return nil, toStatus(err)
	}
	return toVolume(vol), nil
}

// ListVolumes implements BlockService.
func (s *Server) ListVolumes(ctx context.Context, in *storagev1.ListVolumesRequest) (*storagev1.ListVolumesResponse, error) {
	page, err := s.svc.ListVolumes(ctx, pageReq(in.GetPageSize(), in.GetPageToken()))
	if err != nil {
		return nil, toStatus(err)
	}
	out := &storagev1.ListVolumesResponse{NextPageToken: page.NextToken}
	for _, vol := range page.Items {
		out.Volumes = append(out.Volumes, toVolume(vol))
	}
	return out, nil
}

// CreateSnapshot implements BlockService.
func (s *Server) CreateSnapshot(ctx context.Context, in *storagev1.CreateSnapshotRequest) (*storagev1.Operation, error) {
	tok, id, err := tokenAnd(in.GetClientToken(), in.GetSourceVolumeId(), model.ParseVolumeID)
	if err != nil {
		return nil, toStatus(err)
	}
	op, err := s.svc.Submit(ctx, tok, model.CreateSnapshotRequest{Source: id})
	if err != nil {
		return nil, toStatus(err)
	}
	return toOp(op), nil
}

// DeleteSnapshot implements BlockService.
func (s *Server) DeleteSnapshot(ctx context.Context, in *storagev1.DeleteSnapshotRequest) (*storagev1.Operation, error) {
	tok, id, err := tokenAnd(in.GetClientToken(), in.GetId(), model.ParseSnapshotID)
	if err != nil {
		return nil, toStatus(err)
	}
	op, err := s.svc.Submit(ctx, tok, model.DeleteSnapshotRequest{Snapshot: id})
	if err != nil {
		return nil, toStatus(err)
	}
	return toOp(op), nil
}

// GetSnapshot implements BlockService.
func (s *Server) GetSnapshot(ctx context.Context, in *storagev1.GetSnapshotRequest) (*storagev1.Snapshot, error) {
	id, err := model.ParseSnapshotID(in.GetId())
	if err != nil {
		return nil, toStatus(err)
	}
	snap, err := s.svc.Snapshot(ctx, id)
	if err != nil {
		return nil, toStatus(err)
	}
	return toSnapshot(snap), nil
}

// ListSnapshots implements BlockService.
func (s *Server) ListSnapshots(ctx context.Context, in *storagev1.ListSnapshotsRequest) (*storagev1.ListSnapshotsResponse, error) {
	page, err := s.svc.ListSnapshots(ctx, pageReq(in.GetPageSize(), in.GetPageToken()))
	if err != nil {
		return nil, toStatus(err)
	}
	out := &storagev1.ListSnapshotsResponse{NextPageToken: page.NextToken}
	for _, snap := range page.Items {
		out.Snapshots = append(out.Snapshots, toSnapshot(snap))
	}
	return out, nil
}

// CreateBucket implements ObjectService.
func (s *Server) CreateBucket(ctx context.Context, in *storagev1.CreateBucketRequest) (*storagev1.Operation, error) {
	tok, err := model.ParseClientToken(in.GetClientToken())
	if err != nil {
		return nil, toStatus(err)
	}
	name, err := model.ParseBucketName(in.GetName())
	if err != nil {
		return nil, toStatus(err)
	}
	op, err := s.svc.Submit(ctx, tok, model.CreateBucketRequest{Bucket: name})
	if err != nil {
		return nil, toStatus(err)
	}
	return toOp(op), nil
}

// DeleteBucket implements ObjectService.
func (s *Server) DeleteBucket(ctx context.Context, in *storagev1.DeleteBucketRequest) (*storagev1.Operation, error) {
	tok, name, err := tokenAnd(in.GetClientToken(), in.GetName(), model.ParseBucketName)
	if err != nil {
		return nil, toStatus(err)
	}
	op, err := s.svc.Submit(ctx, tok, model.DeleteBucketRequest{Bucket: name})
	if err != nil {
		return nil, toStatus(err)
	}
	return toOp(op), nil
}

// HeadBucket implements ObjectService.
func (s *Server) HeadBucket(ctx context.Context, in *storagev1.HeadBucketRequest) (*storagev1.Bucket, error) {
	name, err := model.ParseBucketName(in.GetName())
	if err != nil {
		return nil, toStatus(err)
	}
	b, err := s.svc.Bucket(ctx, name)
	if err != nil {
		return nil, toStatus(err)
	}
	return toBucket(b), nil
}

// ListBuckets implements ObjectService.
func (s *Server) ListBuckets(ctx context.Context, in *storagev1.ListBucketsRequest) (*storagev1.ListBucketsResponse, error) {
	page, err := s.svc.ListBuckets(ctx, pageReq(in.GetPageSize(), in.GetPageToken()))
	if err != nil {
		return nil, toStatus(err)
	}
	out := &storagev1.ListBucketsResponse{NextPageToken: page.NextToken}
	for _, b := range page.Items {
		out.Buckets = append(out.Buckets, toBucket(b))
	}
	return out, nil
}

// PutObject implements ObjectService.
func (s *Server) PutObject(ctx context.Context, in *storagev1.PutObjectRequest) (*storagev1.Operation, error) {
	tok, err := model.ParseClientToken(in.GetClientToken())
	if err != nil {
		return nil, toStatus(err)
	}
	path, err := objectPath(in.GetBucket(), in.GetKey())
	if err != nil {
		return nil, toStatus(err)
	}
	var sum model.Digest
	if in.GetSha256Hex() != "" {
		sum, err = model.ParseDigest(in.GetSha256Hex())
		if err != nil {
			return nil, toStatus(err)
		}
	}
	op, err := s.svc.Submit(ctx, tok, model.PutObjectRequest{
		Path: path, Size: model.Bytes(in.GetSizeBytes()), SHA256: sum, ContentType: in.GetContentType(),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return toOp(op), nil
}

// DeleteObject implements ObjectService.
func (s *Server) DeleteObject(ctx context.Context, in *storagev1.DeleteObjectRequest) (*storagev1.Operation, error) {
	tok, err := model.ParseClientToken(in.GetClientToken())
	if err != nil {
		return nil, toStatus(err)
	}
	path, err := objectPath(in.GetBucket(), in.GetKey())
	if err != nil {
		return nil, toStatus(err)
	}
	op, err := s.svc.Submit(ctx, tok, model.DeleteObjectRequest{Path: path})
	if err != nil {
		return nil, toStatus(err)
	}
	return toOp(op), nil
}

// HeadObject implements ObjectService.
func (s *Server) HeadObject(ctx context.Context, in *storagev1.HeadObjectRequest) (*storagev1.Object, error) {
	path, err := objectPath(in.GetBucket(), in.GetKey())
	if err != nil {
		return nil, toStatus(err)
	}
	obj, err := s.svc.Object(ctx, path)
	if err != nil {
		return nil, toStatus(err)
	}
	return toObject(obj), nil
}

// GetObject implements ObjectService.
func (s *Server) GetObject(ctx context.Context, in *storagev1.GetObjectRequest) (*storagev1.GetObjectResponse, error) {
	path, err := objectPath(in.GetBucket(), in.GetKey())
	if err != nil {
		return nil, toStatus(err)
	}
	obj, loc, err := s.svc.LocateObject(ctx, path)
	if err != nil {
		return nil, toStatus(err)
	}
	return &storagev1.GetObjectResponse{Object: toObject(obj), Download: toLoc(loc)}, nil
}

// ListObjects implements ObjectService.
func (s *Server) ListObjects(ctx context.Context, in *storagev1.ListObjectsRequest) (*storagev1.ListObjectsResponse, error) {
	name, err := model.ParseBucketName(in.GetBucket())
	if err != nil {
		return nil, toStatus(err)
	}
	page, err := s.svc.ListObjects(ctx, name, in.GetPrefix(), pageReq(in.GetPageSize(), in.GetPageToken()))
	if err != nil {
		return nil, toStatus(err)
	}
	out := &storagev1.ListObjectsResponse{NextPageToken: page.NextToken}
	for _, obj := range page.Items {
		out.Objects = append(out.Objects, toObject(obj))
	}
	return out, nil
}

// GetOperation implements OperationService.
func (s *Server) GetOperation(ctx context.Context, in *storagev1.GetOperationRequest) (*storagev1.Operation, error) {
	id, err := model.ParseOperationID(in.GetId())
	if err != nil {
		return nil, toStatus(err)
	}
	op, err := s.svc.Operation(ctx, id)
	if err != nil {
		return nil, toStatus(err)
	}
	return toOp(op), nil
}

// ListOperations implements OperationService.
func (s *Server) ListOperations(ctx context.Context, in *storagev1.ListOperationsRequest) (*storagev1.ListOperationsResponse, error) {
	page, err := s.svc.ListOperations(ctx, storage.OperationQuery{NonTerminal: in.GetNonTerminal()}, pageReq(in.GetPageSize(), in.GetPageToken()))
	if err != nil {
		return nil, toStatus(err)
	}
	out := &storagev1.ListOperationsResponse{NextPageToken: page.NextToken}
	for _, op := range page.Items {
		out.Operations = append(out.Operations, toOp(op))
	}
	return out, nil
}

func tokenAnd[T any](token, raw string, parse func(string) (T, error)) (model.ClientToken, T, error) {
	var zero T
	tok, err := model.ParseClientToken(token)
	if err != nil {
		return model.ClientToken{}, zero, err
	}
	id, err := parse(raw)
	if err != nil {
		return model.ClientToken{}, zero, err
	}
	return tok, id, nil
}

func objectPath(bucket, key string) (model.ObjectPath, error) {
	b, err := model.ParseBucketName(bucket)
	if err != nil {
		return model.ObjectPath{}, err
	}
	k, err := model.ParseObjectKey(key)
	if err != nil {
		return model.ObjectPath{}, err
	}
	return model.ObjectPath{Bucket: b, Key: k}, nil
}

func pageReq(size int32, token string) model.PageRequest {
	return model.PageRequest{Size: int(size), Token: token}
}

func toOp(st storage.OperationStatus) *storagev1.Operation {
	op := st.Operation
	out := &storagev1.Operation{
		Id: op.ID.ID(), Kind: op.Kind().String(), Phase: op.Phase().String(), Done: op.Terminal(),
	}
	if op.Target() != nil {
		out.Target = targetName(op.Target())
	}
	if !op.Err().IsZero() {
		out.Error = &storagev1.Status{Code: int32(op.Err().Code), Message: op.Err().Message}
	}
	if st.Upload != nil {
		out.Upload = toLoc(*st.Upload)
	}
	return out
}

func targetName(ref model.Ref) string {
	obj, ok := ref.(model.ObjectRef)
	if !ok {
		return ref.String()
	}
	return obj.Path.Bucket.Name() + "/" + url.PathEscape(obj.Path.Key.String())
}

func toVolume(v model.Volume) *storagev1.Volume {
	out := &storagev1.Volume{
		Id: v.ID.ID(), SizeBytes: int64(v.Spec.Size), Zone: v.Spec.Zone.String(),
		Type: v.Spec.Type.String(), State: lifeString(v.State),
	}
	if id, ok := v.Spec.Source.Snapshot(); ok {
		out.SourceSnapshotId = id.ID()
	}
	return out
}

func toSnapshot(s model.Snapshot) *storagev1.Snapshot {
	return &storagev1.Snapshot{
		Id: s.ID.ID(), SourceVolumeId: s.Source.ID(), SizeBytes: int64(s.Size), State: lifeString(s.State),
	}
}

func toBucket(b model.Bucket) *storagev1.Bucket {
	return &storagev1.Bucket{Name: b.Name.Name(), State: lifeString(b.State)}
}

func toObject(o model.Object) *storagev1.Object {
	return &storagev1.Object{
		Bucket: o.Path.Bucket.Name(), Key: o.Path.Key.String(), SizeBytes: int64(o.Size),
		Etag: string(o.ETag), Generation: o.Generation.String(), ContentType: o.ContentType,
	}
}

func toLoc(l model.Locator) *storagev1.Locator {
	return &storagev1.Locator{
		Method: l.Method, Url: l.URL, Headers: l.Headers, ExpiresUnixMicro: l.ExpiresAt.UnixMicro(),
	}
}

func lifeString(s model.Lifecycle) string {
	switch s.(type) {
	case model.Creating:
		return "creating"
	case model.Available:
		return "available"
	case model.Deleting:
		return "deleting"
	default:
		return ""
	}
}

func toStatus(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, model.ErrTokenReused):
		st := status.New(codes.AlreadyExists, err.Error())
		with, derr := st.WithDetails(&errdetails.ErrorInfo{Reason: "CLIENT_TOKEN_REUSED", Domain: "vws.storage"})
		if derr != nil {
			return st.Err()
		}
		return with.Err()
	case errors.Is(err, model.ErrInvalidArgument):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, model.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, model.ErrAlreadyExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, model.ErrFailedPrecondition):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, model.ErrStale):
		return status.Error(codes.Aborted, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
