package server

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/popododo0720/vws-proto/gen/go/vws/common/v1"
	storagev1 "github.com/popododo0720/vws-proto/gen/go/vws/storage/v1"

	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/storage"
)

const errorDomain = "vws.dev"

// maxInlineObject is the unary CreateObject payload limit from the proto.
const maxInlineObject = 4194304

var uuid4Pattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

type statusError struct {
	code   codes.Code
	reason string
	err    error
}

func (e statusError) Error() string { return e.err.Error() }
func (e statusError) Unwrap() error { return e.err }

func invalidID(msg string) error {
	return statusError{code: codes.InvalidArgument, reason: "INVALID_RESOURCE_ID", err: fmt.Errorf("%w: %s", model.ErrInvalidArgument, msg)}
}

func invalidSpec(msg string) error {
	return statusError{code: codes.InvalidArgument, reason: "INVALID_SPEC", err: fmt.Errorf("%w: %s", model.ErrInvalidArgument, msg)}
}

func notFound(msg string) error {
	return statusError{code: codes.NotFound, reason: "RESOURCE_NOT_FOUND", err: fmt.Errorf("%w: %s", model.ErrNotFound, msg)}
}

func (s *Server) volumeName(zone model.Zone, id model.VolumeID) string {
	return "projects/" + s.project + "/zones/" + zone.String() + "/volumes/" + id.ID()
}

func (s *Server) snapshotName(id model.SnapshotID) string {
	return "projects/" + s.project + "/regions/" + s.region + "/snapshots/" + id.ID()
}

func (s *Server) bucketName(name model.BucketName) string {
	return "projects/" + s.project + "/buckets/" + name.Name()
}

func (s *Server) objectName(p model.ObjectPath) string {
	return "projects/" + s.project + "/buckets/" + p.Bucket.Name() + "/objects/" + encodeKey(p.Key.String())
}

// encodeKey percent-encodes every byte that is not RFC 3986 unreserved.
// "/" becomes "%2F", so the key stays one path segment.
func encodeKey(key string) string {
	var b strings.Builder
	for i := 0; i < len(key); i++ {
		c := key[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '.' || c == '_' || c == '~' {
			b.WriteByte(c)
			continue
		}
		fmt.Fprintf(&b, "%%%02X", c)
	}
	return b.String()
}

func (s *Server) opName(id model.OperationID) string {
	return "projects/" + s.project + "/operations/" + id.ID()
}

func (s *Server) parseVolumeName(name string) (model.VolumeID, model.Zone, error) {
	parts, err := nameParts(name, 6)
	if err != nil || parts[2] != "zones" || parts[4] != "volumes" {
		return model.VolumeID{}, model.Zone{}, invalidID("volume name")
	}
	if parts[1] != s.project {
		return model.VolumeID{}, model.Zone{}, notFound("volume")
	}
	zone, err := model.ParseZone(parts[3])
	if err != nil {
		return model.VolumeID{}, model.Zone{}, invalidID("zone")
	}
	id, err := model.ParseVolumeID(parts[5])
	if err != nil {
		return model.VolumeID{}, model.Zone{}, invalidID("volume id")
	}
	return id, zone, nil
}

func (s *Server) parseVolumeParent(parent string) (model.Zone, error) {
	parts, err := nameParts(parent, 4)
	if err != nil || parts[2] != "zones" {
		return model.Zone{}, invalidID("volume parent")
	}
	if parts[1] != s.project {
		return model.Zone{}, notFound("volume")
	}
	zone, err := model.ParseZone(parts[3])
	if err != nil {
		return model.Zone{}, invalidID("zone")
	}
	return zone, nil
}

func (s *Server) parseSnapshotName(name string) (model.SnapshotID, error) {
	parts, err := nameParts(name, 6)
	if err != nil || parts[2] != "regions" || parts[4] != "snapshots" {
		return model.SnapshotID{}, invalidID("snapshot name")
	}
	if parts[1] != s.project || parts[3] != s.region {
		return model.SnapshotID{}, notFound("snapshot")
	}
	id, err := model.ParseSnapshotID(parts[5])
	if err != nil {
		return model.SnapshotID{}, invalidID("snapshot id")
	}
	return id, nil
}

func (s *Server) parseSnapshotParent(parent string) error {
	parts, err := nameParts(parent, 4)
	if err != nil || parts[2] != "regions" {
		return invalidID("snapshot parent")
	}
	if parts[1] != s.project || parts[3] != s.region {
		return notFound("snapshot")
	}
	return nil
}

func (s *Server) parseBucketName(name string) (model.BucketName, error) {
	parts, err := nameParts(name, 4)
	if err != nil || parts[2] != "buckets" {
		return model.BucketName{}, invalidID("bucket name")
	}
	if parts[1] != s.project {
		return model.BucketName{}, notFound("bucket")
	}
	id, err := model.ParseBucketName(parts[3])
	if err != nil {
		return model.BucketName{}, invalidID("bucket id")
	}
	return id, nil
}

func (s *Server) parseBucketParent(parent string) error {
	parts, err := nameParts(parent, 2)
	if err != nil {
		return invalidID("bucket parent")
	}
	if parts[1] != s.project {
		return notFound("bucket")
	}
	return nil
}

func (s *Server) parseObjectParent(parent string) (model.BucketName, error) {
	parts, err := nameParts(parent, 4)
	if err != nil || parts[2] != "buckets" {
		return model.BucketName{}, invalidID("object parent")
	}
	if parts[1] != s.project {
		return model.BucketName{}, notFound("object")
	}
	return model.ParseBucketName(parts[3])
}

func (s *Server) parseObjectName(name string) (model.ObjectPath, error) {
	const mark = "/objects/"
	i := strings.Index(name, mark)
	if i < 0 {
		return model.ObjectPath{}, invalidID("object name")
	}
	bucket, err := s.parseObjectParent(name[:i])
	if err != nil {
		return model.ObjectPath{}, err
	}
	raw := name[i+len(mark):]
	if raw == "" || strings.Contains(raw, "/") {
		return model.ObjectPath{}, invalidID("object id")
	}
	key, err := url.PathUnescape(raw)
	if err != nil {
		return model.ObjectPath{}, invalidID("object id")
	}
	if key == "." || key == ".." || strings.HasPrefix(key, "/") || strings.HasSuffix(key, "/") || strings.Contains(key, "//") {
		return model.ObjectPath{}, invalidID("object id")
	}
	parsed, err := model.ParseObjectKey(key)
	if err != nil {
		return model.ObjectPath{}, invalidID("object id")
	}
	return model.ObjectPath{Bucket: bucket, Key: parsed}, nil
}

func (s *Server) parseOpName(name string) (model.OperationID, error) {
	parts, err := nameParts(name, 4)
	if err != nil || parts[2] != "operations" {
		return model.OperationID{}, invalidID("operation name")
	}
	if parts[1] != s.project {
		return model.OperationID{}, notFound("operation")
	}
	id, err := model.ParseOperationID(parts[3])
	if err != nil {
		return model.OperationID{}, invalidID("operation name")
	}
	return id, nil
}

func nameParts(name string, n int) ([]string, error) {
	parts := strings.Split(name, "/")
	if len(parts) != n || parts[0] != "projects" {
		return nil, invalidID("name")
	}
	for _, p := range parts {
		if p == "" {
			return nil, invalidID("name")
		}
	}
	return parts, nil
}

func (s *Server) volumeCreate(in *storagev1.CreateVolumeRequest) (model.CreateVolumeRequest, string, error) {
	zone, err := s.parseVolumeParent(in.GetParent())
	if err != nil {
		return model.CreateVolumeRequest{}, "", err
	}
	id, err := model.ParseVolumeID(in.GetVolumeId())
	if err != nil {
		return model.CreateVolumeRequest{}, "", invalidID("volume id")
	}
	specIn := in.GetVolume().GetSpec()
	if in.GetVolume() == nil || specIn == nil {
		return model.CreateVolumeRequest{}, "", invalidSpec("volume")
	}
	if err := rejectLabels(in.GetVolume().GetLabels()); err != nil {
		return model.CreateVolumeRequest{}, "", err
	}
	if specIn.GetImage() != "" {
		return model.CreateVolumeRequest{}, "", invalidSpec("image")
	}
	typ, err := volumeType(specIn.GetType())
	if err != nil {
		return model.CreateVolumeRequest{}, "", err
	}
	src := model.NoSource()
	if snapName := specIn.GetSnapshot(); snapName != "" {
		snap, err := s.parseSnapshotName(snapName)
		if err != nil {
			return model.CreateVolumeRequest{}, "", err
		}
		src, err = model.FromSnapshot(snap)
		if err != nil {
			return model.CreateVolumeRequest{}, "", invalidSpec("snapshot")
		}
	}
	size, err := model.VolumeSize(specIn.GetSizeGib())
	if err != nil {
		return model.CreateVolumeRequest{}, "", invalidSpec("size_gib")
	}
	name := s.volumeName(zone, id)
	if got := in.GetVolume().GetName(); got != "" && got != name {
		return model.CreateVolumeRequest{}, "", invalidID("volume name")
	}
	return model.CreateVolumeRequest{ID: id, Spec: model.VolumeSpec{
		Size: size, Zone: zone, Type: typ, Source: src,
	}}, name, nil
}

func (s *Server) snapshotCreate(in *storagev1.CreateSnapshotRequest) (model.CreateSnapshotRequest, string, error) {
	if err := s.parseSnapshotParent(in.GetParent()); err != nil {
		return model.CreateSnapshotRequest{}, "", err
	}
	id, err := model.ParseSnapshotID(in.GetSnapshotId())
	if err != nil {
		return model.CreateSnapshotRequest{}, "", invalidID("snapshot id")
	}
	snap := in.GetSnapshot()
	if snap == nil || snap.GetSpec() == nil {
		return model.CreateSnapshotRequest{}, "", invalidSpec("snapshot")
	}
	if err := rejectLabels(snap.GetLabels()); err != nil {
		return model.CreateSnapshotRequest{}, "", err
	}
	if snap.GetSpec().GetDescription() != "" {
		return model.CreateSnapshotRequest{}, "", invalidSpec("description")
	}
	vol, zone, err := s.parseVolumeName(snap.GetSpec().GetVolume())
	if err != nil {
		return model.CreateSnapshotRequest{}, "", err
	}
	name := s.snapshotName(id)
	if got := snap.GetName(); got != "" && got != name {
		return model.CreateSnapshotRequest{}, "", invalidID("snapshot name")
	}
	return model.CreateSnapshotRequest{ID: id, Source: vol, Zone: zone}, name, nil
}

func (s *Server) bucketCreate(in *storagev1.CreateBucketRequest) (model.CreateBucketRequest, string, error) {
	if err := s.parseBucketParent(in.GetParent()); err != nil {
		return model.CreateBucketRequest{}, "", err
	}
	id, err := model.ParseBucketName(in.GetBucketId())
	if err != nil {
		return model.CreateBucketRequest{}, "", invalidID("bucket id")
	}
	b := in.GetBucket()
	if b == nil || b.GetSpec() == nil {
		return model.CreateBucketRequest{}, "", invalidSpec("bucket")
	}
	if err := rejectLabels(b.GetLabels()); err != nil {
		return model.CreateBucketRequest{}, "", err
	}
	if b.GetSpec().GetVersioning() {
		return model.CreateBucketRequest{}, "", invalidSpec("versioning")
	}
	if b.GetSpec().GetRegion() != s.region {
		return model.CreateBucketRequest{}, "", invalidSpec("region")
	}
	name := s.bucketName(id)
	if got := b.GetName(); got != "" && got != name {
		return model.CreateBucketRequest{}, "", invalidID("bucket name")
	}
	return model.CreateBucketRequest{Bucket: id}, name, nil
}

func volumeType(t storagev1.VolumeType) (model.VolumeType, error) {
	switch t {
	case storagev1.VolumeType_VOLUME_TYPE_STANDARD:
		return model.VolumeTypeStandard, nil
	case storagev1.VolumeType_VOLUME_TYPE_SSD:
		return model.VolumeTypeSSD, nil
	default:
		return 0, invalidSpec("volume type")
	}
}

func rejectLabels(labels map[string]string) error {
	if len(labels) > 0 {
		return invalidSpec("labels")
	}
	return nil
}

func tokenFor(requestID string) (model.ClientToken, error) {
	if requestID == "" {
		return model.NewClientToken(), nil
	}
	if !uuid4Pattern.MatchString(requestID) {
		return model.ClientToken{}, invalidSpec("request_id")
	}
	return model.ParseClientToken(requestID)
}

func expectedVersion(etag string) (model.Version, error) {
	if etag == "" {
		return 0, nil
	}
	n, err := strconv.ParseUint(etag, 10, 64)
	if err != nil || n == 0 {
		return 0, invalidSpec("etag")
	}
	return model.Version(n), nil
}

func pageSize(n int32) (int, error) {
	switch {
	case n < 0:
		return 0, invalidSpec("page_size")
	case n > 1000:
		return 1000, nil
	default:
		return int(n), nil
	}
}

func etagOf(m model.Meta) string { return strconv.FormatUint(uint64(m.Version), 10) }

func (s *Server) operation(ctx context.Context, st storage.OperationStatus) (*longrunningpb.Operation, error) {
	op := st.Operation
	md := &commonv1.OperationMetadata{
		Resource:   s.resourceName(ctx, op),
		Verb:       verbOf(op.Kind()),
		Generation: 1,
		CreateTime: stamp(op.Meta.CreateTime),
	}
	if tok := op.Claim.Token.String(); tok != "" && !strings.HasPrefix(tok, "auto-") {
		md.RequestId = tok
	}
	if op.Terminal() {
		md.EndTime = stamp(op.Meta.UpdateTime)
	}
	meta, err := anypb.New(md)
	if err != nil {
		return nil, err
	}
	out := &longrunningpb.Operation{Name: s.opName(op.ID), Metadata: meta, Done: op.Terminal()}
	if !op.Terminal() {
		return out, nil
	}
	if !op.Err().IsZero() {
		out.Result = &longrunningpb.Operation_Error{Error: failedStatus(op.Err())}
		return out, nil
	}
	msg, err := s.successMessage(ctx, op)
	if err != nil {
		return nil, toStatus(err)
	}
	if msg != nil {
		body, err := anypb.New(msg)
		if err != nil {
			return nil, err
		}
		out.Result = &longrunningpb.Operation_Response{Response: body}
	}
	return out, nil
}

func (s *Server) resourceName(ctx context.Context, op model.Operation) string {
	switch w := op.Work.(type) {
	case model.VolumeCreation:
		return s.volumeName(w.Spec.Zone, w.Volume)
	case model.VolumeDeletion:
		zone := w.Zone
		if zone.IsZero() {
			if vol, err := s.svc.Volume(ctx, w.Volume); err == nil {
				zone = vol.Spec.Zone
			}
		}
		return s.volumeName(zone, w.Volume)
	case model.SnapshotCreation:
		return s.snapshotName(w.Snapshot)
	case model.SnapshotDeletion:
		return s.snapshotName(w.Snapshot)
	case model.BucketCreation:
		return s.bucketName(w.Bucket)
	case model.BucketDeletion:
		return s.bucketName(w.Bucket)
	case model.ObjectUpload:
		return s.objectName(w.Object.Path)
	case model.ObjectPurge:
		return s.objectName(w.Object.Path)
	default:
		return ""
	}
}

func (s *Server) successMessage(ctx context.Context, op model.Operation) (proto.Message, error) {
	switch w := op.Work.(type) {
	case model.VolumeCreation:
		vol, err := s.svc.Volume(ctx, w.Volume)
		if errors.Is(err, model.ErrNotFound) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		return s.volumeMsg(vol), nil
	case model.SnapshotCreation:
		snap, err := s.svc.Snapshot(ctx, w.Snapshot)
		if errors.Is(err, model.ErrNotFound) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		return s.snapshotMsg(ctx, snap), nil
	case model.BucketCreation:
		b, err := s.svc.Bucket(ctx, w.Bucket)
		if errors.Is(err, model.ErrNotFound) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		return s.bucketMsg(b), nil
	case model.ObjectUpload:
		obj, err := s.svc.Object(ctx, w.Object.Path)
		if errors.Is(err, model.ErrNotFound) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		return s.objectMsg(obj), nil
	default:
		return nil, nil
	}
}

func (s *Server) volumeMsg(v model.Volume) *storagev1.Volume {
	out := &storagev1.Volume{
		Name: s.volumeName(v.Spec.Zone, v.ID), Generation: int64(v.Meta.Version), Etag: etagOf(v.Meta),
		Spec: &storagev1.VolumeSpec{
			SizeGib: int64(v.Spec.Size / model.GiB), Type: protoVolumeType(v.Spec.Type),
		},
		Status:     &storagev1.VolumeStatus{Observed: observed(v.State, v.Meta.Version), SizeGib: int64(v.Spec.Size / model.GiB)},
		CreateTime: stamp(v.Meta.CreateTime), UpdateTime: stamp(v.Meta.UpdateTime),
	}
	if id, ok := v.Spec.Source.Snapshot(); ok {
		out.Spec.Source = &storagev1.VolumeSpec_Snapshot{Snapshot: s.snapshotName(id)}
	}
	if _, ok := v.State.(model.Deleting); ok {
		out.DeleteTime = stamp(v.Meta.UpdateTime)
	}
	return out
}

func (s *Server) snapshotMsg(ctx context.Context, sn model.Snapshot) *storagev1.Snapshot {
	volume := sn.Source.ID()
	if vol, err := s.svc.Volume(ctx, sn.Source); err == nil {
		volume = s.volumeName(vol.Spec.Zone, sn.Source)
	}
	out := &storagev1.Snapshot{
		Name: s.snapshotName(sn.ID), Generation: int64(sn.Meta.Version), Etag: etagOf(sn.Meta),
		Spec:       &storagev1.SnapshotSpec{Volume: volume},
		Status:     &storagev1.SnapshotStatus{Observed: observed(sn.State, sn.Meta.Version), SizeGib: int64(sn.Size / model.GiB)},
		CreateTime: stamp(sn.Meta.CreateTime), UpdateTime: stamp(sn.Meta.UpdateTime),
	}
	if _, ok := sn.State.(model.Deleting); ok {
		out.DeleteTime = stamp(sn.Meta.UpdateTime)
	}
	return out
}

func (s *Server) bucketMsg(b model.Bucket) *storagev1.Bucket {
	out := &storagev1.Bucket{
		Name: s.bucketName(b.Name), Generation: int64(b.Meta.Version), Etag: etagOf(b.Meta),
		Spec:       &storagev1.BucketSpec{Region: s.region},
		Status:     &storagev1.BucketStatus{Observed: observed(b.State, b.Meta.Version)},
		CreateTime: stamp(b.Meta.CreateTime), UpdateTime: stamp(b.Meta.UpdateTime),
	}
	if _, ok := b.State.(model.Deleting); ok {
		out.DeleteTime = stamp(b.Meta.UpdateTime)
	}
	return out
}

func (s *Server) objectMsg(o model.Object) *storagev1.Object {
	return &storagev1.Object{
		Name: s.objectName(o.Path), Generation: int64(o.Meta.Version), Etag: etagOf(o.Meta),
		Spec: &storagev1.ObjectSpec{ContentType: o.ContentType, Sha256: o.SHA256.String()},
		Status: &storagev1.ObjectStatus{
			Observed: observed(model.Available{}, o.Meta.Version), SizeBytes: int64(o.Size),
		},
		CreateTime: stamp(o.Meta.CreateTime), UpdateTime: stamp(o.Meta.UpdateTime),
	}
}

func protoVolumeType(t model.VolumeType) storagev1.VolumeType {
	switch t {
	case model.VolumeTypeStandard:
		return storagev1.VolumeType_VOLUME_TYPE_STANDARD
	case model.VolumeTypeSSD:
		return storagev1.VolumeType_VOLUME_TYPE_SSD
	default:
		return storagev1.VolumeType_VOLUME_TYPE_UNSPECIFIED
	}
}

func observed(state model.Lifecycle, version model.Version) *commonv1.ObservedState {
	out := &commonv1.ObservedState{}
	switch state.(type) {
	case model.Creating:
		out.State = commonv1.ReconciliationState_RECONCILIATION_STATE_RECONCILING
		out.Generation = int64(version) - 1
	case model.Available:
		out.State = commonv1.ReconciliationState_RECONCILIATION_STATE_READY
		out.Generation = int64(version)
	case model.Deleting:
		out.State = commonv1.ReconciliationState_RECONCILIATION_STATE_DELETING
		out.Generation = int64(version) - 1
	default:
		out.State = commonv1.ReconciliationState_RECONCILIATION_STATE_UNSPECIFIED
	}
	if out.Generation < 0 {
		out.Generation = 0
	}
	return out
}

func verbOf(k model.Kind) commonv1.OperationVerb {
	switch k {
	case model.KindCreateVolume, model.KindCreateSnapshot, model.KindCreateBucket, model.KindPutObject:
		return commonv1.OperationVerb_OPERATION_VERB_CREATE
	case model.KindDeleteVolume, model.KindDeleteSnapshot, model.KindDeleteBucket, model.KindDeleteObject:
		return commonv1.OperationVerb_OPERATION_VERB_DELETE
	default:
		return commonv1.OperationVerb_OPERATION_VERB_UNSPECIFIED
	}
}

func stamp(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

func doneOnly(name string, verb commonv1.OperationVerb) (*longrunningpb.Operation, error) {
	md, err := anypb.New(&commonv1.OperationMetadata{Resource: name, Verb: verb, CreateTime: timestamppb.Now(), EndTime: timestamppb.Now()})
	if err != nil {
		return nil, err
	}
	return &longrunningpb.Operation{Metadata: md, Done: true}, nil
}

func failedStatus(st model.Status) *spb.Status {
	code := grpcCode(st.Code)
	out := &spb.Status{Code: int32(code), Message: st.Message}
	info, err := anypb.New(&errdetails.ErrorInfo{Reason: "RECONCILIATION_FAILED", Domain: errorDomain})
	if err == nil {
		out.Details = []*anypb.Any{info}
	}
	return out
}

func grpcCode(c model.Code) codes.Code {
	switch c {
	case model.CodeInvalidArgument:
		return codes.InvalidArgument
	case model.CodeNotFound:
		return codes.NotFound
	case model.CodeFailedPrecondition:
		return codes.FailedPrecondition
	case model.CodeDeadlineExceeded:
		return codes.DeadlineExceeded
	case model.CodeUnavailable:
		return codes.Unavailable
	default:
		return codes.Internal
	}
}

func toStatus(err error) error {
	if err == nil {
		return nil
	}
	var wire statusError
	if errors.As(err, &wire) {
		return wire.grpc()
	}
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, model.ErrTokenReused):
		return statusError{code: codes.Aborted, reason: "IDEMPOTENCY_CONFLICT", err: err}.grpc()
	case errors.Is(err, model.ErrInvalidArgument):
		return statusError{code: codes.InvalidArgument, reason: "INVALID_SPEC", err: err}.grpc()
	case errors.Is(err, model.ErrNotFound):
		return statusError{code: codes.NotFound, reason: "RESOURCE_NOT_FOUND", err: err}.grpc()
	case errors.Is(err, model.ErrAlreadyExists):
		return statusError{code: codes.AlreadyExists, reason: "ALREADY_EXISTS", err: err}.grpc()
	case errors.Is(err, model.ErrETagMismatch):
		return statusError{code: codes.Aborted, reason: "ETAG_MISMATCH", err: err}.grpc()
	case errors.Is(err, model.ErrFailedPrecondition):
		reason := "DEPENDENCY_NOT_READY"
		if strings.Contains(err.Error(), "not empty") || strings.Contains(err.Error(), "sources") {
			reason = "DEPENDENCY_MISSING"
		}
		return statusError{code: codes.FailedPrecondition, reason: reason, err: err}.grpc()
	case errors.Is(err, model.ErrStale):
		return statusError{code: codes.Aborted, reason: "GENERATION_CONFLICT", err: err}.grpc()
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

func (e statusError) grpc() error {
	st := status.New(e.code, e.err.Error())
	if e.reason == "" {
		return st.Err()
	}
	with, err := st.WithDetails(&errdetails.ErrorInfo{Reason: e.reason, Domain: errorDomain})
	if err != nil {
		return st.Err()
	}
	return with.Err()
}
