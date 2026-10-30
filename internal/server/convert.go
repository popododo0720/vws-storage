package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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

	"github.com/popododo0720/vws-lib/obs"

	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/storage"
)

const errorDomain = "vws.dev"

// maxInlineObject is the unary CreateObject payload limit from the proto.
const maxInlineObject = 4194304

// readAttempts bounds ReadObject retries when the head moves under a read.
const readAttempts = 3

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
	return "projects/" + id.Project().String() + "/zones/" + zone.String() + "/volumes/" + id.ID()
}

func (s *Server) snapshotName(id model.SnapshotID, region model.Region) string {
	return "projects/" + id.Project().String() + "/regions/" + region.String() + "/snapshots/" + id.ID()
}

func (s *Server) bucketName(name model.BucketName) string {
	return "projects/" + name.Project().String() + "/buckets/" + name.Name()
}

func (s *Server) objectName(p model.ObjectPath) string {
	return "projects/" + p.Project().String() + "/buckets/" + p.Bucket.Name() + "/objects/" + encodeKey(p.Key.String())
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
	return "projects/" + id.Project().String() + "/operations/" + id.ID()
}

func (s *Server) allowProject(p model.Project) error {
	if s.projects == nil {
		return nil
	}
	if _, ok := s.projects[p]; ok {
		return nil
	}
	return notFound("project " + p.String() + " is not served")
}

func (s *Server) parseProject(seg string) (model.Project, error) {
	p, err := model.ParseProject(seg)
	if err != nil {
		return model.Project{}, invalidID("project")
	}
	if err := s.allowProject(p); err != nil {
		return model.Project{}, err
	}
	return p, nil
}

func (s *Server) parseVolumeName(name string) (model.VolumeID, model.Zone, error) {
	parts, err := nameParts(name, 6)
	if err != nil || parts[2] != "zones" || parts[4] != "volumes" {
		return model.VolumeID{}, model.Zone{}, invalidID("volume name")
	}
	project, err := s.parseProject(parts[1])
	if err != nil {
		return model.VolumeID{}, model.Zone{}, err
	}
	zone, err := model.ParseZone(parts[3])
	if err != nil {
		return model.VolumeID{}, model.Zone{}, invalidID("zone")
	}
	id, err := model.ParseVolumeID(parts[5])
	if err != nil {
		return model.VolumeID{}, model.Zone{}, invalidID("volume id")
	}
	return id.WithProject(project), zone, nil
}

func (s *Server) parseVolumeParent(parent string) (model.Project, model.Zone, error) {
	parts, err := nameParts(parent, 4)
	if err != nil || parts[2] != "zones" {
		return model.Project{}, model.Zone{}, invalidID("volume parent")
	}
	project, err := s.parseProject(parts[1])
	if err != nil {
		return model.Project{}, model.Zone{}, err
	}
	zone, err := model.ParseZone(parts[3])
	if err != nil {
		return model.Project{}, model.Zone{}, invalidID("zone")
	}
	return project, zone, nil
}

func (s *Server) parseSnapshotName(name string) (model.SnapshotID, model.Region, error) {
	parts, err := nameParts(name, 6)
	if err != nil || parts[2] != "regions" || parts[4] != "snapshots" {
		return model.SnapshotID{}, model.Region{}, invalidID("snapshot name")
	}
	project, err := s.parseProject(parts[1])
	if err != nil {
		return model.SnapshotID{}, model.Region{}, err
	}
	region, err := model.ParseRegion(parts[3])
	if err != nil {
		return model.SnapshotID{}, model.Region{}, invalidID("region")
	}
	id, err := model.ParseSnapshotID(parts[5])
	if err != nil {
		return model.SnapshotID{}, model.Region{}, invalidID("snapshot id")
	}
	return id.WithProject(project), region, nil
}

func (s *Server) parseSnapshotParent(parent string) (model.Project, model.Region, error) {
	parts, err := nameParts(parent, 4)
	if err != nil || parts[2] != "regions" {
		return model.Project{}, model.Region{}, invalidID("snapshot parent")
	}
	project, err := s.parseProject(parts[1])
	if err != nil {
		return model.Project{}, model.Region{}, err
	}
	region, err := model.ParseRegion(parts[3])
	if err != nil {
		return model.Project{}, model.Region{}, invalidID("region")
	}
	return project, region, nil
}

func (s *Server) parseBucketName(name string) (model.BucketName, error) {
	parts, err := nameParts(name, 4)
	if err != nil || parts[2] != "buckets" {
		return model.BucketName{}, invalidID("bucket name")
	}
	project, err := s.parseProject(parts[1])
	if err != nil {
		return model.BucketName{}, err
	}
	id, err := model.ParseBucketName(parts[3])
	if err != nil {
		return model.BucketName{}, invalidID("bucket id")
	}
	return id.WithProject(project), nil
}

func (s *Server) parseBucketParent(parent string) (model.Project, error) {
	parts, err := nameParts(parent, 2)
	if err != nil {
		return model.Project{}, invalidID("bucket parent")
	}
	return s.parseProject(parts[1])
}

func (s *Server) parseObjectParent(parent string) (model.BucketName, error) {
	parts, err := nameParts(parent, 4)
	if err != nil || parts[2] != "buckets" {
		return model.BucketName{}, invalidID("object parent")
	}
	project, err := s.parseProject(parts[1])
	if err != nil {
		return model.BucketName{}, err
	}
	id, err := model.ParseBucketName(parts[3])
	if err != nil {
		return model.BucketName{}, err
	}
	return id.WithProject(project), nil
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

func (s *Server) parseOperationParent(name string) (model.Project, error) {
	if name == "" {
		return model.Project{}, invalidID("operation parent")
	}
	name = strings.TrimSuffix(name, "/operations")
	parts, err := nameParts(name, 2)
	if err != nil {
		return model.Project{}, invalidID("operation parent")
	}
	return s.parseProject(parts[1])
}

func (s *Server) parseOpName(name string) (model.OperationID, error) {
	parts, err := nameParts(name, 4)
	if err != nil || parts[2] != "operations" {
		return model.OperationID{}, invalidID("operation name")
	}
	project, err := s.parseProject(parts[1])
	if err != nil {
		return model.OperationID{}, err
	}
	id, err := model.ParseOperationID(parts[3])
	if err != nil {
		return model.OperationID{}, invalidID("operation name")
	}
	return id.WithProject(project), nil
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
	project, zone, err := s.parseVolumeParent(in.GetParent())
	if err != nil {
		return model.CreateVolumeRequest{}, "", err
	}
	id, err := model.ParseVolumeID(in.GetVolumeId())
	if err != nil {
		return model.CreateVolumeRequest{}, "", invalidID("volume id")
	}
	id = id.WithProject(project)
	specIn := in.GetVolume().GetSpec()
	if in.GetVolume() == nil || specIn == nil {
		return model.CreateVolumeRequest{}, "", invalidSpec("volume")
	}
	labels, err := model.ParseLabels(in.GetVolume().GetLabels())
	if err != nil {
		return model.CreateVolumeRequest{}, "", invalidSpec("labels")
	}
	src := model.NoSource()
	switch source := specIn.GetSource().(type) {
	case *storagev1.VolumeSpec_Image:
		img, err := model.ParseImageName(source.Image)
		if err != nil {
			return model.CreateVolumeRequest{}, "", invalidID("image")
		}
		// An image is addressed under the caller's own project, including a
		// public image another project owns (vws-compute lists it there), so
		// a name in another project is reported as absent and a caller cannot
		// learn which images other projects have (#89). Whether the caller
		// may clone the DataSource, owned or public, is the backend's check.
		if img.Project() != project {
			return model.CreateVolumeRequest{}, "", notFound("image")
		}
		src, err = model.FromImage(img)
		if err != nil {
			return model.CreateVolumeRequest{}, "", invalidSpec("image")
		}
	case *storagev1.VolumeSpec_Snapshot:
		// An empty snapshot arm stays a blank volume, as GetSnapshot() == "" did.
		// An empty image arm is a name that does not parse.
		if snapName := source.Snapshot; snapName != "" {
			snap, _, err := s.parseSnapshotName(snapName)
			if err != nil {
				return model.CreateVolumeRequest{}, "", err
			}
			if snap.Project() != project {
				return model.CreateVolumeRequest{}, "", invalidID("snapshot")
			}
			src, err = model.FromSnapshot(snap)
			if err != nil {
				return model.CreateVolumeRequest{}, "", invalidSpec("snapshot")
			}
		}
	case nil:
	default:
		return model.CreateVolumeRequest{}, "", invalidSpec("source")
	}
	typ, err := volumeType(specIn.GetType())
	if err != nil {
		return model.CreateVolumeRequest{}, "", err
	}
	size, err := model.VolumeSize(specIn.GetSizeGib())
	if err != nil {
		return model.CreateVolumeRequest{}, "", invalidSpec("size_gib")
	}
	name := s.volumeName(zone, id)
	if got := in.GetVolume().GetName(); got != "" && got != name {
		return model.CreateVolumeRequest{}, "", invalidID("volume name")
	}
	spec := model.VolumeSpec{
		Size: size, Zone: zone, Type: typ, Source: src,
		Iops: specIn.GetIops(), Throughput: specIn.GetThroughputMibps(),
	}
	return model.CreateVolumeRequest{ID: id, Spec: spec.WithDefaultPerformance(), Labels: labels}, name, nil
}

func (s *Server) snapshotCreate(in *storagev1.CreateSnapshotRequest) (model.CreateSnapshotRequest, string, error) {
	project, region, err := s.parseSnapshotParent(in.GetParent())
	if err != nil {
		return model.CreateSnapshotRequest{}, "", err
	}
	id, err := model.ParseSnapshotID(in.GetSnapshotId())
	if err != nil {
		return model.CreateSnapshotRequest{}, "", invalidID("snapshot id")
	}
	id = id.WithProject(project)
	snap := in.GetSnapshot()
	if snap == nil || snap.GetSpec() == nil {
		return model.CreateSnapshotRequest{}, "", invalidSpec("snapshot")
	}
	labels, err := model.ParseLabels(snap.GetLabels())
	if err != nil {
		return model.CreateSnapshotRequest{}, "", invalidSpec("labels")
	}
	desc, err := model.ParseDescription(snap.GetSpec().GetDescription())
	if err != nil {
		return model.CreateSnapshotRequest{}, "", invalidSpec("description")
	}
	vol, zone, err := s.parseVolumeName(snap.GetSpec().GetVolume())
	if err != nil {
		return model.CreateSnapshotRequest{}, "", err
	}
	if vol.Project() != project {
		return model.CreateSnapshotRequest{}, "", invalidID("volume name")
	}
	name := s.snapshotName(id, region)
	if got := snap.GetName(); got != "" && got != name {
		return model.CreateSnapshotRequest{}, "", invalidID("snapshot name")
	}
	return model.CreateSnapshotRequest{
		ID: id, Source: vol, Zone: zone, Region: region, Labels: labels, Description: desc,
	}, name, nil
}

func (s *Server) bucketCreate(in *storagev1.CreateBucketRequest) (model.CreateBucketRequest, string, error) {
	project, err := s.parseBucketParent(in.GetParent())
	if err != nil {
		return model.CreateBucketRequest{}, "", err
	}
	id, err := model.ParseBucketName(in.GetBucketId())
	if err != nil {
		return model.CreateBucketRequest{}, "", invalidID("bucket id")
	}
	id = id.WithProject(project)
	b := in.GetBucket()
	if b == nil || b.GetSpec() == nil {
		return model.CreateBucketRequest{}, "", invalidSpec("bucket")
	}
	labels, err := model.ParseLabels(b.GetLabels())
	if err != nil {
		return model.CreateBucketRequest{}, "", invalidSpec("labels")
	}
	if b.GetSpec().GetVersioning() {
		return model.CreateBucketRequest{}, "", invalidSpec("versioning")
	}
	region, err := model.ParseRegion(b.GetSpec().GetRegion())
	if err != nil {
		return model.CreateBucketRequest{}, "", invalidSpec("region")
	}
	name := s.bucketName(id)
	if got := b.GetName(); got != "" && got != name {
		return model.CreateBucketRequest{}, "", invalidID("bucket name")
	}
	return model.CreateBucketRequest{Bucket: id, Region: region, Labels: labels}, name, nil
}

// protoTypes pairs each wire value with the model type of the same EC2 name.
var protoTypes = map[storagev1.VolumeType]model.VolumeType{
	storagev1.VolumeType_VOLUME_TYPE_STANDARD: model.VolumeTypeStandard,
	storagev1.VolumeType_VOLUME_TYPE_GP2:      model.VolumeTypeGP2,
	storagev1.VolumeType_VOLUME_TYPE_GP3:      model.VolumeTypeGP3,
	storagev1.VolumeType_VOLUME_TYPE_IO1:      model.VolumeTypeIO1,
	storagev1.VolumeType_VOLUME_TYPE_IO2:      model.VolumeTypeIO2,
	storagev1.VolumeType_VOLUME_TYPE_ST1:      model.VolumeTypeST1,
	storagev1.VolumeType_VOLUME_TYPE_SC1:      model.VolumeTypeSC1,
}

// volumeType reads a wire type. The deprecated SSD value is gp2, the type
// it always was, so a pre-#123 client keeps working (vws-proto v0.5.0).
func volumeType(t storagev1.VolumeType) (model.VolumeType, error) {
	if t == storagev1.VolumeType_VOLUME_TYPE_SSD { //nolint:staticcheck // read for pre-#123 clients
		return model.VolumeTypeGP2, nil
	}
	if typ, ok := protoTypes[t]; ok {
		return typ, nil
	}
	if t == storagev1.VolumeType_VOLUME_TYPE_UNSPECIFIED {
		return 0, invalidSpec("volume type")
	}
	return 0, fmt.Errorf("%w: volume type %s is unsupported", model.ErrUnsupportedVolumeType, t)
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
		Verb:       verbOf(op),
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
		return nil, fmt.Errorf("pack operation metadata: %w", err)
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
	if errors.Is(err, errNoResponse) {
		return out, nil
	}
	if err != nil {
		return nil, toStatus(ctx, err)
	}
	body, err := anypb.New(msg)
	if err != nil {
		return nil, fmt.Errorf("pack response: %w", err)
	}
	out.Result = &longrunningpb.Operation_Response{Response: body}
	return out, nil
}

func (s *Server) resourceName(ctx context.Context, op model.Operation) string {
	switch w := op.Work.(type) {
	case model.VolumeCreation:
		return s.volumeName(w.Spec.Zone, w.Volume)
	case model.VolumeExpansion:
		if vol, err := s.svc.Volume(ctx, w.Volume); err == nil {
			return s.volumeName(vol.Spec.Zone, w.Volume)
		}
		return ""
	case model.VolumeModification:
		if vol, err := s.svc.Volume(ctx, w.Volume); err == nil {
			return s.volumeName(vol.Spec.Zone, w.Volume)
		}
		return ""
	case model.VolumeDeletion:
		zone := w.Zone
		if zone.IsZero() {
			if vol, err := s.svc.Volume(ctx, w.Volume); err == nil {
				zone = vol.Spec.Zone
			}
		}
		return s.volumeName(zone, w.Volume)
	case model.SnapshotCreation:
		return s.snapshotName(w.Snapshot, w.Region)
	case model.SnapshotDeletion:
		return s.snapshotName(w.Snapshot, w.Region)
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

// errNoResponse means the operation succeeded and has no response body: the
// row it created is already gone or the work kind has none.
var errNoResponse = errors.New("operation has no response body")

func (s *Server) volumeResponse(ctx context.Context, id model.VolumeID) (proto.Message, error) {
	vol, err := s.svc.Volume(ctx, id)
	if errors.Is(err, model.ErrNotFound) {
		return nil, errNoResponse
	}
	if err != nil {
		return nil, err
	}
	return s.volumeMsg(ctx, vol), nil
}

func (s *Server) successMessage(ctx context.Context, op model.Operation) (proto.Message, error) {
	switch w := op.Work.(type) {
	case model.VolumeCreation:
		vol, err := s.svc.Volume(ctx, w.Volume)
		if errors.Is(err, model.ErrNotFound) {
			return nil, errNoResponse
		}
		if err != nil {
			return nil, err
		}
		return s.volumeMsg(ctx, vol), nil
	case model.VolumeExpansion:
		return s.volumeResponse(ctx, w.Volume)
	case model.VolumeModification:
		return s.volumeResponse(ctx, w.Volume)
	case model.SnapshotCreation:
		snap, err := s.svc.Snapshot(ctx, w.Snapshot)
		if errors.Is(err, model.ErrNotFound) {
			return nil, errNoResponse
		}
		if err != nil {
			return nil, err
		}
		return s.snapshotMsg(ctx, snap), nil
	case model.BucketCreation:
		b, err := s.svc.Bucket(ctx, w.Bucket)
		if errors.Is(err, model.ErrNotFound) {
			return nil, errNoResponse
		}
		if err != nil {
			return nil, err
		}
		return s.bucketMsg(b), nil
	case model.ObjectUpload:
		obj, err := s.svc.Object(ctx, w.Object.Path)
		if errors.Is(err, model.ErrNotFound) {
			return nil, errNoResponse
		}
		if err != nil {
			return nil, err
		}
		return s.objectMsg(obj), nil
	default:
		return nil, errNoResponse
	}
}

// volumeMsg is v with the instance that uses it, read from the backend.
func (s *Server) volumeMsg(ctx context.Context, v model.Volume) *storagev1.Volume {
	return s.volumeMsgAttached(ctx, v, s.svc.Attachments(ctx, []model.Volume{v}))
}

// instanceName is the compute instance resource name for an instance id in the
// volume's project and zone. An attachment never crosses either.
func instanceName(v model.Volume, id string) string {
	if id == "" {
		return ""
	}
	return "projects/" + v.ID.Project().String() + "/zones/" + v.Spec.Zone.String() + "/instances/" + id
}

func (s *Server) volumeMsgAttached(ctx context.Context, v model.Volume, attached map[model.VolumeID]string) *storagev1.Volume {
	out := &storagev1.Volume{
		Name: s.volumeName(v.Spec.Zone, v.ID), Generation: int64(v.Meta.Version), Etag: etagOf(v.Meta),
		Labels: v.Labels.Map(),
		Spec: &storagev1.VolumeSpec{
			SizeGib: int64(v.Spec.Size / model.GiB), Type: protoVolumeType(v.Spec.Type),
			Iops: v.Spec.Iops, ThroughputMibps: v.Spec.Throughput,
		},
		Status: &storagev1.VolumeStatus{
			Observed: observed(v.State, v.Meta.Version), SizeGib: int64(v.Spec.Size / model.GiB), ClaimName: s.svc.ClaimName(v),
			Instance: instanceName(v, attached[v.ID]),
		},
		CreateTime: stamp(v.Meta.CreateTime), UpdateTime: stamp(v.Meta.UpdateTime),
	}
	if img, ok := v.Spec.Source.Image(); ok {
		out.Spec.Source = &storagev1.VolumeSpec_Image{Image: img.String()}
	}
	if id, ok := v.Spec.Source.Snapshot(); ok {
		region := model.Region{}
		if snap, err := s.svc.Snapshot(ctx, id); err == nil {
			region = snap.Region
		}
		out.Spec.Source = &storagev1.VolumeSpec_Snapshot{Snapshot: s.snapshotName(id, region)}
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
		Name: s.snapshotName(sn.ID, sn.Region), Generation: int64(sn.Meta.Version), Etag: etagOf(sn.Meta),
		Labels: sn.Labels.Map(),
		Spec:   &storagev1.SnapshotSpec{Volume: volume, Description: sn.Description},
		Status: &storagev1.SnapshotStatus{
			Observed: observed(sn.State, sn.Meta.Version), SizeGib: int64(sn.Size / model.GiB), SnapshotObjectName: s.svc.SnapshotObjectName(sn),
		},
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
		Labels:     b.Labels.Map(),
		Spec:       &storagev1.BucketSpec{Region: b.Region.String()},
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
		Labels: o.Labels.Map(),
		Spec:   &storagev1.ObjectSpec{ContentType: o.ContentType, Sha256: o.SHA256.String()},
		Status: &storagev1.ObjectStatus{
			Observed: observed(model.Available{}, o.Meta.Version), SizeBytes: int64(o.Size), Md5: o.MD5.String(),
		},
		CreateTime: stamp(o.Meta.CreateTime), UpdateTime: stamp(o.Meta.UpdateTime),
	}
}

// protoVolumeType writes a model type. It never writes the deprecated SSD
// value: a stored ssd volume reads as gp2.
func protoVolumeType(t model.VolumeType) storagev1.VolumeType {
	for wire, typ := range protoTypes {
		if typ == t {
			return wire
		}
	}
	return storagev1.VolumeType_VOLUME_TYPE_UNSPECIFIED
}

func observed(state model.Lifecycle, version model.Version) *commonv1.ObservedState {
	out := &commonv1.ObservedState{}
	switch st := state.(type) {
	case model.Creating:
		out.State = commonv1.ReconciliationState_RECONCILIATION_STATE_RECONCILING
		out.Generation = int64(version) - 1
	case model.Available:
		out.State = commonv1.ReconciliationState_RECONCILIATION_STATE_READY
		out.Generation = int64(version)
	case model.Failed:
		out.State = commonv1.ReconciliationState_RECONCILIATION_STATE_ERROR
		out.Generation = int64(version) - 1
		out.Message = st.Reason.Message
	case model.Deleting:
		out.State = commonv1.ReconciliationState_RECONCILIATION_STATE_DELETING
		out.Generation = int64(version) - 1
		out.Message = st.Failed.Message
	default:
		out.State = commonv1.ReconciliationState_RECONCILIATION_STATE_UNSPECIFIED
	}
	if out.Generation < 0 {
		out.Generation = 0
	}
	return out
}

func verbOf(op model.Operation) commonv1.OperationVerb {
	if up, ok := op.Work.(model.ObjectUpload); ok && !up.Replaces.IsZero() {
		return commonv1.OperationVerb_OPERATION_VERB_UPDATE
	}
	switch op.Kind() {
	case model.KindExpandVolume, model.KindModifyVolume:
		return commonv1.OperationVerb_OPERATION_VERB_UPDATE
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
		return nil, fmt.Errorf("pack operation metadata: %w", err)
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

func toStatus(ctx context.Context, err error) error {
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
	case errors.Is(err, model.ErrUnsupportedVolumeType):
		return statusError{code: codes.InvalidArgument, reason: "UNSUPPORTED_VOLUME_TYPE", err: err}.grpc()
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
		if errors.Is(err, model.ErrDependencyMissing) {
			reason = "DEPENDENCY_MISSING"
		}
		return statusError{code: codes.FailedPrecondition, reason: reason, err: err}.grpc()
	case errors.Is(err, model.ErrStale):
		return statusError{code: codes.Aborted, reason: "GENERATION_CONFLICT", err: err}.grpc()
	default:
		slog.ErrorContext(ctx, "vws-storage: internal error", slog.String(obs.FieldErr, err.Error()))
		return status.Error(codes.Internal, "internal error")
	}
}

//nolint:wrapcheck // this builds the wire status; wrapping it would change what the client sees
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
