package model

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

// Sentinel errors. internal/server maps each to one gRPC code.
var (
	ErrInvalidArgument    = sentinelError("invalid argument")
	ErrNotFound           = sentinelError("not found")
	ErrAlreadyExists      = sentinelError("already exists")
	ErrFailedPrecondition = sentinelError("failed precondition")
	// ErrTokenReused means the client token is bound to a different request.
	ErrTokenReused = sentinelError("client token reused with a different request")
	// ErrStale means a stored row was not the record a Change was minted from.
	ErrStale = sentinelError("stale change")
	// ErrETagMismatch means the caller sent an etag other than the stored version.
	ErrETagMismatch = sentinelError("etag mismatch")
	// ErrDependencyMissing is an ErrFailedPrecondition whose cause is a resource
	// that must go first, such as a bucket that still holds objects. It prints
	// as ErrFailedPrecondition.
	ErrDependencyMissing error = refinedError{parent: ErrFailedPrecondition}
	// ErrUnsupportedVolumeType is an ErrInvalidArgument for a volume type this
	// build or deployment cannot serve. It prints as ErrInvalidArgument.
	ErrUnsupportedVolumeType error = refinedError{parent: ErrInvalidArgument}
)

// refinedError is a more specific sentinel that still matches its parent.
// name keeps two refinements of the same parent distinct under errors.Is.
type refinedError struct {
	name   string
	parent error
}

func (e refinedError) Error() string { return e.parent.Error() }
func (e refinedError) Unwrap() error { return e.parent }

type sentinelError string

func (e sentinelError) Error() string { return string(e) }

// Bytes is a size in bytes.
type Bytes int64

// GiB is one gibibyte.
const GiB Bytes = 1 << 30

// maxVolume is the largest EC2 volume (gp3 and io2, 65536 GiB). Each type has
// its own range (sizeOf).
const maxVolume Bytes = 64 << 40

// VolumeSize converts a gibibyte count into a volume size.
// Values that would wrap an int64, or that exceed the volume limit, are rejected.
func VolumeSize(gib int64) (Bytes, error) {
	if gib < 1 || gib > int64(maxVolume/GiB) {
		return 0, fmt.Errorf("%w: volume size", ErrInvalidArgument)
	}
	return Bytes(gib) * GiB, nil
}

// MaxObjectBytes is the single-request upload limit. Multipart is out of scope.
const MaxObjectBytes Bytes = 5 * GiB

// Digest is a SHA-256 digest. The zero value means the client did not send one.
type Digest [32]byte

// ParseDigest parses a 64-character lowercase hex SHA-256 digest.
func ParseDigest(hex string) (Digest, error) {
	if len(hex) != 64 || !isLowerHex(hex) {
		return Digest{}, fmt.Errorf("%w: digest", ErrInvalidArgument)
	}
	var d Digest
	for i := 0; i < 32; i++ {
		d[i] = hexByte(hex[i*2])<<4 | hexByte(hex[i*2+1])
	}
	return d, nil
}

// String returns the hex digest, or "" when d is zero.
func (d Digest) String() string {
	if d.IsZero() {
		return ""
	}
	const digits = "0123456789abcdef"
	var b [64]byte
	for i, v := range d {
		b[i*2] = digits[v>>4]
		b[i*2+1] = digits[v&0x0f]
	}
	return string(b[:])
}

// IsZero reports whether d was omitted.
func (d Digest) IsZero() bool { return d == Digest{} }

// MD5 is the MD5 of a stored payload. It is a content digest, not a
// concurrency token. The zero value means no payload digest is recorded.
type MD5 [16]byte

// ParseMD5 parses a 32-character lowercase hex MD5.
func ParseMD5(hex string) (MD5, error) {
	if len(hex) != 32 || !isLowerHex(hex) {
		return MD5{}, fmt.Errorf("%w: md5", ErrInvalidArgument)
	}
	var d MD5
	for i := range d {
		d[i] = hexByte(hex[i*2])<<4 | hexByte(hex[i*2+1])
	}
	return d, nil
}

// String returns the lowercase hex MD5, or "" when d is zero.
func (d MD5) String() string {
	if d == (MD5{}) {
		return ""
	}
	return hex.EncodeToString(d[:])
}

func hexByte(c byte) byte {
	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	default:
		return c - 'a' + 10
	}
}

// ETag is the backend-reported entity tag.
type ETag string

// VolumeType is the EC2 EBS volume type. Each value prints as the EC2
// VolumeType string of the same name. Which storage serves a type is the
// block backend's configuration, not the model's.
type VolumeType int

// The numbers are stored in fingerprints (ModifyVolumeRequest), so a value
// never changes. GP2 keeps the number of the pre-#123 "ssd" type, which it
// replaces.
const (
	// VolumeTypeUnspecified is not a valid stored type.
	VolumeTypeUnspecified VolumeType = iota
	// VolumeTypeStandard is EC2 "standard" (magnetic).
	VolumeTypeStandard
	// VolumeTypeGP2 is EC2 "gp2" (General Purpose SSD). A volume stored as
	// "ssd" before vws-storage#123 reads as gp2.
	VolumeTypeGP2
	// VolumeTypeGP3 is EC2 "gp3" (General Purpose SSD).
	VolumeTypeGP3
	// VolumeTypeIO1 is EC2 "io1" (Provisioned IOPS SSD).
	VolumeTypeIO1
	// VolumeTypeIO2 is EC2 "io2" (Provisioned IOPS SSD).
	VolumeTypeIO2
	// VolumeTypeST1 is EC2 "st1" (Throughput Optimized HDD).
	VolumeTypeST1
	// VolumeTypeSC1 is EC2 "sc1" (Cold HDD).
	VolumeTypeSC1
)

// legacySSD is the pre-#123 name of VolumeTypeGP2. It is read, and for one
// release it is also the stored name of gp2 (wireName), so a pre-#123 build
// can read gp2 rows after a rollback.
const legacySSD = "ssd"

var volumeTypeNames = [...]string{
	VolumeTypeStandard: "standard",
	VolumeTypeGP2:      "gp2",
	VolumeTypeGP3:      "gp3",
	VolumeTypeIO1:      "io1",
	VolumeTypeIO2:      "io2",
	VolumeTypeST1:      "st1",
	VolumeTypeSC1:      "sc1",
}

// VolumeTypes lists every valid type in number order.
func VolumeTypes() []VolumeType {
	return []VolumeType{
		VolumeTypeStandard, VolumeTypeGP2, VolumeTypeGP3, VolumeTypeIO1, VolumeTypeIO2, VolumeTypeST1, VolumeTypeSC1,
	}
}

// ParseVolumeType parses an EC2 volume type name. "ssd", the pre-#123 name,
// parses as gp2: this is the whole stored-data migration (see
// docs/volume-types.md), so it must stay.
func ParseVolumeType(s string) (VolumeType, error) {
	if s == legacySSD {
		return VolumeTypeGP2, nil
	}
	for i, name := range volumeTypeNames {
		if name != "" && name == s {
			return VolumeType(i), nil
		}
	}
	return 0, fmt.Errorf("%w: volume type", ErrInvalidArgument)
}

// String returns the EC2 name, or "" for an invalid type.
func (t VolumeType) String() string {
	if t < 0 || int(t) >= len(volumeTypeNames) {
		return ""
	}
	return volumeTypeNames[t]
}

// Valid reports whether t is one of VolumeTypes.
func (t VolumeType) Valid() bool { return t.String() != "" }

// wireName is the name the store keeps. For one release after #123 gp2 is
// stored as "ssd", which every build reads (a pre-#123 build as its SSD type,
// this one as gp2), so a rollback can still read every gp2 row. A later
// release may store "gp2", which this build already reads. Reading and
// writing again is byte-identical, so no row changes on its own.
func (t VolumeType) wireName() string {
	if t == VolumeTypeGP2 {
		return legacySSD
	}
	return t.String()
}

// fingerprintName is the name a create fingerprint uses. gp2 keeps "ssd" so
// that an idempotency token stored before #123 still matches its request.
func (t VolumeType) fingerprintName() string {
	if t == VolumeTypeGP2 {
		return legacySSD
	}
	return t.String()
}

// Source is absent, one local snapshot, or one compute image.
// Only the constructors set a field, and each sets at most one.
// The zero Source is NoSource. Source is comparable.
type Source struct {
	snap  SnapshotID
	image ImageName
}

// NoSource is a blank volume.
func NoSource() Source { return Source{} }

// FromSnapshot returns a snapshot source. id must be non-zero.
func FromSnapshot(id SnapshotID) (Source, error) {
	if id.IsZero() {
		return Source{}, fmt.Errorf("%w: snapshot id", ErrInvalidArgument)
	}
	return Source{snap: id}, nil
}

// FromImage returns an image source. name must be non-zero.
// The image size is not visible here, so nothing in this process checks
// size_gib against it.
func FromImage(name ImageName) (Source, error) {
	if name.IsZero() {
		return Source{}, fmt.Errorf("%w: image name", ErrInvalidArgument)
	}
	return Source{image: name}, nil
}

// Snapshot returns the source snapshot when one is set.
func (s Source) Snapshot() (SnapshotID, bool) {
	if s.snap.IsZero() {
		return SnapshotID{}, false
	}
	return s.snap, true
}

// Image returns the source image when one is set.
func (s Source) Image() (ImageName, bool) {
	if s.image.IsZero() {
		return ImageName{}, false
	}
	return s.image, true
}

// canonical is the fingerprint form: "-" when blank, SnapshotID.String() for a
// snapshot, ImageName.String() for an image. The first two are what stored
// token fingerprints were hashed from and must not change.
func (s Source) canonical() string {
	switch {
	case !s.image.IsZero():
		return s.image.String()
	case !s.snap.IsZero():
		return s.snap.String()
	default:
		return "-"
	}
}

// wire is the persisted form: "" when blank, SnapshotID.wire() for a snapshot,
// the full image name for an image. An image name is six segments and a
// snapshot wire value is one segment or projects/{project}/snapshots/{id},
// so sourceFrom can tell them apart. A binary that predates images fails to
// decode an image row instead of reading it as blank. SQLite reads one kind
// in a single scan, so that failure stops the list.
func (s Source) wire() string {
	switch {
	case !s.image.IsZero():
		return s.image.String()
	case !s.snap.IsZero():
		return s.snap.wire()
	default:
		return ""
	}
}

func sourceFrom(w string) (Source, error) {
	if w == "" {
		return NoSource(), nil
	}
	img, ierr := ParseImageName(w)
	if ierr == nil {
		return FromImage(img)
	}
	if imageWire(w) {
		return Source{}, ierr
	}
	id, err := ParseSnapshotID(w)
	if err != nil {
		return Source{}, err
	}
	return FromSnapshot(id)
}

func imageWire(w string) bool {
	parts := strings.Split(w, "/")
	return len(parts) == 6 && parts[0] == "projects" && parts[2] == "regions" && parts[4] == "images"
}

// VolumeSpec is the desired shape of a volume.
// Iops and Throughput (MiB/s) are the provisioned performance of EC2
// CreateVolume. They are stored and reported; the backend's StorageClass is
// what delivers them, and that is the admin's responsibility.
type VolumeSpec struct {
	Size       Bytes
	Zone       Zone
	Type       VolumeType
	Source     Source
	Iops       int64
	Throughput int64
}

// performance is the EC2 range of provisioned IOPS and throughput of a type
// (vws-proto v0.5.0 VolumeSpec). A zero range means the type takes no value.
// iopsPerGiB caps IOPS by size; IOPS up to freeIops need no size (gp3's
// included baseline). mibsPerKiops caps throughput by IOPS, in MiB/s per
// 1000 IOPS (gp3: 0.25 MiB/s per IOPS).
type performance struct {
	minIops, maxIops, defIops int64
	minMibs, maxMibs, defMibs int64
	iopsPerGiB, freeIops      int64
	mibsPerKiops              int64
}

var performanceOf = map[VolumeType]performance{
	VolumeTypeGP3: {
		minIops: 3000, maxIops: 80000, defIops: 3000, minMibs: 125, maxMibs: 2000, defMibs: 125,
		iopsPerGiB: 500, freeIops: 3000, mibsPerKiops: 250,
	},
	VolumeTypeIO1: {minIops: 100, maxIops: 64000, iopsPerGiB: 50},
	VolumeTypeIO2: {minIops: 100, maxIops: 256000, iopsPerGiB: 1000},
}

// WithDefaultPerformance fills what EC2 fills when a field is left out: gp3
// takes 3000 IOPS and 125 MiB/s. io1 and io2 IOPS have no default.
func (s VolumeSpec) WithDefaultPerformance() VolumeSpec {
	p := performanceOf[s.Type]
	if s.Iops == 0 {
		s.Iops = p.defIops
	}
	if s.Throughput == 0 {
		s.Throughput = p.defMibs
	}
	return s
}

// sizeOf is the EC2 CreateVolume and ModifyVolume size range of each type, in
// GiB.
var sizeOf = map[VolumeType]struct{ min, max Bytes }{
	VolumeTypeStandard: {1, 1024},
	VolumeTypeGP2:      {1, 16384},
	VolumeTypeGP3:      {1, 65536},
	VolumeTypeIO1:      {4, 16384},
	VolumeTypeIO2:      {4, 65536},
	VolumeTypeST1:      {125, 16384},
	VolumeTypeSC1:      {125, 16384},
}

// checkSize refuses a size outside the range of type t.
func checkSize(t VolumeType, size Bytes) error {
	r, ok := sizeOf[t]
	if !ok || size < r.min*GiB || size > r.max*GiB {
		return fmt.Errorf("%w: volume type %s needs a size from %d to %d GiB", ErrInvalidArgument, t, r.min, r.max)
	}
	return nil
}

// checkPerformance enforces the per-type IOPS and throughput rules of EC2
// CreateVolume and ModifyVolume on a volume of size bytes: the ranges, IOPS
// per GiB, and gp3 throughput per IOPS.
func checkPerformance(t VolumeType, size Bytes, iops, mibs int64) error {
	p := performanceOf[t]
	switch {
	case p.maxIops == 0 && iops != 0:
		return fmt.Errorf("%w: volume type %s takes no iops", ErrInvalidArgument, t)
	case p.maxIops != 0 && (iops < p.minIops || iops > p.maxIops):
		return fmt.Errorf("%w: volume type %s needs iops from %d to %d", ErrInvalidArgument, t, p.minIops, p.maxIops)
	case p.maxMibs == 0 && mibs != 0:
		return fmt.Errorf("%w: volume type %s takes no throughput", ErrInvalidArgument, t)
	case p.maxMibs != 0 && (mibs < p.minMibs || mibs > p.maxMibs):
		return fmt.Errorf("%w: volume type %s needs throughput from %d to %d MiB/s",
			ErrInvalidArgument, t, p.minMibs, p.maxMibs)
	}
	if gib := int64(size / GiB); p.iopsPerGiB != 0 && iops > p.freeIops && iops > p.iopsPerGiB*gib {
		return fmt.Errorf("%w: volume type %s allows at most %d iops per GiB; %d GiB allows %d",
			ErrInvalidArgument, t, p.iopsPerGiB, gib, max(p.freeIops, p.iopsPerGiB*gib))
	}
	if p.mibsPerKiops != 0 && mibs*1000 > iops*p.mibsPerKiops {
		return fmt.Errorf("%w: volume type %s allows at most 0.25 MiB/s of throughput per iops; %d iops allow %d MiB/s",
			ErrInvalidArgument, t, iops, iops*p.mibsPerKiops/1000)
	}
	return nil
}

// retypePerformance is the IOPS and throughput a volume stores after a
// change to type to, as EC2 ModifyVolume without Iops or Throughput: "If you
// change the volume type to io1, io2, or gp3, the default is 3,000" IOPS, and
// throughput is 125 unless source and target are both gp3, which is no type
// change. Other types store none. The caller checks the result against the
// size (checkPerformance); a default over the IOPS-per-GiB cap is refused, as
// EC2 refuses it.
func retypePerformance(to VolumeType) (iops, mibs int64) {
	p := performanceOf[to]
	if p.maxIops != 0 {
		iops = 3000
	}
	return iops, p.defMibs
}

// Request is one client mutation. Adding a kind requires fingerprint so the
// token hash cannot silently drop a field.
type Request interface {
	Validate() error
	Project() Project
	fingerprint() string
	isRequest()
}

// CreateVolumeRequest creates a blank volume, restores one from a snapshot,
// or records a compute image name.
// ID is the client-chosen volume id. The zero ID asks the engine to mint one.
type CreateVolumeRequest struct {
	ID     VolumeID
	Spec   VolumeSpec
	Labels Labels
}

// DeleteVolumeRequest deletes an available volume.
// Expected is the etag version. Zero means the caller did not send one.
// Zone is the zone in the resource name. Zero skips that check.
type DeleteVolumeRequest struct {
	Volume       VolumeID
	Zone         Zone
	Expected     Version
	AllowMissing bool
}

// ExpandVolumeRequest grows an available volume to Size.
// Expected is the etag version. Zero means the caller did not send one.
// Zone is the zone in the resource name. Zero skips that check.
type ExpandVolumeRequest struct {
	Volume   VolumeID
	Zone     Zone
	Size     Bytes
	Expected Version
}

// ModifyVolumeRequest changes an available volume's labels, type, size and
// provisioned performance in one request, as EC2 ModifyVolume (#87).
// SetLabels replaces the labels with Labels (empty clears them). A zero Type
// keeps the type. A zero Size keeps the size; a larger one grows the volume
// and stores the other values when the growth finishes. SetIops sets Iops,
// where zero is the type's EC2 CreateVolume default (gp3 3000; io1 and io2
// have none, so zero is refused); without SetIops the volume keeps its IOPS,
// or takes the EC2 ModifyVolume default when the type changes. SetThroughput
// and Throughput work the same way (gp3 125 MiB/s). Expected and Zone are as
// in ExpandVolumeRequest.
type ModifyVolumeRequest struct {
	Volume        VolumeID
	Zone          Zone
	SetLabels     bool
	Labels        Labels
	Type          VolumeType
	Size          Bytes
	SetIops       bool
	Iops          int64
	SetThroughput bool
	Throughput    int64
	Expected      Version
}

// CreateSnapshotRequest snapshots an available volume.
// ID is the client-chosen snapshot id. The zero ID asks the engine to mint one.
// Zone is the zone in the source volume name. Zero skips that check.
// Region is the region in the snapshot parent. Zero is accepted by the engine
// and rejected by the server.
type CreateSnapshotRequest struct {
	ID          SnapshotID
	Source      VolumeID
	Zone        Zone
	Region      Region
	Labels      Labels
	Description string
}

// DeleteSnapshotRequest deletes an available snapshot.
// Region is the region in the resource name. Zero skips that check.
type DeleteSnapshotRequest struct {
	Snapshot     SnapshotID
	Region       Region
	Expected     Version
	AllowMissing bool
}

// CreateBucketRequest creates a bucket.
type CreateBucketRequest struct {
	Bucket    BucketName
	Region    Region
	Labels    Labels
	Namespace BucketNamespace
}

// DeleteBucketRequest deletes an empty bucket.
type DeleteBucketRequest struct {
	Bucket       BucketName
	Expected     Version
	AllowMissing bool
}

// PutObjectRequest declares an upload. The bytes go to the object backend.
type PutObjectRequest struct {
	Path        ObjectPath
	Size        Bytes
	SHA256      Digest
	ContentType string
	Labels      Labels
}

// UpdateObjectRequest overwrites an existing object, or creates one when
// AllowMissing is set. HasContents is false when the caller omitted the
// payload; the stored bytes stay and only ContentType and Labels change.
// An empty payload cannot be told from an omitted one, so a zero-length
// replacement is not a request this type can express.
type UpdateObjectRequest struct {
	Path         ObjectPath
	Size         Bytes
	SHA256       Digest
	ContentType  string
	Labels       Labels
	HasContents  bool
	Expected     Version
	AllowMissing bool
}

// WriteObjectRequest replaces the payload of an existing object and keeps its
// content type and labels. Size and SHA256 describe the new bytes; the caller
// always fills SHA256, so a zero-length payload is expressible.
// Expected is the stored version token, or zero to skip the check.
type WriteObjectRequest struct {
	Path     ObjectPath
	Size     Bytes
	SHA256   Digest
	Expected Version
}

// AsUpdate returns the overwrite this write performs on head.
func (r WriteObjectRequest) AsUpdate(head Object) UpdateObjectRequest {
	return UpdateObjectRequest{
		Path: r.Path, Size: r.Size, SHA256: r.SHA256,
		ContentType: head.ContentType, Labels: head.Labels,
		HasContents: true, Expected: r.Expected,
	}
}

// DeleteObjectRequest removes an object from reads and reclaims its bytes.
type DeleteObjectRequest struct {
	Path         ObjectPath
	Expected     Version
	AllowMissing bool
}

// Validate implements Request.
func (r CreateVolumeRequest) Project() Project   { return r.ID.Project() }
func (r DeleteVolumeRequest) Project() Project   { return r.Volume.Project() }
func (r ExpandVolumeRequest) Project() Project   { return r.Volume.Project() }
func (r ModifyVolumeRequest) Project() Project   { return r.Volume.Project() }
func (r CreateSnapshotRequest) Project() Project { return r.ID.Project() }
func (r DeleteSnapshotRequest) Project() Project { return r.Snapshot.Project() }
func (r CreateBucketRequest) Project() Project   { return r.Bucket.Project() }
func (r DeleteBucketRequest) Project() Project   { return r.Bucket.Project() }
func (r PutObjectRequest) Project() Project      { return r.Path.Project() }
func (r UpdateObjectRequest) Project() Project   { return r.Path.Project() }
func (r WriteObjectRequest) Project() Project    { return r.Path.Project() }
func (r DeleteObjectRequest) Project() Project   { return r.Path.Project() }

func (r CreateVolumeRequest) Validate() error {
	if r.Spec.Size < GiB || r.Spec.Size > maxVolume || r.Spec.Size%GiB != 0 {
		return fmt.Errorf("%w: volume size", ErrInvalidArgument)
	}
	if r.Spec.Zone.IsZero() {
		return fmt.Errorf("%w: zone", ErrInvalidArgument)
	}
	if !r.Spec.Type.Valid() {
		return fmt.Errorf("%w: volume type", ErrInvalidArgument)
	}
	if err := checkSize(r.Spec.Type, r.Spec.Size); err != nil {
		return err
	}
	return checkPerformance(r.Spec.Type, r.Spec.Size, r.Spec.Iops, r.Spec.Throughput)
}

// Validate implements Request.
func (r DeleteVolumeRequest) Validate() error {
	if r.Volume.IsZero() {
		return fmt.Errorf("%w: volume id", ErrInvalidArgument)
	}
	return nil
}

// Validate implements Request. A request that changes nothing is invalid.
func (r ModifyVolumeRequest) Validate() error {
	if r.Volume.IsZero() {
		return fmt.Errorf("%w: volume id", ErrInvalidArgument)
	}
	if r.Type != VolumeTypeUnspecified && !r.Type.Valid() {
		return fmt.Errorf("%w: volume type", ErrInvalidArgument)
	}
	if r.Size != 0 && (r.Size < GiB || r.Size > maxVolume || r.Size%GiB != 0) {
		return fmt.Errorf("%w: volume size", ErrInvalidArgument)
	}
	if r.Iops < 0 || r.Throughput < 0 {
		return fmt.Errorf("%w: iops and throughput cannot be negative", ErrInvalidArgument)
	}
	if !r.SetLabels && r.Type == VolumeTypeUnspecified && r.Size == 0 && !r.SetIops && !r.SetThroughput {
		return fmt.Errorf("%w: nothing to update", ErrInvalidArgument)
	}
	return nil
}

// Validate implements Request.
func (r ExpandVolumeRequest) Validate() error {
	if r.Volume.IsZero() {
		return fmt.Errorf("%w: volume id", ErrInvalidArgument)
	}
	if r.Size < GiB || r.Size > maxVolume || r.Size%GiB != 0 {
		return fmt.Errorf("%w: volume size", ErrInvalidArgument)
	}
	return nil
}

// Validate implements Request.
func (r CreateSnapshotRequest) Validate() error {
	if r.Source.IsZero() {
		return fmt.Errorf("%w: volume id", ErrInvalidArgument)
	}
	if _, err := ParseDescription(r.Description); err != nil {
		return err
	}
	return nil
}

// Validate implements Request.
func (r DeleteSnapshotRequest) Validate() error {
	if r.Snapshot.IsZero() {
		return fmt.Errorf("%w: snapshot id", ErrInvalidArgument)
	}
	return nil
}

// Validate implements Request.
func (r CreateBucketRequest) Validate() error {
	if r.Bucket.IsZero() {
		return fmt.Errorf("%w: bucket name", ErrInvalidArgument)
	}
	return nil
}

// Validate implements Request.
func (r DeleteBucketRequest) Validate() error {
	if r.Bucket.IsZero() {
		return fmt.Errorf("%w: bucket name", ErrInvalidArgument)
	}
	return nil
}

// Validate implements Request.
func (r PutObjectRequest) Validate() error {
	if r.Path.Bucket.IsZero() || r.Path.Key.IsZero() {
		return fmt.Errorf("%w: object path", ErrInvalidArgument)
	}
	if r.Size < 0 || r.Size > MaxObjectBytes {
		return fmt.Errorf("%w: object size", ErrInvalidArgument)
	}
	return validateContentType(r.ContentType)
}

// Validate implements Request.
func (r UpdateObjectRequest) Validate() error {
	if r.Path.Bucket.IsZero() || r.Path.Key.IsZero() {
		return fmt.Errorf("%w: object path", ErrInvalidArgument)
	}
	if r.AllowMissing && !r.HasContents {
		return fmt.Errorf("%w: contents", ErrInvalidArgument)
	}
	if !r.HasContents {
		if r.Size != 0 || !r.SHA256.IsZero() {
			return fmt.Errorf("%w: contents", ErrInvalidArgument)
		}
		return validateContentType(r.ContentType)
	}
	if r.Size < 0 || r.Size > MaxObjectBytes {
		return fmt.Errorf("%w: object size", ErrInvalidArgument)
	}
	return validateContentType(r.ContentType)
}

func validateContentType(s string) error {
	if len(s) > 128 {
		return fmt.Errorf("%w: content type", ErrInvalidArgument)
	}
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] == 0x7f {
			return fmt.Errorf("%w: content type", ErrInvalidArgument)
		}
	}
	return nil
}

// Validate implements Request.
func (r WriteObjectRequest) Validate() error {
	if r.Path.Bucket.IsZero() || r.Path.Key.IsZero() {
		return fmt.Errorf("%w: object path", ErrInvalidArgument)
	}
	if r.Size < 0 || r.Size > MaxObjectBytes {
		return fmt.Errorf("%w: object size", ErrInvalidArgument)
	}
	if r.SHA256.IsZero() {
		return fmt.Errorf("%w: sha256", ErrInvalidArgument)
	}
	return nil
}

// Validate implements Request.
func (r DeleteObjectRequest) Validate() error {
	if r.Path.Bucket.IsZero() || r.Path.Key.IsZero() {
		return fmt.Errorf("%w: object path", ErrInvalidArgument)
	}
	return nil
}

// A spec without IOPS or throughput keeps the pre-#123 fingerprint.
func (r CreateVolumeRequest) fingerprint() string {
	f := fmt.Sprintf("create-volume\n%s\n%d\n%s\n%s\n%s\n%s", r.ID.String(), r.Spec.Size, r.Spec.Zone.String(),
		r.Spec.Type.fingerprintName(), r.Spec.Source.canonical(), r.Labels.canonical())
	if r.Spec.Iops != 0 || r.Spec.Throughput != 0 {
		f += fmt.Sprintf("\niops=%d\nthroughput=%d", r.Spec.Iops, r.Spec.Throughput)
	}
	return f
}

func (r DeleteVolumeRequest) fingerprint() string {
	return fmt.Sprintf("delete-volume\n%s\n%s\n%d\n%t", r.Volume.String(), r.Zone.String(), r.Expected, r.AllowMissing)
}

func (r ExpandVolumeRequest) fingerprint() string {
	return fmt.Sprintf("expand-volume\n%s\n%s\n%d\n%d", r.Volume.String(), r.Zone.String(), r.Size, r.Expected)
}

// A request without size or performance keeps the pre-#87 fingerprint.
func (r ModifyVolumeRequest) fingerprint() string {
	f := fmt.Sprintf("modify-volume\n%s\n%s\n%t\n%s\n%d\n%d",
		r.Volume.String(), r.Zone.String(), r.SetLabels, r.Labels.canonical(), r.Type, r.Expected)
	if r.Size != 0 || r.SetIops || r.SetThroughput {
		f += fmt.Sprintf("\nsize=%d\niops=%t,%d\nthroughput=%t,%d", r.Size, r.SetIops, r.Iops, r.SetThroughput, r.Throughput)
	}
	return f
}

func (r CreateSnapshotRequest) fingerprint() string {
	return "create-snapshot\n" + r.ID.String() + "\n" + r.Source.String() + "\n" + r.Zone.String() + "\n" + r.Region.String() + "\n" + r.Labels.canonical() + "\n" + r.Description
}

func (r DeleteSnapshotRequest) fingerprint() string {
	return fmt.Sprintf("delete-snapshot\n%s\n%s\n%d\n%t", r.Snapshot.String(), r.Region.String(), r.Expected, r.AllowMissing)
}

func (r CreateBucketRequest) fingerprint() string {
	f := "create-bucket\n" + r.Bucket.String() + "\n" + r.Region.String() + "\n" + r.Labels.canonical()
	// A global create keeps the pre-namespace fingerprint, so a token from
	// before the upgrade still matches its request.
	if r.Namespace != BucketNamespaceGlobal {
		f += "\nnamespace=" + r.Namespace.String()
	}
	return f
}

func (r DeleteBucketRequest) fingerprint() string {
	return fmt.Sprintf("delete-bucket\n%s\n%d\n%t", r.Bucket.String(), r.Expected, r.AllowMissing)
}

func (r PutObjectRequest) fingerprint() string {
	return fmt.Sprintf("put-object\n%s\n%s\n%d\n%s\n%s\n%s", r.Path.Bucket.String(), r.Path.Key.String(), r.Size, r.SHA256.String(), r.ContentType, r.Labels.canonical())
}

func (r UpdateObjectRequest) fingerprint() string {
	return fmt.Sprintf("update-object\n%s\n%s\n%d\n%s\n%s\n%s\n%t\n%d\n%t", r.Path.Bucket.String(), r.Path.Key.String(), r.Size, r.SHA256.String(), r.ContentType, r.Labels.canonical(), r.HasContents, r.Expected, r.AllowMissing)
}

func (r WriteObjectRequest) fingerprint() string {
	return fmt.Sprintf("write-object\n%s\n%s\n%d\n%s\n%d", r.Path.Bucket.String(), r.Path.Key.String(), r.Size, r.SHA256.String(), r.Expected)
}

func (r DeleteObjectRequest) fingerprint() string {
	return fmt.Sprintf("delete-object\n%s\n%d\n%t", r.Path.String(), r.Expected, r.AllowMissing)
}

func (CreateVolumeRequest) isRequest()   {}
func (DeleteVolumeRequest) isRequest()   {}
func (ExpandVolumeRequest) isRequest()   {}
func (ModifyVolumeRequest) isRequest()   {}
func (CreateSnapshotRequest) isRequest() {}
func (DeleteSnapshotRequest) isRequest() {}
func (CreateBucketRequest) isRequest()   {}
func (DeleteBucketRequest) isRequest()   {}
func (PutObjectRequest) isRequest()      {}
func (UpdateObjectRequest) isRequest()   {}
func (WriteObjectRequest) isRequest()    {}
func (DeleteObjectRequest) isRequest()   {}

// Content returns the comparable contents of the upload.
func (r PutObjectRequest) Content() ObjectContent {
	return ObjectContent{Size: r.Size, SHA256: r.SHA256, ContentType: r.ContentType, Labels: r.Labels}
}

// Content returns the comparable contents of the update.
func (r UpdateObjectRequest) Content() ObjectContent {
	return ObjectContent{Size: r.Size, SHA256: r.SHA256, ContentType: r.ContentType, Labels: r.Labels}
}

// AsPut returns the create-shaped request for an allow-missing update.
func (r UpdateObjectRequest) AsPut() PutObjectRequest {
	return PutObjectRequest{Path: r.Path, Size: r.Size, SHA256: r.SHA256, ContentType: r.ContentType, Labels: r.Labels}
}

// Fingerprint is the canonical hash of a Request.
type Fingerprint [32]byte

// String returns the hex encoding, or "" when f is zero.
func (f Fingerprint) String() string { return Digest(f).String() }

// HashRequest hashes req. The hash covers the kind and every field a client
// sends. It does not cover the token, the server-minted id, or proto encoding.
func HashRequest(req Request) (Fingerprint, error) {
	if req == nil {
		return Fingerprint{}, fmt.Errorf("%w: request", ErrInvalidArgument)
	}
	return sha256.Sum256([]byte(req.fingerprint())), nil
}

// TokenClaim binds a client token to a request fingerprint.
type TokenClaim struct {
	Token       ClientToken
	Fingerprint Fingerprint
}

// Claim fingerprints req for tok.
func Claim(tok ClientToken, req Request) (TokenClaim, error) {
	fp, err := HashRequest(req)
	if err != nil {
		return TokenClaim{}, err
	}
	return TokenClaim{Token: tok, Fingerprint: fp}, nil
}

// IsZero reports whether c carries no client token.
func (c TokenClaim) IsZero() bool { return c.Token.IsZero() }

// TokenRecord is the persisted binding from token to operation.
type TokenRecord struct {
	Project Project
	Claim   TokenClaim
	Op      OperationID
	Meta    Meta
}

// Match returns the bound operation when c is the same request.
func (r TokenRecord) Match(c TokenClaim) (OperationID, error) {
	if r.Claim.Token != c.Token || r.Claim.Fingerprint != c.Fingerprint {
		return OperationID{}, ErrTokenReused
	}
	return r.Op, nil
}

// Version counts writes to a row, starting at 1.
type Version uint64

// Meta is bookkeeping set only by transitions.
type Meta struct {
	Version    Version
	CreateTime time.Time
	UpdateTime time.Time
}

func fresh(now time.Time) Meta {
	n := norm(now)
	return Meta{Version: 1, CreateTime: n, UpdateTime: n}
}

func bump(m Meta, now time.Time) Meta {
	m.Version++
	m.UpdateTime = norm(now)
	return m
}

func norm(t time.Time) time.Time {
	if t.IsZero() {
		return time.Time{}
	}
	return t.UTC().Truncate(time.Microsecond)
}
