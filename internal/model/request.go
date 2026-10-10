package model

import (
	"crypto/sha256"
	"fmt"
	"time"
)

// Sentinel errors. internal/server maps each to one gRPC code.
var (
	ErrInvalidArgument    = errSentinel("invalid argument")
	ErrNotFound           = errSentinel("not found")
	ErrAlreadyExists      = errSentinel("already exists")
	ErrFailedPrecondition = errSentinel("failed precondition")
	// ErrTokenReused means the client token is bound to a different request.
	ErrTokenReused = errSentinel("client token reused with a different request")
	// ErrStale means a stored row was not the record a Change was minted from.
	ErrStale = errSentinel("stale change")
	// ErrETagMismatch means the caller sent an etag other than the stored version.
	ErrETagMismatch = errSentinel("etag mismatch")
)

type errSentinel string

func (e errSentinel) Error() string { return string(e) }

// Bytes is a size in bytes.
type Bytes int64

// GiB is one gibibyte.
const GiB Bytes = 1 << 30

const maxVolume Bytes = 16 << 40

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

// VolumeType selects a block performance class.
type VolumeType int

const (
	// VolumeTypeUnspecified is not a valid stored type.
	VolumeTypeUnspecified VolumeType = iota
	// VolumeTypeStandard is the default performance class.
	VolumeTypeStandard
	// VolumeTypeSSD is the higher performance class.
	VolumeTypeSSD
)

// ParseVolumeType parses "standard" or "ssd".
func ParseVolumeType(s string) (VolumeType, error) {
	switch s {
	case "standard":
		return VolumeTypeStandard, nil
	case "ssd":
		return VolumeTypeSSD, nil
	default:
		return 0, fmt.Errorf("%w: volume type", ErrInvalidArgument)
	}
}

// String returns "standard", "ssd", or "".
func (t VolumeType) String() string {
	switch t {
	case VolumeTypeStandard:
		return "standard"
	case VolumeTypeSSD:
		return "ssd"
	default:
		return ""
	}
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
// decode an image row instead of reading it as blank.
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
	if img, err := ParseImageName(w); err == nil {
		return FromImage(img)
	}
	id, err := ParseSnapshotID(w)
	if err != nil {
		return Source{}, err
	}
	return FromSnapshot(id)
}

// VolumeSpec is the desired shape of a volume.
type VolumeSpec struct {
	Size   Bytes
	Zone   Zone
	Type   VolumeType
	Source Source
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
	Bucket BucketName
	Region Region
	Labels Labels
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

// DeleteObjectRequest removes an object from reads and reclaims its bytes.
type DeleteObjectRequest struct {
	Path         ObjectPath
	Expected     Version
	AllowMissing bool
}

// Validate implements Request.
func (r CreateVolumeRequest) Project() Project   { return r.ID.Project() }
func (r DeleteVolumeRequest) Project() Project   { return r.Volume.Project() }
func (r CreateSnapshotRequest) Project() Project { return r.ID.Project() }
func (r DeleteSnapshotRequest) Project() Project { return r.Snapshot.Project() }
func (r CreateBucketRequest) Project() Project   { return r.Bucket.Project() }
func (r DeleteBucketRequest) Project() Project   { return r.Bucket.Project() }
func (r PutObjectRequest) Project() Project      { return r.Path.Project() }
func (r UpdateObjectRequest) Project() Project   { return r.Path.Project() }
func (r DeleteObjectRequest) Project() Project   { return r.Path.Project() }

func (r CreateVolumeRequest) Validate() error {
	if r.Spec.Size < GiB || r.Spec.Size > maxVolume || r.Spec.Size%GiB != 0 {
		return fmt.Errorf("%w: volume size", ErrInvalidArgument)
	}
	if r.Spec.Zone.IsZero() {
		return fmt.Errorf("%w: zone", ErrInvalidArgument)
	}
	if r.Spec.Type != VolumeTypeStandard && r.Spec.Type != VolumeTypeSSD {
		return fmt.Errorf("%w: volume type", ErrInvalidArgument)
	}
	return nil
}

// Validate implements Request.
func (r DeleteVolumeRequest) Validate() error {
	if r.Volume.IsZero() {
		return fmt.Errorf("%w: volume id", ErrInvalidArgument)
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
func (r DeleteObjectRequest) Validate() error {
	if r.Path.Bucket.IsZero() || r.Path.Key.IsZero() {
		return fmt.Errorf("%w: object path", ErrInvalidArgument)
	}
	return nil
}

func (r CreateVolumeRequest) fingerprint() string {
	return fmt.Sprintf("create-volume\n%s\n%d\n%s\n%s\n%s\n%s", r.ID.String(), r.Spec.Size, r.Spec.Zone.String(), r.Spec.Type.String(), r.Spec.Source.canonical(), r.Labels.canonical())
}

func (r DeleteVolumeRequest) fingerprint() string {
	return fmt.Sprintf("delete-volume\n%s\n%s\n%d\n%t", r.Volume.String(), r.Zone.String(), r.Expected, r.AllowMissing)
}

func (r CreateSnapshotRequest) fingerprint() string {
	return "create-snapshot\n" + r.ID.String() + "\n" + r.Source.String() + "\n" + r.Zone.String() + "\n" + r.Region.String() + "\n" + r.Labels.canonical() + "\n" + r.Description
}

func (r DeleteSnapshotRequest) fingerprint() string {
	return fmt.Sprintf("delete-snapshot\n%s\n%s\n%d\n%t", r.Snapshot.String(), r.Region.String(), r.Expected, r.AllowMissing)
}

func (r CreateBucketRequest) fingerprint() string {
	return "create-bucket\n" + r.Bucket.String() + "\n" + r.Region.String() + "\n" + r.Labels.canonical()
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

func (r DeleteObjectRequest) fingerprint() string {
	return fmt.Sprintf("delete-object\n%s\n%d\n%t", r.Path.String(), r.Expected, r.AllowMissing)
}

func (CreateVolumeRequest) isRequest()   {}
func (DeleteVolumeRequest) isRequest()   {}
func (CreateSnapshotRequest) isRequest() {}
func (DeleteSnapshotRequest) isRequest() {}
func (CreateBucketRequest) isRequest()   {}
func (DeleteBucketRequest) isRequest()   {}
func (PutObjectRequest) isRequest()      {}
func (UpdateObjectRequest) isRequest()   {}
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
