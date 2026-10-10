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
)

type errSentinel string

func (e errSentinel) Error() string { return string(e) }

// Bytes is a size in bytes.
type Bytes int64

// GiB is one gibibyte.
const GiB Bytes = 1 << 30

const maxVolume Bytes = 16 << 40

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

// Source is either absent or exactly one snapshot.
// A zero SnapshotID is not a valid source. Use NoSource.
type Source struct {
	id SnapshotID
	ok bool
}

// NoSource is a blank volume.
func NoSource() Source { return Source{} }

// FromSnapshot returns a source. id must be non-zero.
func FromSnapshot(id SnapshotID) (Source, error) {
	if id.IsZero() {
		return Source{}, fmt.Errorf("%w: snapshot id", ErrInvalidArgument)
	}
	return Source{id: id, ok: true}, nil
}

// Snapshot returns the source snapshot when one is set.
func (s Source) Snapshot() (SnapshotID, bool) {
	if !s.ok {
		return SnapshotID{}, false
	}
	return s.id, true
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
	fingerprint() string
	isRequest()
}

// CreateVolumeRequest creates a blank volume, or restores one when Source is set.
type CreateVolumeRequest struct{ Spec VolumeSpec }

// DeleteVolumeRequest deletes an available volume.
type DeleteVolumeRequest struct{ Volume VolumeID }

// CreateSnapshotRequest snapshots an available volume.
type CreateSnapshotRequest struct{ Source VolumeID }

// DeleteSnapshotRequest deletes an available snapshot.
type DeleteSnapshotRequest struct{ Snapshot SnapshotID }

// CreateBucketRequest creates a bucket.
type CreateBucketRequest struct{ Bucket BucketName }

// DeleteBucketRequest deletes an empty bucket.
type DeleteBucketRequest struct{ Bucket BucketName }

// PutObjectRequest declares an upload. The bytes go to the object backend.
type PutObjectRequest struct {
	Path        ObjectPath
	Size        Bytes
	SHA256      Digest
	ContentType string
}

// DeleteObjectRequest removes an object from reads and reclaims its bytes.
type DeleteObjectRequest struct{ Path ObjectPath }

// Validate implements Request.
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
	if len(r.ContentType) > 128 {
		return fmt.Errorf("%w: content type", ErrInvalidArgument)
	}
	for i := 0; i < len(r.ContentType); i++ {
		if r.ContentType[i] < 0x20 || r.ContentType[i] == 0x7f {
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
	src := "-"
	if id, ok := r.Spec.Source.Snapshot(); ok {
		src = id.String()
	}
	return fmt.Sprintf("create-volume\n%d\n%s\n%s\n%s", r.Spec.Size, r.Spec.Zone.String(), r.Spec.Type.String(), src)
}

func (r DeleteVolumeRequest) fingerprint() string {
	return "delete-volume\n" + r.Volume.String()
}

func (r CreateSnapshotRequest) fingerprint() string {
	return "create-snapshot\n" + r.Source.String()
}

func (r DeleteSnapshotRequest) fingerprint() string {
	return "delete-snapshot\n" + r.Snapshot.String()
}

func (r CreateBucketRequest) fingerprint() string {
	return "create-bucket\n" + r.Bucket.String()
}

func (r DeleteBucketRequest) fingerprint() string {
	return "delete-bucket\n" + r.Bucket.String()
}

func (r PutObjectRequest) fingerprint() string {
	return fmt.Sprintf("put-object\n%s\n%s\n%d\n%s\n%s", r.Path.Bucket.String(), r.Path.Key.String(), r.Size, r.SHA256.String(), r.ContentType)
}

func (r DeleteObjectRequest) fingerprint() string {
	return "delete-object\n" + r.Path.String()
}

func (CreateVolumeRequest) isRequest()   {}
func (DeleteVolumeRequest) isRequest()   {}
func (CreateSnapshotRequest) isRequest() {}
func (DeleteSnapshotRequest) isRequest() {}
func (CreateBucketRequest) isRequest()   {}
func (DeleteBucketRequest) isRequest()   {}
func (PutObjectRequest) isRequest()      {}
func (DeleteObjectRequest) isRequest()   {}

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
	Claim TokenClaim
	Op    OperationID
	Meta  Meta
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
