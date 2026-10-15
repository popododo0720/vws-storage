// Package model is the storage control plane's vocabulary and state machines.
// It has no I/O. The store's only write is a Change, and only the transition
// functions in this package can mint one.
package model

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"unicode/utf8"
)

// Project is the first scope of a stored resource.
// The zero Project is the historical unscoped key, not a default the server applies.
type Project struct{ s string }

// ParseProject validates the VWS resource id grammar.
func ParseProject(s string) (Project, error) {
	if !resourceID(s) {
		return Project{}, fmt.Errorf("%w: project", ErrInvalidArgument)
	}
	return Project{s: s}, nil
}

// String returns the project id.
func (p Project) String() string { return p.s }

// IsZero reports whether p is unset.
func (p Project) IsZero() bool { return p.s == "" }

// Region is echoed metadata. It is not part of a store key.
type Region struct{ s string }

// ParseRegion validates the VWS resource id grammar.
func ParseRegion(s string) (Region, error) {
	if !resourceID(s) {
		return Region{}, fmt.Errorf("%w: region", ErrInvalidArgument)
	}
	return Region{s: s}, nil
}

// String returns the region id.
func (r Region) String() string { return r.s }

// IsZero reports whether r is unset.
func (r Region) IsZero() bool { return r.s == "" }

// ImageName is a compute image: projects/{project}/regions/{region}/images/{image}.
// Compute owns the image. Storage keeps the name as desired state and never
// reads it. ImageName is neither a Key nor a Ref: no store row is keyed by it
// and no operation holds it. A volume may only name an image of its own
// project (#89); there are no public images yet.
type ImageName struct{ s string }

// ParseImageName validates the full name. Every segment uses the resource id
// grammar. There is no short form, so ParseImageName(n.String()) returns n.
func ParseImageName(s string) (ImageName, error) {
	parts := strings.Split(s, "/")
	if len(parts) != 6 || parts[0] != "projects" || parts[2] != "regions" || parts[4] != "images" ||
		!resourceID(parts[1]) || !resourceID(parts[3]) || !resourceID(parts[5]) {
		return ImageName{}, fmt.Errorf("%w: image name", ErrInvalidArgument)
	}
	return ImageName{s: s}, nil
}

// String returns the full image name.
func (n ImageName) String() string { return n.s }

// IsZero reports whether n is unset.
func (n ImageName) IsZero() bool { return n.s == "" }

// Project returns the project segment. Zero for the zero name.
func (n ImageName) Project() Project { return Project{s: n.segment(1)} }

// Region returns the region segment. Zero for the zero name.
func (n ImageName) Region() Region { return Region{s: n.segment(3)} }

// ID returns the image id, the last segment. Empty for the zero name.
func (n ImageName) ID() string { return n.segment(5) }

func (n ImageName) segment(i int) string {
	if n.s == "" {
		return ""
	}
	return strings.Split(n.s, "/")[i]
}

func scopedName(p Project, collection, id string) string {
	if id == "" {
		return ""
	}
	if p.IsZero() {
		return collection + "/" + id
	}
	return "projects/" + p.s + "/" + collection + "/" + id
}

// splitScoped parses projects/{project}/{collection}/{id}.
// The id segment may contain anything except '/'.
func splitScoped(s, collection string) (Project, string, bool) {
	rest, ok := strings.CutPrefix(s, "projects/")
	if !ok {
		return Project{}, "", false
	}
	proj, rest, ok := strings.Cut(rest, "/")
	if !ok || !resourceID(proj) {
		return Project{}, "", false
	}
	col, id, ok := strings.Cut(rest, "/")
	if !ok || col != collection || id == "" || strings.Contains(id, "/") {
		return Project{}, "", false
	}
	return Project{s: proj}, id, true
}

// VolumeID is the final segment of a volume name.
// The grammar is a VWS resource id: a lowercase letter, then up to 62
// more lowercase letters, digits, or hyphens, ending with a letter or digit.
// NewVolumeID still mints the historical "vol-" form, which matches that grammar.
type VolumeID struct {
	project Project
	s       string
}

// NewVolumeID returns a new random volume id.
func NewVolumeID() VolumeID { return VolumeID{s: newPrefixed("vol-", 8)} }

// ParseVolumeID validates s. s is the id, "volumes/" plus the id, or
// "projects/{project}/volumes/{id}".
func ParseVolumeID(s string) (VolumeID, error) {
	if p, id, ok := splitScoped(s, "volumes"); ok {
		if !resourceID(id) {
			return VolumeID{}, fmt.Errorf("%w: volume id", ErrInvalidArgument)
		}
		return VolumeID{project: p, s: id}, nil
	}
	s = strings.TrimPrefix(s, "volumes/")
	if !resourceID(s) {
		return VolumeID{}, fmt.Errorf("%w: volume id", ErrInvalidArgument)
	}
	return VolumeID{s: s}, nil
}

// WithProject returns id scoped to p. A zero id stays zero.
func (id VolumeID) WithProject(p Project) VolumeID {
	id.project = p
	return id
}

// Project returns the stored project. Zero means the historical unscoped key.
func (id VolumeID) Project() Project { return id.project }

// String returns the canonical store key.
func (id VolumeID) String() string { return scopedName(id.project, "volumes", id.s) }

// wire is the codec form. Unscoped ids stay the raw segment so old payloads load.
func (id VolumeID) wire() string {
	if id.s == "" || id.project.IsZero() {
		return id.s
	}
	return id.String()
}

// ID returns the raw id.
func (id VolumeID) ID() string { return id.s }

// IsZero reports whether id is unset.
func (id VolumeID) IsZero() bool { return id.s == "" }

// SnapshotID is the final segment of a snapshot name. See VolumeID.
// NewSnapshotID still mints the historical "snap-" form.
type SnapshotID struct {
	project Project
	s       string
}

// NewSnapshotID returns a new random snapshot id.
func NewSnapshotID() SnapshotID { return SnapshotID{s: newPrefixed("snap-", 8)} }

// ParseSnapshotID validates s. s is the id, "snapshots/" plus the id, or
// "projects/{project}/snapshots/{id}".
func ParseSnapshotID(s string) (SnapshotID, error) {
	if p, id, ok := splitScoped(s, "snapshots"); ok {
		if !resourceID(id) {
			return SnapshotID{}, fmt.Errorf("%w: snapshot id", ErrInvalidArgument)
		}
		return SnapshotID{project: p, s: id}, nil
	}
	s = strings.TrimPrefix(s, "snapshots/")
	if !resourceID(s) {
		return SnapshotID{}, fmt.Errorf("%w: snapshot id", ErrInvalidArgument)
	}
	return SnapshotID{s: s}, nil
}

// WithProject returns id scoped to p.
func (id SnapshotID) WithProject(p Project) SnapshotID {
	id.project = p
	return id
}

// Project returns the stored project.
func (id SnapshotID) Project() Project { return id.project }

// String returns the canonical store key.
func (id SnapshotID) String() string { return scopedName(id.project, "snapshots", id.s) }

func (id SnapshotID) wire() string {
	if id.s == "" || id.project.IsZero() {
		return id.s
	}
	return id.String()
}

// ID returns the raw id.
func (id SnapshotID) ID() string { return id.s }

// IsZero reports whether id is unset.
func (id SnapshotID) IsZero() bool { return id.s == "" }

// OperationID is "op-" plus 20 lowercase hex digits.
type OperationID struct {
	project Project
	s       string
}

// NewOperationID returns a new random operation id.
func NewOperationID() OperationID { return OperationID{s: newPrefixed("op-", 10)} }

// ParseOperationID validates s. s is the id, "operations/" plus the id, or
// "projects/{project}/operations/{id}".
func ParseOperationID(s string) (OperationID, error) {
	if p, id, ok := splitScoped(s, "operations"); ok {
		if !isHexID(id, "op-", 10) {
			return OperationID{}, fmt.Errorf("%w: operation id", ErrInvalidArgument)
		}
		return OperationID{project: p, s: id}, nil
	}
	s = strings.TrimPrefix(s, "operations/")
	if !isHexID(s, "op-", 10) {
		return OperationID{}, fmt.Errorf("%w: operation id", ErrInvalidArgument)
	}
	return OperationID{s: s}, nil
}

// WithProject returns id scoped to p.
func (id OperationID) WithProject(p Project) OperationID {
	id.project = p
	return id
}

// Project returns the stored project.
func (id OperationID) Project() Project { return id.project }

// String returns the canonical store key.
func (id OperationID) String() string { return scopedName(id.project, "operations", id.s) }

func (id OperationID) wire() string {
	if id.s == "" || id.project.IsZero() {
		return id.s
	}
	return id.String()
}

// ID returns the raw id.
func (id OperationID) ID() string { return id.s }

// IsZero reports whether id is unset.
func (id OperationID) IsZero() bool { return id.s == "" }

// Generation names one immutable upload of an object.
type Generation struct{ s string }

// NewGeneration returns a new random generation.
func NewGeneration() Generation { return Generation{s: hex.EncodeToString(mustRand(8))} }

// ParseGeneration validates s.
func ParseGeneration(s string) (Generation, error) {
	if len(s) != 16 || !isLowerHex(s) {
		return Generation{}, fmt.Errorf("%w: generation", ErrInvalidArgument)
	}
	return Generation{s: s}, nil
}

// String returns the hex generation.
func (g Generation) String() string { return g.s }

// IsZero reports whether g is unset.
func (g Generation) IsZero() bool { return g.s == "" }

// BucketName is the final segment of a bucket name. The grammar is a VWS resource id.
type BucketName struct {
	project Project
	s       string
}

// ParseBucketName validates s. s may be the raw name, "buckets/" plus the name,
// or "projects/{project}/buckets/{name}".
func ParseBucketName(s string) (BucketName, error) {
	if p, name, ok := splitScoped(s, "buckets"); ok {
		if !resourceID(name) {
			return BucketName{}, fmt.Errorf("%w: bucket name", ErrInvalidArgument)
		}
		return BucketName{project: p, s: name}, nil
	}
	s = strings.TrimPrefix(s, "buckets/")
	if !resourceID(s) {
		return BucketName{}, fmt.Errorf("%w: bucket name", ErrInvalidArgument)
	}
	return BucketName{s: s}, nil
}

// WithProject returns b scoped to p.
func (b BucketName) WithProject(p Project) BucketName {
	b.project = p
	return b
}

// Project returns the stored project.
func (b BucketName) Project() Project { return b.project }

// String returns the canonical store key.
func (b BucketName) String() string { return scopedName(b.project, "buckets", b.s) }

func (b BucketName) wire() string {
	if b.s == "" || b.project.IsZero() {
		return b.s
	}
	return b.String()
}

// Name returns the raw bucket name.
func (b BucketName) Name() string { return b.s }

// IsZero reports whether b is unset.
func (b BucketName) IsZero() bool { return b.s == "" }

// ObjectKey is a UTF-8 object key of 1 to 1024 bytes.
type ObjectKey struct{ s string }

// ParseObjectKey validates s.
func ParseObjectKey(s string) (ObjectKey, error) {
	if len(s) == 0 || len(s) > 1024 || !utf8.ValidString(s) {
		return ObjectKey{}, fmt.Errorf("%w: object key", ErrInvalidArgument)
	}
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] == 0x7f {
			return ObjectKey{}, fmt.Errorf("%w: object key", ErrInvalidArgument)
		}
	}
	return ObjectKey{s: s}, nil
}

// String returns the raw key.
func (k ObjectKey) String() string { return k.s }

// IsZero reports whether k is unset.
func (k ObjectKey) IsZero() bool { return k.s == "" }

// Zone is an availability zone name. Existence is deployment config.
// The grammar is a VWS resource id.
type Zone struct{ s string }

// ParseZone validates the syntax of s.
func ParseZone(s string) (Zone, error) {
	if !resourceID(s) {
		return Zone{}, fmt.Errorf("%w: zone", ErrInvalidArgument)
	}
	return Zone{s: s}, nil
}

// String returns the zone name.
func (z Zone) String() string { return z.s }

// IsZero reports whether z is unset.
func (z Zone) IsZero() bool { return z.s == "" }

// ClientToken is the idempotency key on every mutation.
type ClientToken struct{ s string }

// NewClientToken mints a token for a call that sent no request_id.
// It is not derived from the request, so a later identical call is a new call.
func NewClientToken() ClientToken {
	return ClientToken{s: "auto-" + hex.EncodeToString(mustRand(16))}
}

// ParseClientToken validates s. The empty string is rejected.
func ParseClientToken(s string) (ClientToken, error) {
	if len(s) == 0 || len(s) > 64 {
		return ClientToken{}, fmt.Errorf("%w: client token", ErrInvalidArgument)
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		ok := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == ':' || c == '-'
		if !ok {
			return ClientToken{}, fmt.Errorf("%w: client token", ErrInvalidArgument)
		}
	}
	return ClientToken{s: s}, nil
}

// String returns the token.
func (t ClientToken) String() string { return t.s }

// IsZero reports whether t is unset.
func (t ClientToken) IsZero() bool { return t.s == "" }

// TokenKey is the token row. A zero project keeps the raw token, which is
// the key a current database already stores.
type TokenKey struct {
	Project Project
	Token   ClientToken
}

// String returns the store key.
func (k TokenKey) String() string {
	if k.Token.IsZero() {
		return ""
	}
	if k.Project.IsZero() {
		return k.Token.String()
	}
	return "projects/" + k.Project.String() + "/tokens/" + k.Token.String()
}

func (TokenKey) isKey() {}

// ObjectPath is the client-visible address of an object.
type ObjectPath struct {
	Bucket BucketName
	Key    ObjectKey
}

// Project returns the bucket's project.
func (p ObjectPath) Project() Project { return p.Bucket.Project() }

// String returns a canonical name. Keys cannot contain control characters,
// so the tab separators do not collide with a key.
func (p ObjectPath) String() string {
	if p.Bucket.IsZero() && p.Key.IsZero() {
		return ""
	}
	body := "object\t" + p.Bucket.Name() + "\t" + p.Key.String()
	if p.Bucket.Project().IsZero() {
		return body
	}
	return "projects/" + p.Bucket.Project().String() + "/" + body
}

// ObjectRef is one generation of an object's bytes in the backend.
type ObjectRef struct {
	Path       ObjectPath
	Generation Generation
}

// String returns a canonical name for this generation.
func (r ObjectRef) String() string {
	if r.Generation.IsZero() && r.Path.String() == "" {
		return ""
	}
	return r.Path.String() + "\t" + r.Generation.String()
}

func newPrefixed(prefix string, n int) string {
	return prefix + hex.EncodeToString(mustRand(n))
}

func mustRand(n int) []byte {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return b
}

// resourceID is the VWS resource id grammar: [a-z]([a-z0-9-]{0,61}[a-z0-9])?
func resourceID(s string) bool {
	if len(s) < 1 || len(s) > 63 || s[0] < 'a' || s[0] > 'z' {
		return false
	}
	if len(s) == 1 {
		return true
	}
	last := s[len(s)-1]
	if !isAlnum(last) {
		return false
	}
	for i := 1; i < len(s)-1; i++ {
		if c := s[i]; !isAlnum(c) && c != '-' {
			return false
		}
	}
	return true
}

func isAlnum(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= '0' && c <= '9'
}

func isHexID(s, prefix string, n int) bool {
	if !strings.HasPrefix(s, prefix) || len(s) != len(prefix)+n*2 {
		return false
	}
	return isLowerHex(s[len(prefix):])
}

func isLowerHex(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
