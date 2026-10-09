// Package model is the storage control plane's vocabulary and state machines.
// It has no I/O. The store's only write is a Change, and only the transition
// functions in this package can mint one.
package model

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"strings"
	"unicode/utf8"
)

// VolumeID is "vol-" plus 16 lowercase hex digits.
type VolumeID struct{ s string }

// NewVolumeID returns a new random volume id.
func NewVolumeID() VolumeID { return VolumeID{s: newPrefixed("vol-", 8)} }

// ParseVolumeID validates s. s is the id, without a "volumes/" prefix.
func ParseVolumeID(s string) (VolumeID, error) {
	const p = "volumes/"
	s = strings.TrimPrefix(s, p)
	if !isHexID(s, "vol-", 8) {
		return VolumeID{}, fmt.Errorf("%w: volume id", ErrInvalidArgument)
	}
	return VolumeID{s: s}, nil
}

// String returns the canonical resource name.
func (id VolumeID) String() string {
	if id.s == "" {
		return ""
	}
	return "volumes/" + id.s
}

// ID returns the raw id.
func (id VolumeID) ID() string { return id.s }

// IsZero reports whether id is unset.
func (id VolumeID) IsZero() bool { return id.s == "" }

// SnapshotID is "snap-" plus 16 lowercase hex digits.
type SnapshotID struct{ s string }

// NewSnapshotID returns a new random snapshot id.
func NewSnapshotID() SnapshotID { return SnapshotID{s: newPrefixed("snap-", 8)} }

// ParseSnapshotID validates s.
func ParseSnapshotID(s string) (SnapshotID, error) {
	s = strings.TrimPrefix(s, "snapshots/")
	if !isHexID(s, "snap-", 8) {
		return SnapshotID{}, fmt.Errorf("%w: snapshot id", ErrInvalidArgument)
	}
	return SnapshotID{s: s}, nil
}

// String returns the canonical resource name.
func (id SnapshotID) String() string {
	if id.s == "" {
		return ""
	}
	return "snapshots/" + id.s
}

// ID returns the raw id.
func (id SnapshotID) ID() string { return id.s }

// IsZero reports whether id is unset.
func (id SnapshotID) IsZero() bool { return id.s == "" }

// OperationID is "op-" plus 20 lowercase hex digits.
type OperationID struct{ s string }

// NewOperationID returns a new random operation id.
func NewOperationID() OperationID { return OperationID{s: newPrefixed("op-", 10)} }

// ParseOperationID validates s.
func ParseOperationID(s string) (OperationID, error) {
	s = strings.TrimPrefix(s, "operations/")
	if !isHexID(s, "op-", 10) {
		return OperationID{}, fmt.Errorf("%w: operation id", ErrInvalidArgument)
	}
	return OperationID{s: s}, nil
}

// String returns the canonical resource name.
func (id OperationID) String() string {
	if id.s == "" {
		return ""
	}
	return "operations/" + id.s
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

// BucketName is a DNS-style bucket name.
type BucketName struct{ s string }

// ParseBucketName validates s.
func ParseBucketName(s string) (BucketName, error) {
	if len(s) < 3 || len(s) > 63 || net.ParseIP(s) != nil || strings.Contains(s, "..") {
		return BucketName{}, fmt.Errorf("%w: bucket name", ErrInvalidArgument)
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		ok := c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '.'
		if !ok || (c == '-' || c == '.') && (i == 0 || i == len(s)-1) {
			return BucketName{}, fmt.Errorf("%w: bucket name", ErrInvalidArgument)
		}
	}
	return BucketName{s: s}, nil
}

// String returns the canonical resource name.
func (b BucketName) String() string {
	if b.s == "" {
		return ""
	}
	return "buckets/" + b.s
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
type Zone struct{ s string }

// ParseZone validates the syntax of s.
func ParseZone(s string) (Zone, error) {
	if len(s) == 0 || len(s) > 32 {
		return Zone{}, fmt.Errorf("%w: zone", ErrInvalidArgument)
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') || (c == '-' && (i == 0 || i == len(s)-1)) {
			return Zone{}, fmt.Errorf("%w: zone", ErrInvalidArgument)
		}
	}
	return Zone{s: s}, nil
}

// String returns the zone name.
func (z Zone) String() string { return z.s }

// IsZero reports whether z is unset.
func (z Zone) IsZero() bool { return z.s == "" }

// ClientToken is the idempotency key on every mutation.
type ClientToken struct{ s string }

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

// ObjectPath is the client-visible address of an object.
type ObjectPath struct {
	Bucket BucketName
	Key    ObjectKey
}

// String returns a canonical name. Keys cannot contain control characters,
// so the tab separators do not collide with a key.
func (p ObjectPath) String() string {
	if p.Bucket.IsZero() && p.Key.IsZero() {
		return ""
	}
	return "object\t" + p.Bucket.Name() + "\t" + p.Key.String()
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

func isHexID(s, prefix string, n int) bool {
	if !strings.HasPrefix(s, prefix) || len(s) != len(prefix)+n*2 {
		return false
	}
	return isLowerHex(s[len(prefix):])
}

func isLowerHex(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
