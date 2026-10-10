package model

import (
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"
)

// MaxDescription is the stored limit for a snapshot description.
// The proto does not set one.
const MaxDescription = 1024

// Labels is an immutable set of resource labels.
// The zero value is empty and comparable.
type Labels struct{ enc string }

// ParseLabels validates the resource.proto label grammar.
// A nil or empty map is success.
func ParseLabels(in map[string]string) (Labels, error) {
	if len(in) == 0 {
		return Labels{}, nil
	}
	if len(in) > 64 {
		return Labels{}, fmt.Errorf("%w: labels", ErrInvalidArgument)
	}
	keys := make([]string, 0, len(in))
	for k, v := range in {
		if !labelKey(k) || !labelValue(v) {
			return Labels{}, fmt.Errorf("%w: labels", ErrInvalidArgument)
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(in[k])
		b.WriteByte(0)
	}
	return Labels{enc: b.String()}, nil
}

// Map returns a copy of the labels. Empty is nil.
func (l Labels) Map() map[string]string {
	if l.enc == "" {
		return nil
	}
	out := make(map[string]string)
	for _, part := range strings.Split(l.enc, "\x00") {
		if part == "" {
			continue
		}
		k, v, _ := strings.Cut(part, "=")
		out[k] = v
	}
	return out
}

func (l Labels) canonical() string {
	if l.enc == "" {
		return "-"
	}
	return l.enc
}

func labelKey(s string) bool {
	if len(s) < 1 || len(s) > 63 || s[0] < 'a' || s[0] > 'z' {
		return false
	}
	for i := 1; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_' || c == '-' {
			continue
		}
		return false
	}
	return true
}

func labelValue(s string) bool {
	if len(s) > 63 || !utf8.ValidString(s) {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] == 0x7f {
			return false
		}
	}
	return true
}

// ParseDescription validates a snapshot description.
// Empty is success.
func ParseDescription(s string) (string, error) {
	if s == "" {
		return "", nil
	}
	if len(s) > MaxDescription || !utf8.ValidString(s) {
		return "", fmt.Errorf("%w: description", ErrInvalidArgument)
	}
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] == 0x7f {
			return "", fmt.Errorf("%w: description", ErrInvalidArgument)
		}
	}
	return s, nil
}
