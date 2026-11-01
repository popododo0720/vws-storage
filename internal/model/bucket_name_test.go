package model

import (
	"errors"
	"strings"
	"testing"
)

// S3 bucket names may contain periods (#160). These cases follow the S3
// general purpose bucket naming rules.
func TestParseBucketNameAcceptsDottedNames(t *testing.T) {
	for _, name := range []string{
		"example.com",
		"www.example.com",
		"my.bucket-1.logs",
		"a.b",
		"1.example",
		"example.c0m",
		"192.168.5.4.example",
		"10.0.0",
		strings.Repeat("a.", 31) + "b", // 63 characters
	} {
		b, err := ParseBucketName(name)
		if err != nil {
			t.Errorf("ParseBucketName(%q) = %v, want nil", name, err)
			continue
		}
		if b.Name() != name {
			t.Errorf("ParseBucketName(%q).Name() = %q", name, b.Name())
		}
		scoped, err := ParseBucketName("projects/p1/buckets/" + name)
		if err != nil || scoped.Name() != name || scoped.Project().String() != "p1" {
			t.Errorf("ParseBucketName(scoped %q) = %v, %v", name, scoped, err)
		}
		if again, err := ParseBucketName(scoped.String()); err != nil || again != scoped {
			t.Errorf("round trip of %q = %v, %v", scoped.String(), again, err)
		}
	}
}

func TestParseBucketNameRefusesBadDottedNames(t *testing.T) {
	for _, name := range []string{
		".example.com",                 // leading period
		"example.com.",                 // trailing period
		"example..com",                 // doubled period
		"-example.com",                 // leading hyphen
		"example.com-",                 // trailing hyphen
		"192.168.5.4",                  // IP shaped
		"0.0.0.0",                      // IP shaped
		"999.999.999.999",              // IP shaped, out of range
		"a.",                           // too short and trailing period
		"Example.com",                  // uppercase
		"example_com.net",              // underscore
		"example.com/x",                // slash
		strings.Repeat("a.", 32) + "b", // 65 characters
	} {
		if _, err := ParseBucketName(name); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("ParseBucketName(%q) = %v, want ErrInvalidArgument", name, err)
		}
		if _, err := ParseBucketName("projects/p1/buckets/" + name); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("ParseBucketName(scoped %q) = %v, want ErrInvalidArgument", name, err)
		}
	}
}

// Names valid before #160 must still parse, so stored records decode unchanged.
func TestParseBucketNameKeepsResourceIDNames(t *testing.T) {
	for _, name := range []string{"a", "ab", "logs", "b-1", strings.Repeat("a", 63)} {
		if _, err := ParseBucketName(name); err != nil {
			t.Errorf("ParseBucketName(%q) = %v, want nil", name, err)
		}
	}
}

// Only buckets take periods. Other resource ids keep the resource id grammar.
func TestDottedNamesStayInvalidForOtherIDs(t *testing.T) {
	const dotted = "example.com"
	checks := map[string]func(string) error{
		"project": func(s string) error { _, err := ParseProject(s); return err },
		"region":  func(s string) error { _, err := ParseRegion(s); return err },
		"bucket project": func(s string) error {
			_, err := ParseBucketName("projects/" + s + "/buckets/logs")
			return err
		},
	}
	for what, parse := range checks {
		if err := parse(dotted); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%s %q = %v, want ErrInvalidArgument", what, dotted, err)
		}
	}
}
