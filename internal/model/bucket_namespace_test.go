package model_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/popododo0720/vws-storage/internal/model"
)

func nsRequest(t *testing.T, project, region, name string, ns model.BucketNamespace) model.CreateBucketRequest {
	t.Helper()
	p, err := model.ParseProject(project)
	if err != nil {
		t.Fatal(err)
	}
	b, err := model.ParseBucketName(name)
	if err != nil {
		t.Fatal(err)
	}
	var r model.Region
	if region != "" {
		if r, err = model.ParseRegion(region); err != nil {
			t.Fatal(err)
		}
	}
	return model.CreateBucketRequest{Bucket: b.WithProject(p), Region: r, Namespace: ns}
}

// The S3 namespace naming rules (S3 User Guide "Namespaces for general
// purpose buckets" and "General purpose bucket naming rules").
func TestCreateBucketNamespaceRules(t *testing.T) {
	const own = "a111122223333"
	ar, global := model.BucketNamespaceAccountRegional, model.BucketNamespaceGlobal
	for _, tc := range []struct {
		name, project, region, bucket string
		ns                            model.BucketNamespace
		wantErr                       string
	}{
		{name: "own suffix", project: own, region: "us-west-2", bucket: "logs-111122223333-us-west-2-an", ns: ar},
		{name: "prefix with digits", project: own, region: "kr-1", bucket: "x-999999999999-111122223333-kr-1-an", ns: ar},
		{
			name: "another account's suffix", project: own, region: "us-west-2",
			bucket: "logs-999999999999-us-west-2-an", ns: ar,
			wantErr: `the requested account id "999999999999" does not match the caller's account id "111122223333"`,
		},
		{
			name: "another region's suffix", project: own, region: "us-west-2", bucket: "logs-111122223333-us-east-1-an", ns: ar,
			wantErr: `must be a prefix followed by "-111122223333-us-west-2-an"`,
		},
		{
			name: "no suffix", project: own, region: "us-west-2", bucket: "logs", ns: ar,
			wantErr: `must be a prefix followed by`,
		},
		{
			name: "suffix only", project: own, region: "r1", bucket: "111122223333-r1-an", ns: ar,
			wantErr: `must be a prefix followed by`,
		},
		{
			name: "no region", project: own, bucket: "logs-111122223333-r1-an", ns: ar,
			wantErr: "needs a region",
		},
		{
			name: "project without account id", project: "team", region: "r1", bucket: "logs-111122223333-r1-an", ns: ar,
			wantErr: "has no 12-digit account id",
		},
		{name: "global plain", project: own, region: "r1", bucket: "logs", ns: global},
		{
			name: "global ending in -an", project: own, region: "r1", bucket: "logs-111122223333-r1-an", ns: global,
			wantErr: `reserved for the account-regional namespace`,
		},
		{
			name: "global short -an", project: own, bucket: "xyz-an", ns: global,
			wantErr: `reserved for the account-regional namespace`,
		},
		{name: "global -an inside the name", project: own, bucket: "an-logs-and", ns: global},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := nsRequest(t, tc.project, tc.region, tc.bucket, tc.ns).CheckNamespace()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("CheckNamespace() = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, model.ErrInvalidBucketNamespace) || !errors.Is(err, model.ErrInvalidArgument) {
				t.Fatalf("CheckNamespace() = %v, want ErrInvalidBucketNamespace", err)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("CheckNamespace() = %q, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestInvalidBucketNamespaceIsItsOwnSentinel(t *testing.T) {
	if errors.Is(model.ErrUnsupportedVolumeType, model.ErrInvalidBucketNamespace) ||
		errors.Is(model.ErrInvalidBucketNamespace, model.ErrUnsupportedVolumeType) {
		t.Fatal("ErrInvalidBucketNamespace and ErrUnsupportedVolumeType must not match each other")
	}
	if got := model.ErrInvalidBucketNamespace.Error(); got != model.ErrInvalidArgument.Error() {
		t.Fatalf("Error() = %q, want it to print as ErrInvalidArgument", got)
	}
}

func TestParseBucketNamespace(t *testing.T) {
	for in, want := range map[string]model.BucketNamespace{
		"":                 model.BucketNamespaceGlobal,
		"global":           model.BucketNamespaceGlobal,
		"account-regional": model.BucketNamespaceAccountRegional,
	} {
		got, err := model.ParseBucketNamespace(in)
		if err != nil || got != want {
			t.Fatalf("ParseBucketNamespace(%q) = %v, %v; want %v", in, got, err, want)
		}
		if in != "" && got.String() != in {
			t.Fatalf("String() = %q, want %q", got.String(), in)
		}
	}
	for _, in := range []string{"Global", "account_regional", "regional"} {
		if _, err := model.ParseBucketNamespace(in); !errors.Is(err, model.ErrInvalidArgument) {
			t.Fatalf("ParseBucketNamespace(%q) = %v, want ErrInvalidArgument", in, err)
		}
	}
}

// A global bucket row stays byte-identical to the pre-namespace encoding, an
// account-regional row round-trips its namespace, and an unknown stored
// namespace fails to decode instead of reading as global.
func TestBucketNamespaceCodec(t *testing.T) {
	req := nsRequest(t, "a111122223333", "r1", "logs-111122223333-r1-an", model.BucketNamespaceAccountRegional)
	for _, tc := range []struct {
		ns   model.BucketNamespace
		want string
	}{
		{model.BucketNamespaceGlobal, ""},
		{model.BucketNamespaceAccountRegional, `"namespace":"account-regional"`},
	} {
		b := model.Bucket{Name: req.Bucket, Region: req.Region, Namespace: tc.ns, State: model.Available{}}
		raw, err := model.MarshalRecord(b)
		if err != nil {
			t.Fatal(err)
		}
		if tc.want == "" && strings.Contains(string(raw), "namespace") {
			t.Fatalf("global row %s carries a namespace", raw)
		}
		if tc.want != "" && !strings.Contains(string(raw), tc.want) {
			t.Fatalf("row %s, want %s", raw, tc.want)
		}
		got, err := model.UnmarshalRecord(raw)
		if err != nil {
			t.Fatal(err)
		}
		if got.(model.Bucket).Namespace != tc.ns {
			t.Fatalf("decoded namespace %v, want %v", got.(model.Bucket).Namespace, tc.ns)
		}
	}
	b := model.Bucket{
		Name: req.Bucket, Region: req.Region, Namespace: model.BucketNamespaceAccountRegional, State: model.Available{},
	}
	raw, err := model.MarshalRecord(b)
	if err != nil {
		t.Fatal(err)
	}
	bad := strings.Replace(string(raw), `"account-regional"`, `"zonal"`, 1)
	if _, err := model.UnmarshalRecord([]byte(bad)); err == nil {
		t.Fatal("decoding an unknown namespace succeeded")
	}
}
