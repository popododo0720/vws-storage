package policycheck

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

func TestRepoPolicy(t *testing.T) {
	root := repoRoot(t)
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relPath := rel(t, root, path)
		if d.IsDir() {
			base := d.Name()
			if base == ".git" || base == "vendor" {
				return filepath.SkipDir
			}
			if relPath == "third_party/vws-proto" {
				t.Errorf("vendored vws-proto copy %s; require a vws-proto tag in go.mod instead", relPath)
				return filepath.SkipDir
			}
			if strings.HasPrefix(relPath, "third_party/") || relPath == "third_party" {
				return nil
			}
			switch base {
			case "deploy", "k8s", "kubernetes", "helm", "charts":
				t.Errorf("deploy tree %s", relPath)
			}
			return nil
		}
		name := d.Name()
		if strings.HasSuffix(name, ".pb.go") {
			t.Errorf("generated protobuf %s; import it from the vws-proto module", relPath)
		}
		if strings.HasPrefix(relPath, "third_party/") {
			return nil
		}
		lower := strings.ToLower(name)
		switch {
		case isContainer(name):
			t.Errorf("container file %s", relPath)
		case name == "Chart.yaml" || name == "Chart.lock":
			t.Errorf("helm file %s", relPath)
		case name == "kustomization.yaml" || name == "kustomization.yml":
			t.Errorf("kustomize file %s", relPath)
		case name == "NOTICE" || strings.HasPrefix(name, "NOTICE."):
			t.Errorf("notice file %s", relPath)
		case isLogo(lower):
			t.Errorf("logo file %s", relPath)
		case strings.HasSuffix(name, ".proto"):
			t.Errorf("proto file %s; this repo keeps no proto", relPath)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	mod, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range protoModuleRules(string(mod)) {
		t.Error(f)
	}
	readme, err := os.ReadFile(filepath.Join(root, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	const note = "Never copy or `replace` the module"
	if !strings.Contains(string(readme), note) {
		t.Error("README missing the vws-proto module note")
	}
	if _, err := os.Stat(filepath.Join(root, "proto")); !os.IsNotExist(err) {
		t.Error("proto tree still exists")
	}
	if _, err := os.Stat(filepath.Join(root, "internal", "gen")); !os.IsNotExist(err) {
		t.Error("internal/gen still exists")
	}
}

// protoModulePath is the resource API module. It is consumed from a release tag through the module proxy.
const protoModulePath = "github.com/popododo0720/vws-proto"

// protoRequire matches the require line for protoModulePath at a release tag (no pseudo-version, no pre-release).
var protoRequire = regexp.MustCompile(
	`^(require\s+)?` + regexp.QuoteMeta(protoModulePath) + `\s+v[0-9]+\.[0-9]+\.[0-9]+$`,
)

// protoModuleRules returns the go.mod problems for the resource API module: a replace directive
// (a local copy), or a require that is missing or not a release tag.
func protoModuleRules(mod string) []string {
	var failures []string
	tagged := false
	for _, raw := range strings.Split(mod, "\n") {
		line := strings.TrimSpace(raw)
		if i := strings.Index(line, "//"); i >= 0 {
			line = strings.TrimSpace(line[:i])
		}
		if strings.Contains(line, "=>") && strings.Contains(line, protoModulePath) {
			failures = append(failures, "go.mod replaces "+protoModulePath+" ("+line+"); require a release tag instead")
		}
		if protoRequire.MatchString(line) {
			tagged = true
		}
	}
	if !tagged {
		failures = append(failures, "go.mod does not require "+protoModulePath+" at a release tag vX.Y.Z")
	}
	return failures
}

func TestProtoModuleRules(t *testing.T) {
	const tagged = "require github.com/popododo0720/vws-proto v0.5.0\n"
	cases := []struct {
		name string
		mod  string
		bad  bool
	}{
		{"tag in a require block", "require (\n\tgithub.com/popododo0720/vws-proto v0.5.0\n)\n", false},
		{"single-line require", tagged, false},
		{"tag with indirect comment", "require (\n\tgithub.com/popododo0720/vws-proto v0.5.0 // indirect\n)\n", false},
		{"replace to a local copy", tagged + "replace github.com/popododo0720/vws-proto => ./third_party/vws-proto\n", true},
		{"replace in a block", tagged + "replace (\n\tgithub.com/popododo0720/vws-proto => ../vws-proto\n)\n", true},
		{"pseudo-version", "require github.com/popododo0720/vws-proto v0.4.1-0.20261021123535-8f0a84ed9918\n", true},
		{"placeholder version", "require github.com/popododo0720/vws-proto v0.0.0-00010101000000-000000000000\n", true},
		{"pre-release", "require github.com/popododo0720/vws-proto v0.6.0-rc.1\n", true},
		{"missing", "require google.golang.org/grpc v1.84.0\n", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := protoModuleRules(tc.mod)
			if (len(got) > 0) != tc.bad {
				t.Fatalf("protoModuleRules(%q) = %q, want failure %v", tc.mod, got, tc.bad)
			}
		})
	}
}

func isContainer(name string) bool {
	switch name {
	case "Dockerfile", "Containerfile", "docker-compose.yml", "docker-compose.yaml", "compose.yml", "compose.yaml":
		return true
	default:
		return strings.HasPrefix(name, "Dockerfile.")
	}
}

func isLogo(lower string) bool {
	return lower == "logo" || strings.HasPrefix(lower, "logo.") || strings.HasSuffix(lower, ".logo")
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("caller")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func rel(t *testing.T, root, path string) string {
	t.Helper()
	r, err := filepath.Rel(root, path)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
