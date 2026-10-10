package policycheck

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestRepoPolicy(t *testing.T) {
	root := repoRoot(t)
	var protos int
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			base := d.Name()
			if base == ".git" || base == "vendor" || base == "third_party" {
				return filepath.SkipDir
			}
			switch base {
			case "deploy", "k8s", "kubernetes", "helm", "charts":
				t.Errorf("deploy tree %s", rel(t, root, path))
			}
			return nil
		}
		name := d.Name()
		relPath := rel(t, root, path)
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
			protos++
			body, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if !strings.Contains(string(body), "TODO(vws): move to the shared VWS proto repository") {
				t.Errorf("proto missing shared-repo TODO: %s", relPath)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if protos == 0 {
		t.Fatal("no proto files")
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
