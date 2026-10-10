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
			if strings.HasPrefix(relPath, "third_party/") || relPath == "third_party" {
				return nil
			}
			switch base {
			case "deploy", "k8s", "kubernetes", "helm", "charts":
				t.Errorf("deploy tree %s", relPath)
			}
			return nil
		}
		if strings.HasPrefix(relPath, "third_party/") {
			return nil
		}
		name := d.Name()
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
			t.Errorf("proto outside third_party/vws-proto: %s", relPath)
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
	if !strings.Contains(string(mod), "replace github.com/popododo0720/vws-proto => ./third_party/vws-proto") {
		t.Error("go.mod missing vws-proto replace")
	}
	readme, err := os.ReadFile(filepath.Join(root, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	const note = "vws-proto가 공개되면 replace를 제거하고 태그 버전을 require한다"
	if !strings.Contains(string(readme), note) {
		t.Error("README missing public-module note")
	}
	if _, err := os.Stat(filepath.Join(root, "proto")); !os.IsNotExist(err) {
		t.Error("proto tree still exists")
	}
	if _, err := os.Stat(filepath.Join(root, "internal", "gen")); !os.IsNotExist(err) {
		t.Error("internal/gen still exists")
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
