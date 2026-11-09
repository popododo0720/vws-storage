package policycheck

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// k8sVersion is the exact version setup-envtest use accepts: X.Y.Z, no leading v.
var k8sVersion = regexp.MustCompile(`^\d+\.\d+\.\d+$`)

// envtestPinRules returns the commands of a workflow that install setup-envtest at anything but a release
// tag, or run `setup-envtest ... use` without an exact Kubernetes version among the words after `use`, so
// an upstream release cannot move CI (#211). It reports whether some command runs `use`.
func envtestPinRules(name, workflow string) (failures []string, uses bool) {
	lines := strings.Split(workflow, "\n")
	for i := 0; i < len(lines); i++ {
		where := name + ":" + strconv.Itoa(i+1)
		line := lines[i]
		for strings.HasSuffix(line, "\\") && i+1 < len(lines) {
			i++
			line = strings.TrimSuffix(line, "\\") + " " + lines[i]
		}
		if !strings.Contains(line, "setup-envtest") {
			continue
		}
		words := strings.FieldsFunc(line, func(r rune) bool {
			return r == ' ' || r == '\t' || r == '"' || r == '\'' || r == '(' || r == ')' || r == '`'
		})
		tool, use := false, false
		pinned := false
		for _, w := range words {
			switch {
			case strings.Contains(w, "setup-envtest@"):
				tool = true
				if v := w[strings.Index(w, "@")+1:]; !isReleaseTag(v) {
					failures = append(failures, where+": setup-envtest@"+v+"; pin a release tag vX.Y.Z")
				}
			case strings.HasSuffix(w, "setup-envtest"):
				tool = true
			case tool && w == "use":
				use = true
			case use && k8sVersion.MatchString(w):
				pinned = true
			}
		}
		if use {
			uses = true
			if !pinned {
				failures = append(failures, where+": setup-envtest use without an exact Kubernetes version X.Y.Z")
			}
		}
	}
	return failures, uses
}

func TestEnvtestIsPinned(t *testing.T) {
	root := repoRoot(t)
	var files []string
	for _, ext := range []string{"*.yml", "*.yaml"} {
		m, err := filepath.Glob(filepath.Join(root, ".github", "workflows", ext))
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, m...)
	}
	used := false
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		failures, uses := envtestPinRules(rel(t, root, f), string(b))
		for _, failure := range failures {
			t.Error(failure)
		}
		used = used || uses
	}
	if !used {
		t.Error("no workflow runs setup-envtest use; the envtest tests would skip in CI")
	}
}

func TestEnvtestPinRules(t *testing.T) {
	const tool = "sigs.k8s.io/controller-runtime/tools/setup-envtest"
	for _, tc := range []struct {
		line string
		bad  bool
	}{
		{"go install " + tool + "@v0.25.2", false},
		{`go install "` + tool + `@v0.25.2"`, false},
		{`echo "KUBEBUILDER_ASSETS=$(setup-envtest use 1.37.0 -p path)"`, false},
		{`echo "KUBEBUILDER_ASSETS=$(setup-envtest use 1.37.0)"`, false},
		{"setup-envtest use -p path 1.37.0", false},
		{"setup-envtest -i use 1.37.0 -p path", false},
		{"go run " + tool + "@v0.25.2 use 1.37.0 -p path", false},
		{"setup-envtest \\\n  use 1.37.0 -p path", false},
		{"- run: go test ./...", false},
		{"go install " + tool + "@latest", true},
		{"go install " + tool + "@release-0.25", true},
		{"go install " + tool + "@v0.0.0-20260101000000-abcdefabcdef", true},
		{"go install " + tool + "@", true},
		{`echo "KUBEBUILDER_ASSETS=$(setup-envtest use -p path)"`, true},
		{`echo "KUBEBUILDER_ASSETS=$(setup-envtest use)"`, true},
		{"setup-envtest use 1.37 -p path", true},
		{"setup-envtest use latest -p path", true},
		{"setup-envtest use v1.37.0 -p path", true},
		{"setup-envtest  use  -p path", true},
		{"setup-envtest -p path use", true},
		{"setup-envtest --os linux use -p path", true},
		{"go run " + tool + "@v0.25.2 use -p path", true},
		{"setup-envtest \\\n  use -p path", true},
	} {
		failures, _ := envtestPinRules("ci.yml", tc.line)
		if got := len(failures) > 0; got != tc.bad {
			t.Errorf("%q: refused=%v, want %v (%v)", tc.line, got, tc.bad, failures)
		}
	}
	if _, uses := envtestPinRules("ci.yml", "go install "+tool+"@v0.25.2"); uses {
		t.Error("an install alone counts as running setup-envtest use")
	}
}
