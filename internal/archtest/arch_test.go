// Package archtest checks the import direction between this repository's
// packages. It parses the non-test sources and needs no build; files with
// known debt are inspected down to the exempted calls.
package archtest

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const module = "github.com/popododo0720/vws-storage"

const (
	model   = "internal/model"
	backend = "internal/backend"
	engine  = "internal/storage"
	store   = "internal/storage/internal/store"
	server  = "internal/server"
	health  = "internal/health"
	cmd     = "cmd/vws-storage"
)

// testSupport lists non-test packages that exist only to serve tests. They
// are exempt from the production rules, and no other non-test package may
// import "testing".
var testSupport = []string{store + "/storetest"}

func under(dir string, roots ...string) bool {
	for _, r := range roots {
		if dir == r || strings.HasPrefix(dir, r+"/") {
			return true
		}
	}
	return false
}

func hasPrefix(prefixes ...string) func(string) bool {
	return func(s string) bool { return under(s, prefixes...) }
}

// local is the module-relative path of an import of this module, or "".
func local(imp string) string {
	if !strings.HasPrefix(imp, module+"/") {
		return ""
	}
	return strings.TrimPrefix(imp, module+"/")
}

// onlyLocal allows imports of this module from the listed directories only
// (exact packages, not their children).
func onlyLocal(allowed ...string) func(string) bool {
	return func(imp string) bool {
		l := local(imp)
		if l == "" {
			return false
		}
		for _, a := range allowed {
			if l == a {
				return false
			}
		}
		return true
	}
}

// packageRules restrict which packages of this module a directory (and the
// directories under it) may import.
var packageRules = []struct {
	dir    string
	forbid func(imp string) bool
	why    string
}{
	{model, onlyLocal(), "the model imports nothing from this module"},
	{backend, onlyLocal(model, backend), "a backend sees only the backend contract and the model"},
	{store, onlyLocal(model, store), "a store sees only the store contract and the model"},
	{engine, func(imp string) bool {
		return under(local(imp), server, health, cmd, backend+"/fake", backend+"/kube", backend+"/s3")
	}, "the engine sees the backend contract, not a backend, and never the server"},
	{server, onlyLocal(model, engine), "the server calls the engine only: no backend, no store"},
	{health, onlyLocal(), "health checks are handed in by the process"},
}

// ownerRules confine an import to the directories that own it.
var ownerRules = []struct {
	match  func(imp string) bool
	owners []string
	why    string
}{
	{
		hasPrefix(
			"google.golang.org/protobuf",
			"google.golang.org/genproto",
			"github.com/popododo0720/vws-proto",
			"cloud.google.com/go/longrunning",
		),
		[]string{server},
		"wire types stay in the server",
	},
	{hasPrefix("google.golang.org/grpc"), []string{server, health, cmd}, "gRPC stays at the edge"},
	{hasPrefix("database/sql", "modernc.org/sqlite"), []string{store + "/sqlite"}, "SQL stays in the SQLite store"},
	{hasPrefix("k8s.io", "sigs.k8s.io"), []string{backend + "/kube", cmd}, "Kubernetes types stay in the kube backend and its flags"},
	{hasPrefix("github.com/aws"), []string{backend + "/s3"}, "the AWS SDK stays in the S3 backend"},
	{func(imp string) bool { return imp == "testing" }, testSupport, "production code does not import testing"},
	{func(imp string) bool { return imp == "log" }, nil, "logs go through log/slog and vws-lib/obs"},
}

// debt is one reported, unfixed violation of an owner rule. It exempts only
// the listed uses of the import, counted per enclosing function; any other
// use, or a missing one, is a violation. Remove an entry with its fix.
type debt struct {
	issue string
	why   string
	uses  map[string]int // enclosing function -> selector uses of the import
}

// knownDebt is keyed by "<file> <import>".
var knownDebt = map[string]debt{}

// usesOf counts selector uses of the package imported as imp in f, keyed by
// the enclosing top-level function ("" outside any function).
func usesOf(f *ast.File, spec *ast.ImportSpec, imp string) map[string]int {
	name := imp[strings.LastIndex(imp, "/")+1:]
	if spec.Name != nil {
		name = spec.Name.Name
	}
	uses := map[string]int{}
	for _, d := range f.Decls {
		fn := ""
		if fd, ok := d.(*ast.FuncDecl); ok {
			fn = fd.Name.Name
		}
		ast.Inspect(d, func(n ast.Node) bool {
			if sel, ok := n.(*ast.SelectorExpr); ok {
				if id, ok := sel.X.(*ast.Ident); ok && id.Name == name && id.Obj == nil {
					uses[fn]++
				}
			}
			return true
		})
	}
	return uses
}

// excused reports whether f's use of imp is exactly a knownDebt entry for r.
// A matching entry is marked in used, so an entry that matches no rule is stale.
func excused(dir string, f file, spec *ast.ImportSpec, imp, why string, used map[string]bool) (bool, string) {
	key := dir + "/" + f.name + " " + imp
	d, ok := knownDebt[key]
	if !ok || d.why != why {
		return false, ""
	}
	used[key] = true
	got := usesOf(f.ast, spec, imp)
	if reflect.DeepEqual(got, d.uses) {
		return true, ""
	}
	return false, fmt.Sprintf(" (known debt %s allows uses %v in %s, found %v)", d.issue, d.uses, f.name, got)
}

// constrainedDirs are the directories a rule names. Renaming one would
// silently disable its rule, so each must exist.
func constrainedDirs() []string {
	dirs := []string{backend + "/kube", backend + "/s3", backend + "/fake", store + "/sqlite", cmd}
	for _, r := range packageRules {
		dirs = append(dirs, r.dir)
	}
	return append(dirs, testSupport...)
}

type file struct {
	name string
	ast  *ast.File
}

// archViolations applies every rule to the non-test files in pkgs, keyed by
// slash-separated directory relative to the module root. It also returns the
// knownDebt entries that matched no violation.
func archViolations(pkgs map[string][]file) (bad, stale []string) {
	used := map[string]bool{}
	for dir, files := range pkgs {
		if under(dir, testSupport...) {
			continue
		}
		for _, f := range files {
			for _, spec := range f.ast.Imports {
				imp, _ := strconv.Unquote(spec.Path.Value)
				for _, r := range packageRules {
					if under(dir, r.dir) && r.forbid(imp) {
						bad = append(bad, fmt.Sprintf("%s imports %s: %s", dir, imp, r.why))
					}
				}
				for _, r := range ownerRules {
					if !r.match(imp) || under(dir, r.owners...) {
						continue
					}
					if ok, detail := excused(dir, f, spec, imp, r.why, used); !ok {
						bad = append(bad, fmt.Sprintf("%s imports %s: %s%s", dir, imp, r.why, detail))
					}
				}
			}
		}
	}
	sort.Strings(bad)
	for key, d := range knownDebt {
		if !used[key] {
			stale = append(stale, fmt.Sprintf("%s: known debt %s matches no violation", key, d.issue))
		}
	}
	sort.Strings(stale)
	return bad, stale
}

func loadProduction(t *testing.T, root string) map[string][]file {
	t.Helper()
	fset := token.NewFileSet()
	pkgs := map[string][]file{}
	err := walkFiles(root, func(rel string, data []byte) {
		if !strings.HasSuffix(rel, ".go") || strings.HasSuffix(rel, "_test.go") {
			return
		}
		f, err := parser.ParseFile(fset, rel, data, 0)
		if err != nil {
			t.Fatal(err)
		}
		dir := filepath.ToSlash(filepath.Dir(rel))
		pkgs[dir] = append(pkgs[dir], file{name: filepath.Base(rel), ast: f})
	})
	if err != nil {
		t.Fatal(err)
	}
	return pkgs
}

func TestArchitectureRules(t *testing.T) {
	probe := t.TempDir()
	imp := func(pkg, path string) string { return "package " + pkg + `; import _ "` + path + `"` }
	for rel, src := range map[string]string{
		model + "/m.go":             imp("model", module+"/"+backend),
		model + "/k.go":             imp("model", "k8s.io/apimachinery/pkg/types"),
		backend + "/kube/k.go":      imp("kube", module+"/"+engine),
		backend + "/s3/s.go":        imp("s3", module+"/"+backend+"/kube"),
		store + "/sqlite/s.go":      imp("sqlite", module+"/"+backend),
		store + "/memory/m.go":      imp("memory", "database/sql"),
		engine + "/e.go":            imp("storage", module+"/"+backend+"/kube"),
		engine + "/w.go":            imp("storage", module+"/"+server),
		server + "/s.go":            imp("server", module+"/"+store),
		server + "/b.go":            imp("server", module+"/"+backend),
		server + "/l.go":            imp("server", "log"),
		health + "/h.go":            imp("health", module+"/"+engine),
		"internal/widget/a.go":      imp("widget", "github.com/aws/aws-sdk-go-v2/aws"),
		"internal/widget/g.go":      imp("widget", "google.golang.org/grpc"),
		"internal/widget/p.go":      imp("widget", "github.com/popododo0720/vws-proto/gen/go/vws/storage/v1"),
		"internal/widget/t.go":      imp("widget", "testing"),
		store + "/storetest/s.go":   imp("storetest", "testing"),
		store + "/memory/m_test.go": imp("memory", "testing"),
		backend + "/kube/ok.go":     imp("kube", "k8s.io/client-go/kubernetes"),
		store + "/sqlite/ok.go":     imp("sqlite", "modernc.org/sqlite"),
		engine + "/ok.go":           imp("storage", module+"/"+store),
		cmd + "/main.go":            imp("main", module+"/"+backend+"/kube"),
	} {
		writeFile(t, filepath.Join(probe, rel), src)
	}
	want := []string{
		backend + "/kube imports " + module + "/" + engine + ": a backend sees only the backend contract and the model",
		backend + "/s3 imports " + module + "/" + backend + "/kube: a backend sees only the backend contract and the model",
		engine + " imports " + module + "/" + backend + "/kube: the engine sees the backend contract, not a backend, and never the server",
		engine + " imports " + module + "/" + server + ": the engine sees the backend contract, not a backend, and never the server",
		health + " imports " + module + "/" + engine + ": health checks are handed in by the process",
		"internal/widget imports github.com/aws/aws-sdk-go-v2/aws: the AWS SDK stays in the S3 backend",
		"internal/widget imports github.com/popododo0720/vws-proto/gen/go/vws/storage/v1: wire types stay in the server",
		"internal/widget imports google.golang.org/grpc: gRPC stays at the edge",
		"internal/widget imports testing: production code does not import testing",
		model + " imports " + module + "/" + backend + ": the model imports nothing from this module",
		model + " imports k8s.io/apimachinery/pkg/types: Kubernetes types stay in the kube backend and its flags",
		server + " imports " + module + "/" + backend + ": the server calls the engine only: no backend, no store",
		server + " imports " + module + "/" + store + ": the server calls the engine only: no backend, no store",
		server + " imports log: logs go through log/slog and vws-lib/obs",
		store + "/memory imports database/sql: SQL stays in the SQLite store",
		store + "/sqlite imports " + module + "/" + backend + ": a store sees only the store contract and the model",
	}
	sort.Strings(want)
	got, stale := archViolations(loadProduction(t, probe))
	if !reflect.DeepEqual(got, want) || len(stale) != 0 {
		t.Fatalf("probe violations:\n got %q\nwant %q\nstale %q", got, want, stale)
	}

	root := repoRoot(t)
	for _, dir := range constrainedDirs() {
		if _, err := os.Stat(filepath.Join(root, dir)); err != nil {
			t.Errorf("rule directory %s: %v", dir, err)
		}
	}
	bad, stale := archViolations(loadProduction(t, root))
	for _, v := range bad {
		t.Error(v)
	}
	for _, v := range stale {
		t.Error(v)
	}
}
