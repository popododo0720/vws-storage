package archtest

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestKnownDebt(t *testing.T) {
	const logWhy = "logs go through log/slog and vws-lib/obs"
	probe := t.TempDir()
	writeFile(t, filepath.Join(probe, "internal/probe/kept/a.go"),
		"package kept\n\nimport \"log\"\n\nfunc kept() { log.Print(\"x\") }\n")
	writeFile(t, filepath.Join(probe, "internal/probe/other/b.go"),
		"package other\n\nimport \"log\"\n\nfunc other() { log.Print(\"x\") }\n")

	old := knownDebt
	t.Cleanup(func() { knownDebt = old })
	knownDebt = map[string]debt{
		"internal/probe/kept/a.go log": {
			issue: "#163",
			why:   logWhy,
			uses:  map[string]int{"kept": 1},
		},
		"internal/probe/gone/c.go log": {
			issue: "#163",
			why:   logWhy,
			uses:  map[string]int{"gone": 1},
		},
	}

	bad, stale := archViolations(loadProduction(t, probe))
	wantBad := []string{"internal/probe/other imports log: " + logWhy}
	if !reflect.DeepEqual(bad, wantBad) {
		t.Fatalf("violations:\n got %q\nwant %q", bad, wantBad)
	}
	wantStale := []string{"internal/probe/gone/c.go log: known debt #163 matches no violation"}
	if !reflect.DeepEqual(stale, wantStale) {
		t.Fatalf("stale debt:\n got %q\nwant %q", stale, wantStale)
	}
}
