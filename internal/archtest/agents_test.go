package archtest

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const judgment = "Judgment"

var testName = regexp.MustCompile("`(Test[A-Za-z0-9_]+)`")

// ruleTableViolations checks the rule table under "## Rules" in agents: every
// row has a rule and an enforcer, and every test an enforcer names exists in
// tests. It returns the number of Judgment rows.
func ruleTableViolations(agents string, tests map[string]bool) (bad []string, judged int) {
	in := false
	rows := 0
	sc := bufio.NewScanner(strings.NewReader(agents))
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "## ") {
			in = line == "## Rules"
			continue
		}
		if !in || !strings.HasPrefix(line, "|") || strings.HasPrefix(line, "| Rule |") || strings.HasPrefix(line, "| ---") {
			continue
		}
		rows++
		cells := strings.Split(strings.Trim(line, "|"), "|")
		if len(cells) != 2 {
			bad = append(bad, fmt.Sprintf("row %d has %d cells: %.60s", rows, len(cells), line))
			continue
		}
		rule, enforcer := strings.TrimSpace(cells[0]), strings.TrimSpace(cells[1])
		switch {
		case rule == "" || enforcer == "":
			bad = append(bad, fmt.Sprintf("row %d has no rule or no enforcer: %.60s", rows, line))
		case enforcer == judgment:
			judged++
		}
		for _, m := range testName.FindAllStringSubmatch(enforcer, -1) {
			if !tests[m[1]] {
				bad = append(bad, fmt.Sprintf("row %d names %s, which is not a test", rows, m[1]))
			}
		}
	}
	if rows == 0 {
		bad = append(bad, "no rule table under ## Rules")
	}
	return bad, judged
}

var testFunc = regexp.MustCompile(`(?m)^func (Test[A-Za-z0-9_]+)\(`)

func testNames(t *testing.T, root string) map[string]bool {
	t.Helper()
	names := map[string]bool{}
	err := walkFiles(root, func(rel string, data []byte) {
		if strings.HasSuffix(rel, "_test.go") {
			for _, m := range testFunc.FindAllSubmatch(data, -1) {
				names[string(m[1])] = true
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	return names
}

func TestAgentsRuleTable(t *testing.T) {
	probe := "# x\n\n## Rules\n\n| Rule | Enforcer |\n| --- | --- |\n" +
		"| a | `TestReal` |\n| b | Judgment |\n| c | |\n| d | `TestGone` |\n| e | f | g |\n\n## Next\n\n| x | y | z |\n"
	bad, judged := ruleTableViolations(probe, map[string]bool{"TestReal": true})
	want := []string{
		"row 3 has no rule or no enforcer: | c | |",
		"row 4 names TestGone, which is not a test",
		"row 5 has 3 cells: | e | f | g |",
	}
	if strings.Join(bad, "\n") != strings.Join(want, "\n") || judged != 1 {
		t.Fatalf("probe: judged %d\n got %q\nwant %q", judged, bad, want)
	}

	root := repoRoot(t)
	agents, err := os.ReadFile(filepath.Join(root, "AGENTS.md"))
	if err != nil {
		t.Fatal(err)
	}
	bad, judged = ruleTableViolations(string(agents), testNames(t, root))
	for _, v := range bad {
		t.Error("AGENTS.md: " + v)
	}
	t.Logf("AGENTS.md rule table: %d Judgment rows", judged)
}
