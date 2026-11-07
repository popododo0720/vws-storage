package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestImportSQLiteFlags(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want string
	}{
		{nil, "-from and -store-namespace are required"},
		{[]string{"-from=x.db"}, "-from and -store-namespace are required"},
		{[]string{"-from=x.db", "-store-namespace=vws", "-dry-run", "-verify"}, "exclusive"},
		{[]string{"-from=x.db", "-store-namespace=vws", "extra"}, `unexpected argument "extra"`},
		{[]string{"-db=x"}, "flag provided but not defined"},
	} {
		var out bytes.Buffer
		err := runImport(tc.args, &out)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%v: %v, want %q", tc.args, err, tc.want)
		}
	}
}

func TestImportSQLiteRoutesFromRun(t *testing.T) {
	err := run([]string{"import-sqlite"})
	if err == nil || !strings.Contains(err.Error(), "import-sqlite: -from") {
		t.Fatalf("run import-sqlite: %v", err)
	}
}
