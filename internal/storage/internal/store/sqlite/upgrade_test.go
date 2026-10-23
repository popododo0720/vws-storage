package sqlite_test

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/storage/internal/store/sqlite"
)

// Every row the 0.1.0 build wrote (testdata/upgrade/0.1.0/README.md) decodes
// with the current code and encodes back to the same bytes, so a renamed
// field or a changed encoding fails here even when no reader looks at it
// (#129). Open accepts the file without a schema error.
func TestUpgradeFrom010RecordsRoundTrip(t *testing.T) {
	raw, err := os.ReadFile("../../../../../testdata/upgrade/0.1.0/storage.db")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "storage.db")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	st, err := sqlite.Open(context.Background(), path)
	if err != nil {
		t.Fatalf("open 0.1.0 file: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.QueryContext(context.Background(), `SELECT kind, key, payload FROM records ORDER BY kind, key`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	kinds := map[string]int{}
	for rows.Next() {
		var kind, key string
		var payload []byte
		if err := rows.Scan(&kind, &key, &payload); err != nil {
			t.Fatal(err)
		}
		rec, err := model.UnmarshalRecord(payload)
		if err != nil {
			t.Errorf("decode %s %s: %v", kind, key, err)
			continue
		}
		again, err := model.MarshalRecord(rec)
		if err != nil {
			t.Errorf("encode %s %s: %v", kind, key, err)
			continue
		}
		if !bytes.Equal(again, payload) {
			t.Errorf("%s %s re-encodes differently:\nstored %s\nnow    %s", kind, key, payload, again)
		}
		kinds[kind]++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := map[string]int{"volume": 3, "snapshot": 1, "token": 4, "operation": 4}
	for k, n := range want {
		if kinds[k] != n {
			t.Errorf("%s rows %d want %d (all %v)", k, kinds[k], n, kinds)
		}
	}
	if len(kinds) != len(want) {
		t.Errorf("kinds %v want %v", kinds, want)
	}
}
