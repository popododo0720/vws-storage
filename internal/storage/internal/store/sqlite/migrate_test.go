package sqlite_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/popododo0720/vws-storage/internal/storage/internal/store/sqlite"
)

func userVersion(t *testing.T, path string) int {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var v int
	if err := db.QueryRowContext(context.Background(), `PRAGMA user_version`).Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

// Version 2 adds the error lifecycle to volume rows. A version 1 file opens
// and moves to 2, so an older binary refuses it at Open instead of failing
// on a row it cannot decode.
func TestOpenMigratesVersion1To2(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vws.db")
	st, err := sqlite.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if v := userVersion(t, path); v != 2 {
		t.Fatalf("new file at version %d", v)
	}
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(context.Background(), `PRAGMA user_version = 1`); err != nil {
		t.Fatal(err)
	}
	db.Close()
	st, err = sqlite.Open(context.Background(), path)
	if err != nil {
		t.Fatalf("open version 1: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if v := userVersion(t, path); v != 2 {
		t.Fatalf("migrated file at version %d", v)
	}
}
