package sqlite_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/popododo0720/vws-storage/internal/model"
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

// Version 3 adds the bucket name index. A version 1 file opens and moves to 3,
// so an older binary refuses it at Open instead of failing on a row it cannot
// decode.
func TestOpenMigratesVersion1To3(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vws.db")
	st, err := sqlite.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if v := userVersion(t, path); v != 3 {
		t.Fatalf("new file at version %d", v)
	}
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(context.Background(), `DROP TABLE bucket_names`); err != nil {
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
	if v := userVersion(t, path); v != 3 {
		t.Fatalf("migrated file at version %d", v)
	}
}

// A version 2 file has bucket rows but no name index. Open fills the index for
// every project that holds a name.
func TestOpenBackfillsBucketNameIndex(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "vws.db")
	st, err := sqlite.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
	for _, name := range []string{
		"projects/a111111111111/buckets/logs",
		"projects/a222222222222/buckets/logs",
	} {
		b, err := model.ParseBucketName(name)
		if err != nil {
			t.Fatal(err)
		}
		payload, err := model.MarshalRecord(model.Bucket{
			Name:  b,
			State: model.Available{},
			Meta:  model.Meta{Version: 1, CreateTime: now, UpdateTime: now},
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx,
			`INSERT INTO records(kind, key, payload) VALUES('bucket', ?, ?)`, b.String(), payload); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, `DROP TABLE bucket_names`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `PRAGMA user_version = 2`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	st, err = sqlite.Open(ctx, path)
	if err != nil {
		t.Fatalf("open version 2: %v", err)
	}
	got, err := st.BucketsNamed(ctx, "logs")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("buckets named logs = %d, want 2", len(got))
	}
	if got[0].Name.String() >= got[1].Name.String() {
		t.Fatalf("bucket names are not sorted: %s, %s", got[0].Name, got[1].Name)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if v := userVersion(t, path); v != 3 {
		t.Fatalf("backfilled file at version %d", v)
	}
}
