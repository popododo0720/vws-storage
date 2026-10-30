package sqlite

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/storage/internal/store"
)

func TestBucketNameIndexIsMaintained(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "vws.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	s := st.(*dbStore)
	b, err := model.ParseBucketName("projects/a111111111111/buckets/logs")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
	admitted, err := model.NewBucket(
		model.Start{Op: model.NewOperationID().WithProject(b.Project()), Now: now},
		model.CreateBucketRequest{Bucket: b},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Update(ctx, func(store.Reader) (model.Change, error) {
		return admitted.Change, nil
	}); err != nil {
		t.Fatal(err)
	}
	got, err := st.BucketsNamed(ctx, "logs")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Name != b {
		t.Fatalf("buckets named logs = %v, want %s", got, b)
	}
	if err := s.Delete(ctx, b); err != nil {
		t.Fatal(err)
	}
	var indexed int
	const countSQL = `SELECT COUNT(*) FROM bucket_names WHERE key = ?`
	if err := s.sql.QueryRowContext(ctx, countSQL, b.String()).Scan(&indexed); err != nil {
		t.Fatal(err)
	}
	if indexed != 0 {
		t.Fatalf("bucket_names rows for %s after delete = %d, want 0", b, indexed)
	}
	got, err = st.BucketsNamed(ctx, "logs")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("buckets named logs after delete = %v, want none", got)
	}
}

// CreateBucket refuses a name another project holds. The lookup must read the
// name index, not every bucket row, so create stays fast as the store grows.
func TestCreateBucketNameLookupDoesNotScanEveryBucket(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "vws.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	rows, err := st.(*dbStore).sql.QueryContext(ctx, `EXPLAIN QUERY PLAN `+bucketsNamedQuery, "logs")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var id, parent, notUsed int
		var detail string
		if err := rows.Scan(&id, &parent, &notUsed, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(plan, "\n")
	if !strings.Contains(joined, "bucket_names") || !strings.Contains(joined, "SEARCH") {
		t.Fatalf("bucket name lookup plan does not use the index:\n%s", joined)
	}
	for _, line := range plan {
		if strings.Contains(line, "SCAN") {
			t.Fatalf("bucket name lookup scans rows:\n%s", joined)
		}
	}
}
