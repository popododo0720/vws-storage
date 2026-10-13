package storage_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/popododo0720/vws-storage/internal/backend/fake"
	"github.com/popododo0720/vws-storage/internal/storage"
)

// A store that fails under the engine is a server fault: it is reported
// (vws.errors), not only printed. Before, it went to log.Printf at INFO and
// was not counted.
func TestEngineReportsAStoreFailure(t *testing.T) {
	reader := setupMetrics(t)
	obj := fake.NewObjects(fake.ObjectOptions{Now: time.Now})
	svc, err := storage.Open(context.Background(), opts(storage.SQLite(filepath.Join(t.TempDir(), "s.db")), fake.NewBlock(), obj))
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Close(); err != nil { // the engine's next read fails
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := svc.Run(ctx); err != nil {
		t.Fatal(err)
	}
	for _, p := range collect(t, reader) {
		if p.name == "vws.errors" {
			return
		}
	}
	t.Fatal("a failing store under the engine counted no vws.errors")
}
