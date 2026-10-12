package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/popododo0720/vws-storage/internal/storage/internal/store/sqlite"
)

// A crash cancels the context while a query runs. Close must still release
// the file, so the next Open in this process gets the lock.
func TestReopenAfterCancelledQuery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vws.db")
	for i := range 300 {
		st, err := sqlite.Open(context.Background(), path)
		if err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			for ctx.Err() == nil {
				_, _ = st.Runnable(ctx, time.Now(), 10)
			}
		}()
		time.Sleep(time.Duration(i%7) * 50 * time.Microsecond)
		cancel()
		<-done
		if err := st.Close(); err != nil {
			t.Fatalf("close %d: %v", i, err)
		}
	}
}
