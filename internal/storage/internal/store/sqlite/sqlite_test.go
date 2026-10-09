package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/popododo0720/vws-storage/internal/storage/internal/store"
	"github.com/popododo0720/vws-storage/internal/storage/internal/store/sqlite"
	"github.com/popododo0720/vws-storage/internal/storage/internal/store/storetest"
)

func TestStore(t *testing.T) {
	storetest.Run(t, func(t *testing.T) store.Store {
		st, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "vws.db"))
		if err != nil {
			t.Fatal(err)
		}
		return st
	})
}
