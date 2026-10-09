package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

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

func TestSecondOpenFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vws.db")
	st, err := sqlite.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	second, err := sqlite.Open(ctx, path)
	if err == nil {
		second.Close()
		t.Fatal("second open succeeded")
	}
}
