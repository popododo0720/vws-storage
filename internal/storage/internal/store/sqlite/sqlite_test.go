package sqlite_test

import (
	"context"
	"os"
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

func TestOpenRelativePath(t *testing.T) {
	dir := t.TempDir()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(wd) })

	const rel = "nested/storage.db"
	st, err := sqlite.Open(context.Background(), rel)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, rel)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	second, err := sqlite.Open(ctx, rel)
	if err == nil {
		second.Close()
		st.Close()
		t.Fatal("second open succeeded")
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	again, err := sqlite.Open(context.Background(), rel)
	if err != nil {
		t.Fatal(err)
	}
	again.Close()
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
