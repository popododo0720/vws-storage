package memory_test

import (
	"testing"

	"github.com/popododo0720/vws-storage/internal/storage/internal/store"
	"github.com/popododo0720/vws-storage/internal/storage/internal/store/memory"
	"github.com/popododo0720/vws-storage/internal/storage/internal/store/storetest"
)

func TestStore(t *testing.T) {
	storetest.Run(t, func(*testing.T) store.Store {
		return memory.Open()
	})
}
