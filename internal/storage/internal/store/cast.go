package store

import (
	"fmt"

	"github.com/popododo0720/vws-storage/internal/model"
)

// Cast returns rec as T. A row filed under a kind always holds that kind's
// record, so a mismatch means the store is corrupt and is an error, not a panic.
func Cast[T model.Record](rec model.Record) (T, error) {
	v, ok := rec.(T)
	if !ok {
		var zero T
		return zero, fmt.Errorf("store: record is %T, want %T", rec, zero)
	}
	return v, nil
}
