package sqlite

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/popododo0720/vws-storage/internal/model"
)

// kinds is every record kind, in the order Export returns them: the rows an
// operation names come before it, and an operation before its tokens.
var kinds = []string{"volume", "snapshot", "bucket", "object", "operation", "token"}

// Export returns every record of the file at path, sorted by kind and key.
// It reads a copy, so the file is left as it is, migration included. A file
// with a journal beside it was not closed cleanly and is refused.
func Export(ctx context.Context, path string) (_ []model.Record, err error) {
	for _, side := range []string{"-journal", "-wal"} {
		if _, err := os.Stat(path + side); err == nil {
			return nil, fmt.Errorf("sqlite: %s%s exists: stop the service that holds the file first", path, side)
		}
	}
	dir, err := os.MkdirTemp("", "vws-storage-export-")
	if err != nil {
		return nil, fmt.Errorf("sqlite: temp dir: %w", err)
	}
	defer func() { err = errors.Join(err, os.RemoveAll(dir)) }()
	cp := filepath.Join(dir, "storage.db")
	if err := copyFile(path, cp); err != nil {
		return nil, err
	}
	st, err := Open(ctx, cp)
	if err != nil {
		return nil, err
	}
	defer closeInto(st, &err)
	db, ok := st.(*dbStore)
	if !ok {
		return nil, fmt.Errorf("sqlite: unexpected store %T", st)
	}
	var out []model.Record
	for _, kind := range kinds {
		recs, err := db.byKind(ctx, kind)
		if err != nil {
			return nil, err
		}
		sort.Slice(recs, func(i, j int) bool { return recs[i].RecordKey().String() < recs[j].RecordKey().String() })
		out = append(out, recs...)
	}
	return out, nil
}

func copyFile(from, to string) (err error) {
	src, err := os.Open(from) //nolint:gosec // the operator names the file to import
	if err != nil {
		return fmt.Errorf("sqlite: open %s: %w", from, err)
	}
	defer closeInto(src, &err)
	dst, err := os.OpenFile(to, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600) //nolint:gosec // to is in a fresh temp dir
	if err != nil {
		return fmt.Errorf("sqlite: create copy: %w", err)
	}
	defer closeInto(dst, &err)
	if _, err := io.Copy(dst, src); err != nil {
		return fmt.Errorf("sqlite: copy %s: %w", from, err)
	}
	return nil
}
