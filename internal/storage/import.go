package storage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/popododo0720/vws-lib/obs"

	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/storage/internal/store"
	"github.com/popododo0720/vws-storage/internal/storage/internal/store/kube"
	"github.com/popododo0720/vws-storage/internal/storage/internal/store/sqlite"
)

// ImportMode is what ImportSQLite does with the file's records.
type ImportMode int

const (
	// ImportWrite creates every record the store does not have.
	ImportWrite ImportMode = iota
	// ImportDryRun reports the records ImportWrite would create and writes nothing.
	ImportDryRun
	// ImportVerify compares every record of the file with the store and writes nothing.
	ImportVerify
)

// ImportTarget is the Kubernetes store an import writes to.
type ImportTarget struct {
	Kubeconfig string
	Namespace  string
}

// ImportReport is what an import found. Create lists the records ImportWrite
// created or ImportDryRun would create. Differences lists the records the
// store holds otherwise than the file, or (ImportVerify) does not hold.
type ImportReport struct {
	Records     int
	Create      []string
	Differences []string
}

// ErrImportDiffers is returned when Differences is not empty.
var ErrImportDiffers = errors.New("storage: the store differs from the file")

// ErrImportObjects refuses a file with object records: the Kubernetes store
// does not keep them yet (#141 AC 6).
var ErrImportObjects = errors.New("storage: the file has object records, which the Kubernetes store does not keep yet")

// ImportSQLite copies the records of the SQLite file at from into the
// Kubernetes store, one model.Import change per record. It never overwrites:
// a record the store already holds unchanged is skipped, so a second run
// writes nothing, and one it holds otherwise is a difference. Nothing is
// written when the file has object records or in ImportDryRun and
// ImportVerify. The file itself is not changed.
func ImportSQLite(ctx context.Context, from string, to ImportTarget, mode ImportMode) (ImportReport, error) {
	recs, err := sqlite.Export(ctx, from)
	if err != nil {
		return ImportReport{}, err
	}
	rep := ImportReport{Records: len(recs)}
	objects := 0
	for _, rec := range recs {
		if _, ok := rec.(model.Object); ok {
			objects++
		}
	}
	if objects > 0 {
		return rep, fmt.Errorf("%w: %d", ErrImportObjects, objects)
	}
	opts := kube.Options{Namespace: to.Namespace}
	if mode == ImportWrite {
		st, err := kube.Connect(ctx, to.Kubeconfig, opts)
		if err != nil {
			return rep, err
		}
		defer closeLogged(ctx, st)
		err = importAll(ctx, st, recs, &rep)
		return rep, errors.Join(err, differs(rep))
	}
	r, err := kube.ConnectReader(ctx, to.Kubeconfig, opts)
	if err != nil {
		return rep, err
	}
	for _, rec := range recs {
		cur, err := readRecord(ctx, r, rec.RecordKey())
		switch {
		case errors.Is(err, model.ErrNotFound) && mode == ImportDryRun:
			rep.Create = append(rep.Create, describe(rec))
		case errors.Is(err, model.ErrNotFound):
			rep.Differences = append(rep.Differences, "missing "+describe(rec))
		case err != nil:
			return rep, err
		case !model.SameRecord(cur, rec):
			rep.Differences = append(rep.Differences, "differs "+describe(rec))
		}
	}
	return rep, differs(rep)
}

func importAll(ctx context.Context, st store.Store, recs []model.Record, rep *ImportReport) error {
	for _, rec := range recs {
		var created, conflict bool
		err := st.Update(ctx, func(r store.Reader) (model.Change, error) {
			created, conflict = false, false
			cur, err := readRecord(ctx, r, rec.RecordKey())
			switch {
			case errors.Is(err, model.ErrNotFound):
				created = true
				return model.Import(rec)
			case err != nil:
				return model.Change{}, err
			case !model.SameRecord(cur, rec):
				conflict = true
			}
			return model.Change{}, nil
		})
		if err != nil {
			return fmt.Errorf("storage: import %s: %w", describe(rec), err)
		}
		switch {
		case created:
			rep.Create = append(rep.Create, describe(rec))
		case conflict:
			rep.Differences = append(rep.Differences, "differs "+describe(rec))
		}
	}
	return nil
}

func differs(rep ImportReport) error {
	if len(rep.Differences) > 0 {
		return fmt.Errorf("%w: %d records", ErrImportDiffers, len(rep.Differences))
	}
	return nil
}

func closeLogged(ctx context.Context, st store.Store) {
	if err := st.Close(); err != nil {
		slog.WarnContext(ctx, "close store", slog.String(obs.FieldErr, err.Error()))
	}
}

// readRecord reads the record under key from r.
func readRecord(ctx context.Context, r store.Reader, key model.Key) (model.Record, error) {
	switch k := key.(type) {
	case model.VolumeID:
		return r.Volume(ctx, k)
	case model.SnapshotID:
		return r.Snapshot(ctx, k)
	case model.BucketName:
		return r.Bucket(ctx, k)
	case model.ObjectPath:
		return r.Object(ctx, k)
	case model.OperationID:
		return r.Operation(ctx, k)
	case model.TokenKey:
		return r.Token(ctx, k)
	default:
		return nil, fmt.Errorf("%w: key %T", model.ErrInvalidArgument, key)
	}
}

func describe(rec model.Record) string {
	kind := "record"
	switch rec.(type) {
	case model.Volume:
		kind = "volume"
	case model.Snapshot:
		kind = "snapshot"
	case model.Bucket:
		kind = "bucket"
	case model.Object:
		kind = "object"
	case model.Operation:
		kind = "operation"
	case model.TokenRecord:
		kind = "token"
	}
	return kind + " " + rec.RecordKey().String()
}
