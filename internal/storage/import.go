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

// ErrImportObjects refuses a file with object records or object operations
// still running: the Kubernetes store does not keep objects yet (#141 AC 6).
var ErrImportObjects = errors.New(
	"storage: the file has object records or running object operations, which the Kubernetes store does not serve yet")

// ImportSQLite copies the records of the SQLite file at from into the
// Kubernetes store, one model.Import change per record. It never overwrites:
// a record the store already holds unchanged is skipped, so a second run
// writes nothing, and one it holds otherwise is a difference. Nothing is
// written when any record differs, when the file has object records or a
// record too large for the store, or in ImportDryRun and ImportVerify. The
// file itself is not changed.
func ImportSQLite(ctx context.Context, from string, to ImportTarget, mode ImportMode) (ImportReport, error) {
	recs, err := sqlite.Export(ctx, from)
	if err != nil {
		return ImportReport{}, err
	}
	rep := ImportReport{Records: len(recs)}
	objects := 0
	for _, rec := range recs {
		if objectState(rec) {
			objects++
		}
	}
	if objects > 0 {
		return rep, fmt.Errorf("%w: %d", ErrImportObjects, objects)
	}
	for _, rec := range recs {
		if err := kube.Fits(rec); err != nil {
			return rep, fmt.Errorf("storage: import %s: %w", describe(rec), err)
		}
	}
	opts := kube.Options{Namespace: to.Namespace}
	if mode != ImportWrite {
		r, err := kube.ConnectReader(ctx, to.Kubeconfig, opts)
		if err != nil {
			return rep, err
		}
		err = compare(ctx, r, recs, mode, &rep)
		return rep, errors.Join(err, differs(rep))
	}
	st, err := kube.Connect(ctx, to.Kubeconfig, opts)
	if err != nil {
		return rep, err
	}
	defer closeLogged(ctx, st)
	// Every record is checked before the first write: an operation of the
	// file must not land beside a record the store holds otherwise, where Run
	// would act on the store's resource.
	if err := compare(ctx, st, recs, ImportDryRun, &rep); err != nil || len(rep.Differences) > 0 {
		rep.Create = nil
		return rep, errors.Join(err, differs(rep))
	}
	rep.Create = nil
	if err := importAll(ctx, st, recs, &rep); err != nil || len(rep.Differences) > 0 {
		return rep, errors.Join(err, differs(rep))
	}
	return rep, st.MarkImported(ctx)
}

// compare fills rep from what r holds of recs. ImportDryRun lists an absent
// record to create; ImportVerify lists it as missing.
// A record the store removed later is a difference: its tombstone is there,
// or an import already finished, so every record of the file was written once.
// The finished mark outlives the tombstones, which Open purges.
func compare(ctx context.Context, r kube.Reader, recs []model.Record, mode ImportMode, rep *ImportReport) error {
	done, err := r.Imported(ctx)
	if err != nil {
		return err
	}
	for _, rec := range recs {
		cur, err := readRecord(ctx, r, rec.RecordKey())
		if errors.Is(err, model.ErrNotFound) {
			removed := done
			if !removed {
				var rerr error
				if removed, rerr = r.Removed(ctx, rec.RecordKey()); rerr != nil {
					return rerr
				}
			}
			if removed {
				rep.Differences = append(rep.Differences, "removed "+describe(rec))
				continue
			}
		}
		switch {
		case errors.Is(err, model.ErrNotFound) && mode == ImportDryRun:
			rep.Create = append(rep.Create, describe(rec))
		case errors.Is(err, model.ErrNotFound):
			rep.Differences = append(rep.Differences, "missing "+describe(rec))
		case err != nil:
			return err
		case !model.SameRecord(cur, rec):
			rep.Differences = append(rep.Differences, "differs "+describe(rec))
		}
	}
	return nil
}

// importAll writes each absent record. A record that changed since compare
// stops the import, so nothing more of the file lands beside it; what was
// written before stays, and the import is not marked finished.
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
		if conflict {
			rep.Differences = append(rep.Differences, "differs "+describe(rec))
			return nil
		}
		if created {
			rep.Create = append(rep.Create, describe(rec))
		}
	}
	return nil
}

// objectState is a record of the object path: an object row, or an object
// operation still running, which reads object rows when it resumes.
func objectState(rec model.Record) bool {
	switch r := rec.(type) {
	case model.Object:
		return true
	case model.Operation:
		switch r.Work.(type) {
		case model.ObjectUpload, model.ObjectPurge:
			return !r.Terminal()
		}
	}
	return false
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
