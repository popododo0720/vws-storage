package store

import (
	"sort"

	"github.com/popododo0720/vws-storage/internal/model"
)

// PageOperations filters and orders ops by id. after is exclusive.
func PageOperations(all []model.Operation, f OperationFilter, after model.OperationID, limit int) []model.Operation {
	out := make([]model.Operation, 0, len(all))
	afterID := after.String()
	for _, op := range all {
		if !f.Allows(op) {
			continue
		}
		if afterID != "" && op.ID.String() <= afterID {
			continue
		}
		out = append(out, op)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID.String() < out[j].ID.String() })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}
