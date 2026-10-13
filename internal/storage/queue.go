package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/popododo0720/vws-lib/obs/report"

	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/storage/internal/store"
)

// QueueName labels the engine queue in vws.queue.depth and vws.queue.oldest_age.
const QueueName = "operations"

const queuePage = 500

// sampleQueue counts the non-terminal operations and finds the oldest. Run
// calls it once per scan, so a metric collection reads memory only.
func (s *Service) sampleQueue(ctx context.Context) {
	var depth int64
	var oldest time.Time
	var after model.OperationID
	for {
		ops, err := s.st.ListOperations(ctx, store.OperationFilter{NonTerminal: true}, after, queuePage)
		if err != nil {
			if ctx.Err() == nil {
				_ = report.Error(ctx, fmt.Errorf("storage: queue sample: %w", err)) //nolint:errcheck // reported here; the next tick samples again
			}
			return
		}
		for _, op := range ops {
			depth++
			if oldest.IsZero() || op.Meta.CreateTime.Before(oldest) {
				oldest = op.Meta.CreateTime
			}
		}
		if len(ops) < queuePage {
			break
		}
		after = ops[len(ops)-1].ID
	}
	s.queueDepth.Store(depth)
	s.queueOldest.Store(oldest.UnixNano())
	if oldest.IsZero() {
		s.queueOldest.Store(0)
	}
}

// QueueDepth is the number of non-terminal operations at the last scan.
func (s *Service) QueueDepth() int64 { return s.queueDepth.Load() }

// QueueOldest is the age of the oldest non-terminal operation at the last
// scan, or zero when there is none.
func (s *Service) QueueOldest() time.Duration {
	n := s.queueOldest.Load()
	if n == 0 {
		return 0
	}
	return s.now().Sub(time.Unix(0, n))
}
