package model

import (
	"fmt"
	"sort"
	"time"
)

// ModificationLimit and ModificationWindow are the EC2 ModifyVolume limit: a
// volume can be modified up to four times within a rolling 24-hour period
// (https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_ModifyVolume.html).
const (
	ModificationLimit  = 4
	ModificationWindow = 24 * time.Hour
)

// ErrModificationRateExceeded refuses a volume modification beyond
// ModificationLimit in ModificationWindow. EC2 answers this with
// VolumeModificationRateExceeded.
var ErrModificationRateExceeded = sentinelError("volume modification rate exceeded")

// Modifications is the start times of a volume's last modifications that
// count against the EC2 limit, oldest first. Zero entries are unused. It is
// stored on the volume row, so the count survives retries and restarts.
type Modifications [ModificationLimit]time.Time

// recent returns the entries that started less than ModificationWindow
// before now, oldest first. An entry after now (clock moved back) counts.
func (m Modifications) recent(now time.Time) []time.Time {
	out := make([]time.Time, 0, ModificationLimit)
	for _, t := range m {
		if !t.IsZero() && t.Add(ModificationWindow).After(now) {
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Before(out[j]) })
	return out
}

// Count returns how many modifications count against the limit at now.
func (m Modifications) Count(now time.Time) int { return len(m.recent(now)) }

// admit refuses a new modification at now when the limit is reached. The
// error names the time the oldest counted modification leaves the window,
// as the EC2 error message does.
func (m Modifications) admit(now time.Time) error {
	r := m.recent(now)
	if len(r) < ModificationLimit {
		return nil
	}
	next := r[0].Add(ModificationWindow).UTC().Format("2006-01-02T15:04:05.000Z")
	return fmt.Errorf("%w: wait until %s before the next modification of this volume", ErrModificationRateExceeded, next)
}

// add records a modification started at t. Entries outside the window at t
// are dropped, and only the newest ModificationLimit are kept.
func (m Modifications) add(t time.Time) Modifications {
	r := append(m.recent(t), norm(t))
	sort.Slice(r, func(i, j int) bool { return r[i].Before(r[j]) })
	if len(r) > ModificationLimit {
		r = r[len(r)-ModificationLimit:]
	}
	var out Modifications
	copy(out[:], r)
	return out
}

// modificationsOf builds Modifications from stored times, keeping the newest
// ModificationLimit.
func modificationsOf(ts []time.Time) Modifications {
	r := make([]time.Time, 0, len(ts))
	for _, t := range ts {
		if !t.IsZero() {
			r = append(r, norm(t))
		}
	}
	sort.Slice(r, func(i, j int) bool { return r[i].Before(r[j]) })
	if len(r) > ModificationLimit {
		r = r[len(r)-ModificationLimit:]
	}
	var out Modifications
	copy(out[:], r)
	return out
}

// times returns the used entries, oldest first.
func (m Modifications) times() []time.Time {
	var out []time.Time
	for _, t := range m {
		if !t.IsZero() {
			out = append(out, t)
		}
	}
	return out
}
