package kube

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/storage/internal/store"
)

const (
	maxCommitAttempts = 30
	maxCallAttempts   = 5
)

// errLostRace means another commit moved the head after this one read it.
var errLostRace = errors.New("kubestore: head moved")

// intent is a commit: every write it makes, recorded in the head before the
// first record changes.
type intent struct {
	Txn    string    `json:"txn"`
	Start  time.Time `json:"start"`
	Writes []write   `json:"writes"`
}

// write sets one record ConfigMap. RV is the resourceVersion the commit read,
// or "" when the ConfigMap did not exist. A nil Record writes a tombstone.
// Purge deletes the ConfigMap.
type write struct {
	Name   string          `json:"name"`
	Kind   string          `json:"kind"`
	Key    string          `json:"key"`
	RV     string          `json:"rv,omitempty"`
	Record json.RawMessage `json:"record,omitempty"`
	Live   bool            `json:"live,omitempty"`
	Purge  bool            `json:"purge,omitempty"`
}

// Update runs fn with the head clear and commits its change. fn runs again
// when another commit lands between its reads and this commit.
func (s *Store) Update(ctx context.Context, fn func(store.Reader) (model.Change, error)) error {
	return s.transact(ctx, func(ctx context.Context) ([]write, error) {
		ch, err := fn(s)
		if err != nil {
			return nil, err
		}
		st := &stage{s: s, slots: map[string]*slot{}}
		if err := store.Apply(ctx, st, ch); err != nil {
			return nil, err
		}
		return st.writes(), nil
	})
}

func (s *Store) transact(ctx context.Context, build func(context.Context) ([]write, error)) error {
	delay := 10 * time.Millisecond
	for attempt := 1; ; attempt++ {
		head, err := s.clearHead(ctx)
		if err != nil {
			return err
		}
		ws, err := build(ctx)
		if err == nil && len(ws) > 0 {
			err = s.commitAndFinish(ctx, head, ws)
		} else if s.moved(ctx, head) {
			// The reads may straddle another commit, so their result
			// (an error included) is not one serial order allows.
			err = errLostRace
		}
		if errors.Is(err, errLostRace) {
			if attempt == maxCommitAttempts {
				return fmt.Errorf("kubestore: %d commits lost the head race: %w", attempt, err)
			}
			if err := sleep(ctx, delay); err != nil {
				return err
			}
			delay = min(2*delay, 200*time.Millisecond)
			continue
		}
		return err
	}
}

// commitAndFinish commits ws and rolls them forward.
func (s *Store) commitAndFinish(ctx context.Context, head *corev1.ConfigMap, ws []write) error {
	in := intent{Txn: newTxn(), Start: s.o.Now(), Writes: ws}
	// The budget starts before the commit, so a writer suspended after the
	// commit wakes past its deadline and writes nothing; by then a takeover
	// owns the intent. A client that gave up must not leave the head
	// blocked until a takeover, so the budget ignores ctx's cancellation.
	rctx, cancel := context.WithDeadline(context.WithoutCancel(ctx), time.Now().Add(s.o.WriterBudget))
	defer cancel()
	next, err := s.commit(ctx, head, in)
	if err != nil {
		return err
	}
	if s.afterCommit != nil {
		s.afterCommit()
	}
	if err := s.finish(rctx, next, in); err != nil {
		return fmt.Errorf("kubestore: txn %s committed, the next writer finishes it: %w", in.Txn, err)
	}
	return nil
}

// moved reports whether a commit started since head was read. A head that
// cannot be read counts as moved, so the caller retries.
func (s *Store) moved(ctx context.Context, head *corev1.ConfigMap) bool {
	cur, err := s.head(ctx)
	return err != nil || cur.ResourceVersion != head.ResourceVersion
}

// clearHead returns the head with no intent. An intent younger than
// TakeoverAfter belongs to a writer that may still be rolling it, so this
// waits; an older one is rolled forward here.
func (s *Store) clearHead(ctx context.Context) (*corev1.ConfigMap, error) {
	delay := 10 * time.Millisecond
	for {
		head, err := s.head(ctx)
		if err != nil {
			return nil, err
		}
		raw, ok := head.Data[dataIntent]
		if !ok {
			return head, nil
		}
		var in intent
		if err := json.Unmarshal([]byte(raw), &in); err != nil {
			return nil, fmt.Errorf("kubestore: decode intent: %w", err)
		}
		if age := s.o.Now().Sub(in.Start); age < s.o.TakeoverAfter {
			if err := sleep(ctx, min(delay, s.o.TakeoverAfter-age)); err != nil {
				return nil, err
			}
			delay = min(2*delay, time.Second)
			continue
		}
		s.o.Log.WarnContext(
			ctx,
			"kubestore: rolling forward an intent its writer left",
			slog.String("txn", in.Txn),
			slog.Int("writes", len(in.Writes)),
		)
		// Bounded like the writer's own roll, so a taker that is suspended
		// wakes past its deadline instead of writing late.
		rctx, cancel := context.WithTimeout(ctx, s.o.WriterBudget)
		err = s.finish(rctx, head, in)
		cancel()
		if err != nil {
			return nil, err
		}
	}
}

func (s *Store) head(ctx context.Context) (*corev1.ConfigMap, error) {
	var head *corev1.ConfigMap
	err := s.call(ctx, func(ctx context.Context) error {
		var err error
		head, err = s.cms().Get(ctx, headName, metav1.GetOptions{})
		return apiErr(err)
	})
	if err != nil {
		return nil, fmt.Errorf("kubestore: get head: %w", err)
	}
	return head, nil
}

// commit writes in into the clear head it was read from. It is the commit
// point: either the head names in, or it does not and nothing was written.
func (s *Store) commit(ctx context.Context, head *corev1.ConfigMap, in intent) (*corev1.ConfigMap, error) {
	raw, err := json.Marshal(in)
	if err != nil {
		return nil, fmt.Errorf("kubestore: encode intent: %w", err)
	}
	next := head.DeepCopy()
	if next.Data == nil {
		next.Data = map[string]string{}
	}
	next.Data[dataIntent] = string(raw)
	var out *corev1.ConfigMap
	err = s.call(ctx, func(ctx context.Context) error {
		var err error
		out, err = s.cms().Update(ctx, next, metav1.UpdateOptions{})
		return apiErr(err)
	})
	if apierrors.IsConflict(err) {
		// A retried call whose first try landed sees its own intent.
		cur, gerr := s.head(ctx)
		if gerr == nil && holds(cur, in.Txn) {
			return cur, nil
		}
		return nil, errLostRace
	}
	if err != nil {
		return nil, fmt.Errorf("kubestore: commit: %w", err)
	}
	return out, nil
}

// finish rolls in forward and clears it from head.
func (s *Store) finish(ctx context.Context, head *corev1.ConfigMap, in intent) error {
	for _, w := range in.Writes {
		if err := s.roll(ctx, in.Txn, w); err != nil {
			return err
		}
	}
	next := head.DeepCopy()
	delete(next.Data, dataIntent)
	err := s.call(ctx, func(ctx context.Context) error {
		_, err := s.cms().Update(ctx, next, metav1.UpdateOptions{})
		return apiErr(err)
	})
	if apierrors.IsConflict(err) {
		cur, gerr := s.head(ctx)
		if gerr != nil {
			return gerr
		}
		if !holds(cur, in.Txn) {
			return nil
		}
		return s.finish(ctx, cur, in)
	}
	if err != nil {
		return fmt.Errorf("kubestore: clear head: %w", err)
	}
	return nil
}

func holds(head *corev1.ConfigMap, txn string) bool {
	raw, ok := head.Data[dataIntent]
	if !ok {
		return false
	}
	var in intent
	return json.Unmarshal([]byte(raw), &in) == nil && in.Txn == txn
}

// roll applies one write. It is idempotent: a write this txn already made, or
// one a later commit replaced, is left alone.
func (s *Store) roll(ctx context.Context, txn string, w write) error {
	if w.Purge {
		rv := w.RV
		err := s.call(ctx, func(ctx context.Context) error {
			return apiErr(
				s.cms().
					Delete(ctx, w.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{ResourceVersion: &rv}}),
			)
		})
		if err == nil || apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
			return nil
		}
		return fmt.Errorf("kubestore: purge %s: %w", w.Key, err)
	}
	cm := configMap(w, txn, s.o.Now())
	err := s.call(ctx, func(ctx context.Context) error {
		var err error
		if w.RV == "" {
			_, err = s.cms().Create(ctx, cm, metav1.CreateOptions{})
		} else {
			_, err = s.cms().Update(ctx, cm, metav1.UpdateOptions{})
		}
		return apiErr(err)
	})
	switch {
	case err == nil:
		return nil
	case apierrors.IsAlreadyExists(err), apierrors.IsConflict(err), apierrors.IsNotFound(err):
		var cur *corev1.ConfigMap
		gerr := s.call(ctx, func(ctx context.Context) error {
			var err error
			cur, err = s.cms().Get(ctx, w.Name, metav1.GetOptions{})
			return apiErr(err)
		})
		if gerr == nil && cur.Annotations[annTxn] == txn {
			return nil
		}
		if gerr != nil && !apierrors.IsNotFound(gerr) {
			return fmt.Errorf("kubestore: reread %s: %w", w.Key, gerr)
		}
		s.o.Log.WarnContext(
			ctx,
			"kubestore: write superseded before roll forward",
			slog.String("txn", txn),
			slog.String("key", w.Key),
			slog.String("err", err.Error()),
		)
		return nil
	default:
		return fmt.Errorf("kubestore: write %s: %w", w.Key, err)
	}
}

// purge removes tombstones older than TombstoneTTL, through the head so no
// commit can have read one it is about to delete.
func (s *Store) purge(ctx context.Context) error {
	return s.transact(ctx, func(ctx context.Context) ([]write, error) {
		sel := labelManagedBy + "=" + managedBy + "," + labelTombstone + "=true"
		var ws []write
		cont := ""
		for {
			var page *corev1.ConfigMapList
			err := s.call(ctx, func(ctx context.Context) error {
				var err error
				page, err = s.cms().List(ctx, metav1.ListOptions{LabelSelector: sel, Limit: listPage, Continue: cont})
				return apiErr(err)
			})
			if err != nil {
				return nil, fmt.Errorf("kubestore: list tombstones: %w", err)
			}
			for _, cm := range page.Items {
				at, err := time.Parse(time.RFC3339, cm.Annotations[annRemovedAt])
				if err == nil && s.o.Now().Sub(at) < s.o.TombstoneTTL {
					continue
				}
				ws = append(
					ws,
					write{
						Name:  cm.Name,
						Kind:  cm.Labels[labelRecord],
						Key:   cm.Annotations[annKey],
						RV:    cm.ResourceVersion,
						Purge: true,
					},
				)
			}
			if cont = page.Continue; cont == "" {
				break
			}
		}
		if len(ws) > 0 {
			s.o.Log.InfoContext(ctx, "kubestore: purging tombstones", slog.Int("count", len(ws)))
		}
		return ws, nil
	})
}

// stage is the Table store.Apply checks a change against. Reads go to the
// API server once per key; writes collect in order until the commit.
type stage struct {
	s     *Store
	order []*slot
	slots map[string]*slot
}

type slot struct {
	w       write
	cur     model.Record
	touched bool
}

func (st *stage) Get(ctx context.Context, key model.Key) (model.Record, error) {
	sl, err := st.slot(ctx, key)
	if err != nil {
		return nil, err
	}
	if sl.cur == nil {
		return nil, model.ErrNotFound
	}
	return sl.cur, nil
}

func (st *stage) Put(ctx context.Context, rec model.Record) error {
	sl, err := st.slot(ctx, rec.RecordKey())
	if err != nil {
		return err
	}
	raw, err := model.MarshalRecord(rec)
	if err != nil {
		return fmt.Errorf("kubestore: %w", err)
	}
	sl.w.Record, sl.cur, sl.touched = raw, rec, true
	sl.w.Live = live(rec)
	return nil
}

func (st *stage) Delete(ctx context.Context, key model.Key) error {
	sl, err := st.slot(ctx, key)
	if err != nil {
		return err
	}
	sl.w.Record, sl.w.Live, sl.cur, sl.touched = nil, false, nil, true
	return nil
}

// SetRefs and ClearRefs are no-ops: InFlight reads refs from the operations.
func (*stage) SetRefs(context.Context, model.OperationID, []model.Ref) error { return nil }
func (*stage) ClearRefs(context.Context, model.OperationID) error            { return nil }

func (st *stage) slot(ctx context.Context, key model.Key) (*slot, error) {
	name, err := recordName(key)
	if err != nil {
		return nil, err
	}
	if sl, ok := st.slots[name]; ok {
		return sl, nil
	}
	rec, rv, err := st.s.fetch(ctx, key)
	if err != nil {
		return nil, err
	}
	kind, err := kindOf(key)
	if err != nil {
		return nil, err
	}
	sl := &slot{w: write{Name: name, Kind: kind, Key: key.String(), RV: rv}, cur: rec}
	st.slots[name] = sl
	st.order = append(st.order, sl)
	return sl, nil
}

func (st *stage) writes() []write {
	var out []write
	for _, sl := range st.order {
		if sl.touched {
			out = append(out, sl.w)
		}
	}
	return out
}

func live(rec model.Record) bool {
	op, ok := rec.(model.Operation)
	return ok && !op.Terminal()
}

func newTxn() string {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("kubestore: crypto/rand: %v", err))
	}
	return hex.EncodeToString(b)
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return fmt.Errorf("kubestore: %w", ctx.Err())
	case <-t.C:
		return nil
	}
}

// call runs one API call under CallTimeout and retries transient failures
// with bounded backoff. The error is returned unwrapped so callers can test
// it with apierrors.
func (s *Store) call(ctx context.Context, fn func(context.Context) error) error {
	delay := 50 * time.Millisecond
	for attempt := 1; ; attempt++ {
		cctx, cancel := context.WithTimeout(ctx, s.o.CallTimeout)
		err := fn(cctx)
		cancel()
		if err == nil || attempt == maxCallAttempts || ctx.Err() != nil || !transient(err) {
			return err
		}
		if serr := sleep(ctx, delay); serr != nil {
			return err
		}
		delay *= 2
	}
}

// apiErr marks an error from the API server. apierrors still sees through it.
func apiErr(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("kubernetes: %w", err)
}

func transient(err error) bool {
	return apierrors.IsServerTimeout(err) || apierrors.IsTimeout(err) || apierrors.IsTooManyRequests(err) ||
		apierrors.IsServiceUnavailable(
			err,
		) || apierrors.IsInternalError(err) || errors.Is(err, context.DeadlineExceeded)
}
