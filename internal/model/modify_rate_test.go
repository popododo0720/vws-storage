package model

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

// The window keeps the newest four start times, drops the ones 24 hours old
// and names when the oldest counted one leaves it (#157).
func TestModificationsRollingWindow(t *testing.T) {
	t0 := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	var m Modifications
	for i := range ModificationLimit {
		if err := m.admit(t0.Add(time.Duration(i) * time.Hour)); err != nil {
			t.Fatalf("modification %d: %v", i+1, err)
		}
		m = m.add(t0.Add(time.Duration(i) * time.Hour))
	}
	err := m.admit(t0.Add(23 * time.Hour))
	const wait = "wait until 2026-11-02T00:00:00.000Z"
	if !errors.Is(err, ErrModificationRateExceeded) || !strings.Contains(err.Error(), wait) {
		t.Fatalf("fifth: %v", err)
	}
	if err := m.admit(t0.Add(ModificationWindow)); err != nil {
		t.Fatalf("after 24h: %v", err)
	}
	m = m.add(t0.Add(ModificationWindow))
	if got := m.times(); len(got) != ModificationLimit || !got[0].Equal(t0.Add(time.Hour)) {
		t.Fatalf("times %v", got)
	}
	// A start time after now (the clock moved back) still counts.
	if n := m.Count(t0); n != ModificationLimit {
		t.Fatalf("count before the window %d", n)
	}
}

// Modified survives the codec, an old row without it decodes empty, and a
// row with more than four times keeps the newest four.
func TestModifiedSurvivesCodec(t *testing.T) {
	t0 := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	vol := availableVolume(t, t0)
	vol.Modified = Modifications{}.add(t0).add(t0.Add(time.Hour))
	b, err := MarshalRecord(vol)
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnmarshalRecord(b)
	if err != nil {
		t.Fatal(err)
	}
	if !SameRecord(got, vol) || got.(Volume).Modified != vol.Modified {
		t.Fatalf("round trip %+v", got)
	}

	var env map[string]json.RawMessage
	if err := json.Unmarshal(b, &env); err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(env["body"], &body); err != nil {
		t.Fatal(err)
	}
	delete(body, "modified")
	old := reencode(t, env, body)
	dec, err := UnmarshalRecord(old)
	if err != nil {
		t.Fatal(err)
	}
	if dec.(Volume).Modified.Count(t0) != 0 {
		t.Fatalf("old row counted %v", dec.(Volume).Modified)
	}

	var six []any
	for i := range 6 {
		six = append(six, timeTo(t0.Add(time.Duration(i)*time.Hour)))
	}
	body["modified"] = six
	dec, err = UnmarshalRecord(reencode(t, env, body))
	if err != nil {
		t.Fatal(err)
	}
	if ts := dec.(Volume).Modified.times(); len(ts) != ModificationLimit || !ts[0].Equal(t0.Add(2*time.Hour)) {
		t.Fatalf("six stored times decode to %v", ts)
	}
}

func reencode(t *testing.T, env map[string]json.RawMessage, body map[string]any) []byte {
	t.Helper()
	nb, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	env["body"] = nb
	out, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// Modify counts size, type and performance changes and refuses the fifth;
// a labels-only change neither counts nor is refused (#157).
func TestModifyCountsOnlyEC2ModifyVolumeChanges(t *testing.T) {
	t0 := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	vol := availableVolume(t, t0)
	vol.Spec.Size = 100 * GiB
	vol.Spec.Type = mustType(t, "gp2")
	for i := range ModificationLimit {
		vol.Modified = vol.Modified.add(t0.Add(time.Duration(i) * time.Minute))
	}
	avail, _ := vol.AsAvailable()
	st := Start{Op: NewOperationID(), Now: t0.Add(time.Hour)}
	labels, err := ParseLabels(map[string]string{"a": "b"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := avail.Modify(st, VolumeTarget{Labels: labels, Type: vol.Spec.Type}); err != nil {
		t.Fatalf("labels only: %v", err)
	}
	for name, to := range map[string]VolumeTarget{
		"type": {Type: mustType(t, "gp3")},
		"size": {Type: vol.Spec.Type, Size: 200 * GiB},
	} {
		if _, err := avail.Modify(st, to); !errors.Is(err, ErrModificationRateExceeded) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := avail.Expand(st, 200*GiB); !errors.Is(err, ErrModificationRateExceeded) {
		t.Errorf("expand: %v", err)
	}
}
