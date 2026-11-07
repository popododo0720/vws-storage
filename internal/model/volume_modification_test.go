package model

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func admittedOp(t *testing.T, ad Admitted) Operation {
	t.Helper()
	raw, err := MarshalRecord(ad.Op)
	if err != nil {
		t.Fatal(err)
	}
	back, err := UnmarshalRecord(raw)
	if err != nil {
		t.Fatal(err)
	}
	return back.(Operation)
}

// Admission stores the shape the volume had, an expansion carries it for the
// modification that rides on it, and the shape survives the codec (#173).
// Labels-only and no-op updates change no value of the shape, so they are not
// modifications.
func TestModificationRecordsTheOriginalShape(t *testing.T) {
	now := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	vol := availableVolume(t, now)
	vol.Spec.Size, vol.Spec.Type, vol.Spec.Iops, vol.Spec.Throughput = 10*GiB, mustType(t, "gp3"), 3000, 125
	avail, _ := vol.AsAvailable()
	from := vol.Spec.Shape()
	labels, err := ParseLabels(map[string]string{"team": "storage"})
	if err != nil {
		t.Fatal(err)
	}
	start := func() Start { return Start{Op: NewOperationID(), Now: now} }

	grow, err := avail.Expand(start(), 20*GiB)
	if err != nil {
		t.Fatal(err)
	}
	retype, err := avail.Modify(start(), VolumeTarget{Type: mustType(t, "gp2")})
	if err != nil {
		t.Fatal(err)
	}
	both, err := avail.Modify(start(), VolumeTarget{Type: mustType(t, "io2"), Size: 20 * GiB, SetIops: true, Iops: 8000})
	if err != nil {
		t.Fatal(err)
	}
	perf, err := avail.Modify(start(), VolumeTarget{Type: vol.Spec.Type, SetThroughput: true, Throughput: 250})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		op    Operation
		state ModificationState
		to    VolumeShape
	}{
		{
			"size", admittedOp(t, grow), ModificationModifying,
			VolumeShape{Size: 20 * GiB, Type: from.Type, Iops: 3000, Throughput: 125},
		},
		{"type", admittedOp(t, retype), ModificationCompleted, VolumeShape{Size: 10 * GiB, Type: mustType(t, "gp2")}},
		{
			"size and type", admittedOp(t, both), ModificationModifying,
			VolumeShape{Size: 20 * GiB, Type: mustType(t, "io2"), Iops: 8000},
		},
		{
			"throughput", admittedOp(t, perf), ModificationCompleted,
			VolumeShape{Size: 10 * GiB, Type: from.Type, Iops: 3000, Throughput: 250},
		},
	} {
		m, ok := tc.op.Modification()
		if !ok || m.From != from || m.To != tc.to || m.State != tc.state || m.Volume != vol.ID || m.Op != tc.op.ID ||
			!m.Start.Equal(now) || !m.Cause.IsZero() {
			t.Errorf("%s: %+v ok %v, want from %+v to %+v %v", tc.name, m, ok, from, tc.to, tc.state)
		}
		if tc.state == ModificationModifying && !m.End.IsZero() {
			t.Errorf("%s: end %v while modifying", tc.name, m.End)
		}
		if tc.state == ModificationCompleted && !m.End.Equal(now) {
			t.Errorf("%s: end %v, want the admission time", tc.name, m.End)
		}
	}
	if w := admittedOp(t, both).Work.(VolumeExpansion); !w.Then.From.IsZero() || w.From != from {
		t.Fatalf("a modification on an expansion carries From itself: %+v", w)
	}

	for name, to := range map[string]VolumeTarget{
		"labels only": {Type: vol.Spec.Type, Labels: labels},
		"no-op":       {Type: vol.Spec.Type},
		"same values": {Type: vol.Spec.Type, SetIops: true, Iops: 3000, SetThroughput: true, Throughput: 125, Size: 10 * GiB},
	} {
		ad, err := avail.Modify(start(), to)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if m, ok := admittedOp(t, ad).Modification(); ok {
			t.Errorf("%s is listed: %+v", name, m)
		}
	}
}

// The state follows the phase: waiting and running are modifying, success is
// completed with the end time, failure is failed with its cause. Work that is
// not a volume modification is never one.
func TestModificationStateFollowsThePhase(t *testing.T) {
	t0 := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	vol := availableVolume(t, t0)
	op := Operation{
		ID: NewOperationID(), Work: VolumeExpansion{Volume: vol.ID, Size: 2 * GiB, From: vol.Spec.Shape()},
		State: OpRunning{}, Meta: fresh(t0),
	}
	cause := Status{Code: CodeInternal, Message: "boom"}
	for _, tc := range []struct {
		state OpState
		want  ModificationState
		end   time.Time
		cause Status
	}{
		{OpRunning{}, ModificationModifying, time.Time{}, Status{}},
		{OpRunning{Attempts: 2, LastError: cause}, ModificationModifying, time.Time{}, Status{}},
		{OpSucceeded{}, ModificationCompleted, t0.Add(time.Minute), Status{}},
		{OpFailed{Cause: cause}, ModificationFailed, t0.Add(time.Minute), cause},
	} {
		o := op
		o.State = tc.state
		o.Meta.UpdateTime = t0.Add(time.Minute)
		m, ok := o.Modification()
		if !ok || m.State != tc.want || !m.End.Equal(tc.end) || m.Cause != tc.cause {
			t.Errorf("%T: %+v ok %v", tc.state, m, ok)
		}
	}
	for _, w := range []Work{VolumeDeletion{Volume: vol.ID}, VolumeCreation{Volume: vol.ID, Spec: vol.Spec}} {
		o := op
		o.Work = w
		if _, ok := o.Modification(); ok {
			t.Errorf("%T is listed", w)
		}
	}
	if _, ok := (Operation{}).Modification(); ok {
		t.Error("an empty operation is listed")
	}
}

func operationJSON(id OperationID, work string) []byte {
	return fmt.Appendf(nil,
		`{"record":"operation","body":{"id":%q,"work":%s,"state":{"kind":"succeeded"},"claim":{"token":"","hash":""},`+
			`"meta":{"version":2,"create":1793491200000000,"update":1793494800000000}}}`,
		id.wire(), work)
}

// Operations written before #173 have no original shape. They decode with
// From zero and are still listed, with the original unknown (zero) and, for a
// modify-volume, no target size.
func TestOperationsWrittenBefore173DecodeAndAreListed(t *testing.T) {
	vol := NewVolumeID()
	expand := operationJSON(NewOperationID(),
		fmt.Sprintf(`{"kind":"expand-volume","volume":%q,"size":2147483648,"type":"ssd","then":true,`+
			`"labels":{"a":"b"}}`, vol.wire()))
	modify := operationJSON(NewOperationID(),
		fmt.Sprintf(`{"kind":"modify-volume","volume":%q,"type":"gp3","iops":3000,"throughput_mibps":125}`, vol.wire()))
	plain := operationJSON(NewOperationID(),
		fmt.Sprintf(`{"kind":"expand-volume","volume":%q,"size":4294967296}`, vol.wire()))
	want := []VolumeModificationStatus{
		{Volume: vol, To: VolumeShape{Size: 2 * GiB, Type: VolumeTypeGP2}},
		{Volume: vol, To: VolumeShape{Type: VolumeTypeGP3, Iops: 3000, Throughput: 125}},
		{Volume: vol, To: VolumeShape{Size: 4 * GiB}},
	}
	for i, raw := range [][]byte{expand, modify, plain} {
		rec, err := UnmarshalRecord(raw)
		if err != nil {
			t.Fatalf("%s: %v", raw, err)
		}
		op := rec.(Operation)
		m, ok := op.Modification()
		w := want[i]
		w.Op, w.State = op.ID, ModificationCompleted
		w.Start, w.End = op.Meta.CreateTime, op.Meta.UpdateTime
		if !ok || m != w || !m.From.IsZero() || m.Start.IsZero() || !m.End.After(m.Start) {
			t.Errorf("%s:\n got %+v ok %v\n want %+v", raw, m, ok, w)
		}
		again, err := MarshalRecord(op)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(again, []byte(`"from"`)) {
			t.Errorf("an operation without From writes it: %s", again)
		}
	}
}

// A build from before #173 reads an operation written now: its own decoder
// ignores "from" and still sees the target values.
func TestOlderBuildReadsAnOperationWithFrom(t *testing.T) {
	now := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	vol := availableVolume(t, now)
	vol.Spec.Size, vol.Spec.Type, vol.Spec.Iops, vol.Spec.Throughput = 10*GiB, mustType(t, "gp2"), 0, 0
	avail, _ := vol.AsAvailable()
	ad, err := avail.Modify(Start{Op: NewOperationID(), Now: now}, VolumeTarget{
		Type: mustType(t, "io2"), Size: 20 * GiB, SetIops: true, Iops: 8000,
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := MarshalRecord(ad.Op)
	if err != nil {
		t.Fatal(err)
	}
	var env struct {
		Body struct {
			Work json.RawMessage `json:"work"`
		} `json:"body"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(env.Body.Work, []byte(`"from":{"size":10737418240,"type":"ssd"}`)) {
		t.Fatalf("work %s: gp2 is stored as ssd and zero IOPS are omitted", env.Body.Work)
	}
	// workWire as it was before #173.
	var old struct {
		Kind        string            `json:"kind"`
		Volume      string            `json:"volume"`
		Snapshot    string            `json:"snapshot"`
		Bucket      string            `json:"bucket"`
		Key         string            `json:"key"`
		Generation  string            `json:"generation"`
		Size        int64             `json:"size"`
		Zone        string            `json:"zone"`
		Type        string            `json:"type"`
		Source      string            `json:"source"`
		SHA256      string            `json:"sha256"`
		ContentType string            `json:"content_type"`
		Labels      map[string]string `json:"labels,omitempty"`
		Replaces    string            `json:"replaces,omitempty"`
		Reclaim     bool              `json:"reclaim,omitempty"`
		Deadline    int64             `json:"deadline"`
		Iops        int64             `json:"iops,omitempty"`
		Throughput  int64             `json:"throughput_mibps,omitempty"`
		Then        bool              `json:"then,omitempty"`
	}
	if err := json.Unmarshal(env.Body.Work, &old); err != nil {
		t.Fatal(err)
	}
	if old.Kind != "expand-volume" || old.Size != int64(20*GiB) || old.Type != "io2" || old.Iops != 8000 ||
		!old.Then || old.Volume != vol.ID.wire() {
		t.Fatalf("older build sees %+v", old)
	}
}
