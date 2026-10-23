package model

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

// IOPS and throughput follow the per-type rules of vws-proto v0.5.0
// VolumeSpec, which are the EC2 CreateVolume rules.
func TestCreateVolumePerformanceRules(t *testing.T) {
	zone, err := ParseZone("az-1")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		typ        string
		iops, mibs int64
		ok         bool
	}{
		{"gp3", 3000, 125, true},
		{"gp3", 80000, 2000, true},
		{"gp3", 2999, 125, false},
		{"gp3", 80001, 125, false},
		{"gp3", 3000, 124, false},
		{"gp3", 3000, 2001, false},
		{"gp3", 0, 0, false}, // the server fills the defaults; the model never stores 0
		{"io1", 100, 0, true},
		{"io1", 64000, 0, true},
		{"io1", 99, 0, false},
		{"io1", 64001, 0, false},
		{"io1", 0, 0, false},
		{"io1", 1000, 125, false},
		{"io2", 100, 0, true},
		{"io2", 256000, 0, true},
		{"io2", 256001, 0, false},
		{"io2", 0, 0, false},
		{"io2", 1000, 125, false},
		{"gp2", 0, 0, true},
		{"gp2", 100, 0, false},
		{"gp2", 0, 125, false},
		{"standard", 0, 0, true},
		{"standard", 100, 0, false},
		{"st1", 0, 0, true},
		{"st1", 0, 125, false},
		{"sc1", 0, 0, true},
		{"sc1", 100, 0, false},
		{"gp3", -1, 125, false},
	} {
		req := CreateVolumeRequest{ID: NewVolumeID(), Spec: VolumeSpec{
			Size: GiB, Zone: zone, Type: mustType(t, tc.typ), Source: NoSource(),
			Iops: tc.iops, Throughput: tc.mibs,
		}}
		err := req.Validate()
		if (err == nil) != tc.ok || (err != nil && !errors.Is(err, ErrInvalidArgument)) {
			t.Errorf("%s iops=%d throughput=%d: err %v", tc.typ, tc.iops, tc.mibs, err)
		}
	}
}

// DefaultPerformance fills what EC2 fills when a field is left out: gp3 gets
// 3000 IOPS and 125 MiB/s; nothing else gets a value.
func TestDefaultPerformance(t *testing.T) {
	for _, tc := range []struct {
		typ                    string
		iops, mibs, wIops, wMb int64
	}{
		{"gp3", 0, 0, 3000, 125},
		{"gp3", 4000, 0, 4000, 125},
		{"gp3", 0, 500, 3000, 500},
		{"io1", 0, 0, 0, 0},
		{"io2", 500, 0, 500, 0},
		{"gp2", 0, 0, 0, 0},
	} {
		s := VolumeSpec{Type: mustType(t, tc.typ), Iops: tc.iops, Throughput: tc.mibs}.WithDefaultPerformance()
		if s.Iops != tc.wIops || s.Throughput != tc.wMb {
			t.Errorf("%s %d/%d: got %d/%d", tc.typ, tc.iops, tc.mibs, s.Iops, s.Throughput)
		}
	}
}

// IOPS and throughput survive the codec, and a spec without them stores no
// new field, so a row written for gp2 is byte-identical to a pre-#123 one.
func TestPerformanceSurvivesCodec(t *testing.T) {
	now := time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC)
	vol := availableVolume(t, now)
	vol.Spec.Type = mustType(t, "gp3")
	vol.Spec.Iops, vol.Spec.Throughput = 6000, 250
	raw, err := MarshalRecord(vol)
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnmarshalRecord(raw)
	if err != nil {
		t.Fatal(err)
	}
	if s := got.(Volume).Spec; s.Iops != 6000 || s.Throughput != 250 {
		t.Fatalf("spec %+v", s)
	}
	vol.Spec.Type, vol.Spec.Iops, vol.Spec.Throughput = mustType(t, "gp2"), 0, 0
	raw, err = MarshalRecord(vol)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte("iops")) || bytes.Contains(raw, []byte("throughput")) {
		t.Fatalf("gp2 row %s", raw)
	}
}

// A type change re-derives IOPS and throughput for the new type, as an EC2
// ModifyVolume without Iops or Throughput does: gp3 keeps an in-range value
// or takes its default, io1 and io2 need an in-range stored IOPS, and a type
// without settings stores none.
func TestModifyRederivesPerformance(t *testing.T) {
	now := time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		from             string
		iops, mibs       int64
		to               string
		ok               bool
		wantIops, wantMb int64
	}{
		{"gp2", 0, 0, "gp3", true, 3000, 125},
		{"gp3", 3000, 125, "gp2", true, 0, 0},
		{"io1", 5000, 0, "gp3", true, 5000, 125},
		{"io2", 100000, 0, "gp3", true, 3000, 125},
		{"io1", 5000, 0, "io2", true, 5000, 0},
		{"io2", 100000, 0, "io1", false, 0, 0},
		{"gp2", 0, 0, "io1", false, 0, 0},
		{"gp3", 6000, 500, "io2", true, 6000, 0},
		{"st1", 0, 0, "sc1", true, 0, 0},
		{"standard", 0, 0, "gp2", true, 0, 0},
	} {
		vol := availableVolume(t, now)
		vol.Spec.Type, vol.Spec.Iops, vol.Spec.Throughput = mustType(t, tc.from), tc.iops, tc.mibs
		avail, ok := vol.AsAvailable()
		if !ok {
			t.Fatal("not available")
		}
		st := Start{Op: NewOperationID(), Now: now}
		ad, err := avail.Modify(st, vol.Labels, mustType(t, tc.to))
		if (err == nil) != tc.ok {
			t.Errorf("%s->%s: err %v", tc.from, tc.to, err)
			continue
		}
		if err != nil {
			if !errors.Is(err, ErrInvalidArgument) {
				t.Errorf("%s->%s: err %v", tc.from, tc.to, err)
			}
			continue
		}
		var next Volume
		for _, w := range ad.Change.Writes() {
			if put, ok := w.(Put); ok {
				if v, ok := put.Next.(Volume); ok {
					next = v
				}
			}
		}
		if next.Spec.Type != mustType(t, tc.to) || next.Spec.Iops != tc.wantIops || next.Spec.Throughput != tc.wantMb {
			t.Errorf("%s->%s: spec %+v", tc.from, tc.to, next.Spec)
		}
	}
}
