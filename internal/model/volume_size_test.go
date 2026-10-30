package model

import (
	"errors"
	"testing"
	"time"
)

// Each type has the EC2 CreateVolume and ModifyVolume size range: gp2 1 to
// 16384 GiB, gp3 1 to 65536, io1 4 to 16384, io2 4 to 65536, st1 and sc1 125
// to 16384, standard 1 to 1024.
func TestVolumeSizePerType(t *testing.T) {
	zone, err := ParseZone("az-1")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		typ        string
		gib        Bytes
		iops, mibs int64
		ok         bool
	}{
		{"standard", 1, 0, 0, true},
		{"standard", 1024, 0, 0, true},
		{"standard", 1025, 0, 0, false},
		{"gp2", 16384, 0, 0, true},
		{"gp2", 16385, 0, 0, false},
		{"gp3", 1, 3000, 125, true},
		{"gp3", 65536, 3000, 125, true},
		{"gp3", 65537, 3000, 125, false},
		{"io1", 3, 100, 0, false},
		{"io1", 4, 100, 0, true},
		{"io1", 16384, 100, 0, true},
		{"io1", 16385, 100, 0, false},
		{"io2", 3, 100, 0, false},
		{"io2", 4, 100, 0, true},
		{"io2", 65536, 100, 0, true},
		{"io2", 65537, 100, 0, false},
		{"st1", 124, 0, 0, false},
		{"st1", 125, 0, 0, true},
		{"st1", 16384, 0, 0, true},
		{"st1", 16385, 0, 0, false},
		{"sc1", 124, 0, 0, false},
		{"sc1", 125, 0, 0, true},
		{"sc1", 16385, 0, 0, false},
	} {
		req := CreateVolumeRequest{ID: NewVolumeID(), Spec: VolumeSpec{
			Size: tc.gib * GiB, Zone: zone, Type: mustType(t, tc.typ), Source: NoSource(),
			Iops: tc.iops, Throughput: tc.mibs,
		}}
		err := req.Validate()
		if (err == nil) != tc.ok || (err != nil && !errors.Is(err, ErrInvalidArgument)) {
			t.Errorf("%s %d GiB: err %v", tc.typ, tc.gib, err)
		}
	}
	if _, err := VolumeSize(65536); err != nil {
		t.Errorf("VolumeSize(65536): %v", err)
	}
	if _, err := VolumeSize(65537); err == nil {
		t.Error("VolumeSize(65537) accepted")
	}
}

// A volume grows only within its type's range, and a retype needs the size
// to fit the new type.
func TestExpandAndRetypeKeepTheTypeSizeRange(t *testing.T) {
	now := time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		typ  string
		from Bytes
		to   Bytes
		ok   bool
	}{
		{"gp3", 100, 65536, true},
		{"gp3", 100, 65537, false},
		{"gp2", 100, 16384, true},
		{"gp2", 100, 16385, false},
		{"standard", 100, 1024, true},
		{"standard", 100, 1025, false},
	} {
		vol := availableVolume(t, now)
		vol.Spec.Type, vol.Spec.Size = mustType(t, tc.typ), tc.from*GiB
		vol.Spec.Iops, vol.Spec.Throughput = 0, 0
		if tc.typ == "gp3" {
			vol.Spec.Iops, vol.Spec.Throughput = 3000, 125
		}
		avail, _ := vol.AsAvailable()
		req := ExpandVolumeRequest{Volume: vol.ID, Size: tc.to * GiB}
		err := req.Validate()
		if err == nil {
			_, err = avail.Expand(Start{Op: NewOperationID(), Now: now}, tc.to*GiB)
		}
		if (err == nil) != tc.ok || (err != nil && !errors.Is(err, ErrInvalidArgument)) {
			t.Errorf("expand %s %d to %d GiB: err %v", tc.typ, tc.from, tc.to, err)
		}
	}
	for _, tc := range []struct {
		from, to string
		gib      Bytes
		ok       bool
	}{
		{"gp3", "gp2", 20000, false},
		{"gp3", "io2", 20000, true},
		{"gp2", "st1", 124, false},
		{"gp2", "st1", 125, true},
		{"gp2", "standard", 1025, false},
		{"gp2", "io2", 3, false},
	} {
		vol := availableVolume(t, now)
		vol.Spec.Type, vol.Spec.Size = mustType(t, tc.from), tc.gib*GiB
		if tc.from == "gp3" {
			vol.Spec.Iops, vol.Spec.Throughput = 3000, 125
		}
		avail, _ := vol.AsAvailable()
		to := VolumeTarget{Labels: vol.Labels, Type: mustType(t, tc.to)}
		_, err := avail.Modify(Start{Op: NewOperationID(), Now: now}, to)
		if (err == nil) != tc.ok || (err != nil && !errors.Is(err, ErrInvalidArgument)) {
			t.Errorf("retype %d GiB %s to %s: err %v", tc.gib, tc.from, tc.to, err)
		}
	}
}
