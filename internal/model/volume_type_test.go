package model

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"
)

// ebsTypes are the EC2 EBS volume type names, in the vws-proto v0.5.0 order.
var ebsTypes = []string{"standard", "gp2", "gp3", "io1", "io2", "st1", "sc1"}

func mustType(t *testing.T, s string) VolumeType {
	t.Helper()
	typ, err := ParseVolumeType(s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return typ
}

// Every EBS type parses and prints under its own name (vws-storage#123).
func TestParseVolumeTypeAcceptsEveryEBSType(t *testing.T) {
	seen := map[VolumeType]string{}
	for _, name := range ebsTypes {
		typ := mustType(t, name)
		if typ.String() != name {
			t.Errorf("%q prints as %q", name, typ.String())
		}
		if other, dup := seen[typ]; dup {
			t.Errorf("%q and %q are the same type", name, other)
		}
		seen[typ] = name
	}
	for _, bad := range []string{"", "GP2", "io3", "magnetic", "ssd "} {
		if _, err := ParseVolumeType(bad); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%q: err %v", bad, err)
		}
	}
}

// ssd is the pre-#123 name of the one SSD type. It reads as gp2 and is never
// printed again.
func TestLegacySSDNameReadsAsGP2(t *testing.T) {
	if got := mustType(t, "ssd"); got != mustType(t, "gp2") {
		t.Fatalf("ssd parses as %q", got)
	}
}

// A volume row stored before #123 holds "ssd". It decodes as gp2, a rewrite
// stores "gp2", and decoding either form again gives the same volume: the
// migration is a pure read, so it is idempotent and safe across restarts.
func TestStoredSSDVolumeReadsAsGP2AndRewritesAsGP2(t *testing.T) {
	now := time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC)
	vol := availableVolume(t, now)
	vol.Spec.Type = mustType(t, "gp2")
	vol.Labels = Labels{}
	fresh, err := MarshalRecord(vol)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(fresh, []byte(`"type":"gp2"`)) {
		t.Fatalf("new row %s", fresh)
	}
	legacy := bytes.Replace(fresh, []byte(`"type":"gp2"`), []byte(`"type":"ssd"`), 1)
	got, err := UnmarshalRecord(legacy)
	if err != nil {
		t.Fatal(err)
	}
	read := got.(Volume)
	if read.Spec.Type != vol.Spec.Type {
		t.Fatalf("legacy row type %q", read.Spec.Type)
	}
	again, err := MarshalRecord(read)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(again, fresh) || bytes.Contains(again, []byte(`"ssd"`)) {
		t.Fatalf("rewrite %s want %s", again, fresh)
	}
	twice, err := UnmarshalRecord(again)
	if err != nil || !SameRecord(twice, read) {
		t.Fatalf("second read %+v err %v", twice, err)
	}
}

// An operation stored before #123 carries the spec, and a modify carries
// the type: both read "ssd" as gp2.
func TestStoredSSDOperationsReadAsGP2(t *testing.T) {
	now := time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC)
	vol := availableVolume(t, now)
	vol.Spec.Type = mustType(t, "gp2")
	gp2 := vol.Spec.Type
	for name, work := range map[string]Work{
		"create": VolumeCreation{Volume: vol.ID, Spec: vol.Spec},
		"modify": VolumeModification{Volume: vol.ID, Labels: Labels{}, Type: gp2},
	} {
		op := Operation{ID: NewOperationID(), Work: work, State: OpRunning{}, Meta: fresh(now)}
		raw, err := MarshalRecord(op)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(raw, []byte(`"gp2"`)) {
			t.Fatalf("%s: row %s", name, raw)
		}
		legacy := bytes.Replace(raw, []byte(`"gp2"`), []byte(`"ssd"`), 1)
		got, err := UnmarshalRecord(legacy)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !SameRecord(got, op) {
			t.Fatalf("%s: legacy op %+v want %+v", name, got, op)
		}
	}
}

// A create token stored before #123 holds the fingerprint of an "ssd"
// request. The same request as gp2 must still match it, and adding no IOPS
// or throughput must not change any fingerprint.
func TestGP2CreateFingerprintMatchesThePreUpgradeSSDOne(t *testing.T) {
	zone, err := ParseZone("az-1")
	if err != nil {
		t.Fatal(err)
	}
	id := NewVolumeID()
	spec := VolumeSpec{Size: GiB, Zone: zone, Type: mustType(t, "gp2"), Source: NoSource()}
	req := CreateVolumeRequest{ID: id, Spec: spec}
	legacy := "create-volume\n" + id.String() + "\n1073741824\naz-1\nssd\n" +
		NoSource().canonical() + "\n" + Labels{}.canonical()
	if got := req.fingerprint(); got != legacy {
		t.Fatalf("fingerprint %q want %q", got, legacy)
	}
	std := req
	std.Spec.Type = VolumeTypeStandard
	if strings.Contains(std.fingerprint(), "ssd") {
		t.Fatal("standard fingerprint names ssd")
	}
	// The modify fingerprint prints the type number: gp2 keeps ssd's number.
	if mustType(t, "gp2") != 2 {
		t.Fatalf("gp2 is %d, ssd was 2", mustType(t, "gp2"))
	}
}
