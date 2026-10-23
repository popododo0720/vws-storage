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

// For one release a gp2 volume is stored under the pre-#123 name "ssd", so a
// build from before #123 can still read it after a rollback. Both names read
// as gp2, and a write always stores "ssd": reading and writing again is
// byte-identical, so the round trip is idempotent.
func TestStoredSSDVolumeReadsAsGP2AndIsWrittenAsSSD(t *testing.T) {
	now := time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC)
	vol := availableVolume(t, now)
	vol.Spec.Type = mustType(t, "gp2")
	vol.Labels = Labels{}
	row, err := MarshalRecord(vol)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(row, []byte(`"type":"ssd"`)) || bytes.Contains(row, []byte(`"gp2"`)) {
		t.Fatalf("gp2 row %s", row)
	}
	later := bytes.Replace(row, []byte(`"type":"ssd"`), []byte(`"type":"gp2"`), 1)
	for name, raw := range map[string][]byte{"ssd": row, "gp2": later} {
		got, err := UnmarshalRecord(raw)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		read := got.(Volume)
		if read.Spec.Type != vol.Spec.Type {
			t.Fatalf("%s row type %q", name, read.Spec.Type)
		}
		again, err := MarshalRecord(read)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(again, row) {
			t.Fatalf("%s rewrite %s want %s", name, again, row)
		}
	}
	vol.Spec.Type, vol.Spec.Iops, vol.Spec.Throughput = mustType(t, "gp3"), 3000, 125
	if row, err := MarshalRecord(vol); err != nil || !bytes.Contains(row, []byte(`"type":"gp3"`)) {
		t.Fatalf("gp3 row %s err %v", row, err)
	}
}

// Operations carry the type too (a create its spec, a modify its type): gp2
// is stored as "ssd", and both names read as gp2.
func TestGP2OperationsAreStoredAsSSD(t *testing.T) {
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
		if !bytes.Contains(raw, []byte(`"ssd"`)) || bytes.Contains(raw, []byte(`"gp2"`)) {
			t.Fatalf("%s: row %s", name, raw)
		}
		later := bytes.Replace(raw, []byte(`"ssd"`), []byte(`"gp2"`), 1)
		for form, b := range map[string][]byte{"ssd": raw, "gp2": later} {
			got, err := UnmarshalRecord(b)
			if err != nil {
				t.Fatalf("%s %s: %v", name, form, err)
			}
			if !SameRecord(got, op) {
				t.Fatalf("%s %s: op %+v want %+v", name, form, got, op)
			}
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
