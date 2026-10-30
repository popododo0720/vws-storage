package model

import (
	"bytes"
	"errors"
	"strconv"
	"testing"
	"time"
)

// A target with a larger size admits one expansion that carries the type,
// performance and labels; the row is only guarded until Expanded stores all
// of it in one write, and the carried values survive the codec, so a restart
// mid-expansion finishes the same change (#87).
func TestModifyWithGrowthCarriesTheRestThroughExpansion(t *testing.T) {
	now := time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
	vol := availableVolume(t, now)
	vol.Spec.Size = 10 * GiB
	avail, _ := vol.AsAvailable()
	labels, err := ParseLabels(map[string]string{"team": "storage"})
	if err != nil {
		t.Fatal(err)
	}
	st := Start{Op: NewOperationID(), Now: now}
	_, err = avail.Modify(st, VolumeTarget{Labels: labels, Type: mustType(t, "st1")})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("10 GiB st1 without a size: %v", err)
	}
	_, err = avail.Modify(st, VolumeTarget{Labels: labels, Type: mustType(t, "gp2"), Size: 5 * GiB})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("shrink: %v", err)
	}
	ad, err := avail.Modify(st, VolumeTarget{
		Labels: labels, Type: mustType(t, "gp3"), Size: 20 * GiB,
		SetIops: true, Iops: 10000, SetThroughput: true, Throughput: 500,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := ad.Change.Writes()[0].(Guard); !ok {
		t.Fatal("admission wrote the volume row")
	}
	raw, err := MarshalRecord(ad.Op)
	if err != nil {
		t.Fatal(err)
	}
	back, err := UnmarshalRecord(raw)
	if err != nil {
		t.Fatal(err)
	}
	run, ok := AsRunning[VolumeExpansion](back.(Operation))
	if !ok {
		t.Fatalf("not a running expansion: %+v", back)
	}
	m := run.Work().Then
	if m == nil || m.Type != mustType(t, "gp3") || m.Iops != 10000 || m.Throughput != 500 || m.Labels != labels {
		t.Fatalf("work after codec %+v", run.Work())
	}
	ch, err := avail.Expanded(run, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	got := putNext(t, ch, "volume").(Volume)
	if got.Spec.Size != 20*GiB || got.Spec.Type != mustType(t, "gp3") || got.Spec.Iops != 10000 ||
		got.Spec.Throughput != 500 || got.Labels != labels || got.Meta.Version != vol.Meta.Version+1 {
		t.Fatalf("volume %+v", got)
	}

	// A size-only expansion writes no carried fields, as before #87.
	plain, err := avail.Expand(Start{Op: NewOperationID(), Now: now}, 20*GiB)
	if err != nil {
		t.Fatal(err)
	}
	raw, err = MarshalRecord(plain.Op)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte(`"then"`)) || bytes.Contains(raw, []byte(`"iops"`)) {
		t.Fatalf("size-only expansion %s", raw)
	}
}

// Performance alone changes in one write: SetIops with zero takes gp3's
// default, IOPS on a type that takes none is refused, and the same values
// only record the operation.
func TestModifySetsPerformance(t *testing.T) {
	now := time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
	vol := availableVolume(t, now)
	vol.Spec.Size, vol.Spec.Type, vol.Spec.Iops, vol.Spec.Throughput = 100*GiB, mustType(t, "gp3"), 6000, 250
	avail, _ := vol.AsAvailable()
	st := Start{Op: NewOperationID(), Now: now}
	for _, tc := range []struct {
		name       string
		to         VolumeTarget
		ok         bool
		iops, mibs int64
	}{
		{"iops", VolumeTarget{Type: vol.Spec.Type, SetIops: true, Iops: 12000}, true, 12000, 250},
		{"zero iops is the default", VolumeTarget{Type: vol.Spec.Type, SetIops: true}, true, 3000, 250},
		{"throughput", VolumeTarget{Type: vol.Spec.Type, SetThroughput: true, Throughput: 1000}, true, 6000, 1000},
		{"over 500 per GiB", VolumeTarget{Type: vol.Spec.Type, SetIops: true, Iops: 50001}, false, 0, 0},
		{"over 0.25 per iops", VolumeTarget{Type: vol.Spec.Type, SetThroughput: true, Throughput: 1501}, false, 0, 0},
		{"io2 needs iops", VolumeTarget{Type: mustType(t, "io2"), SetIops: true}, false, 0, 0},
		{"io2 with iops", VolumeTarget{Type: mustType(t, "io2"), SetIops: true, Iops: 64000}, true, 64000, 0},
		{"io2 with throughput", VolumeTarget{
			Type: mustType(t, "io2"), SetIops: true, Iops: 64000, SetThroughput: true, Throughput: 125,
		}, false, 0, 0},
		{"gp2 with iops", VolumeTarget{Type: mustType(t, "gp2"), SetIops: true, Iops: 3000}, false, 0, 0},
		{"gp2 drops gp3 performance", VolumeTarget{Type: mustType(t, "gp2")}, true, 0, 0},
	} {
		ad, err := avail.Modify(st, tc.to)
		if (err == nil) != tc.ok || (err != nil && !errors.Is(err, ErrInvalidArgument)) {
			t.Errorf("%s: err %v", tc.name, err)
			continue
		}
		if err != nil {
			continue
		}
		got := putNext(t, ad.Change, "volume").(Volume)
		if got.Spec.Iops != tc.iops || got.Spec.Throughput != tc.mibs || ad.Op.Phase() != PhaseSucceeded {
			t.Errorf("%s: spec %+v phase %s", tc.name, got.Spec, ad.Op.Phase())
		}
	}
	same, err := avail.Modify(st, VolumeTarget{
		Type: vol.Spec.Type, SetIops: true, Iops: 6000, SetThroughput: true, Throughput: 250,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := same.Change.Writes()[0].(Guard); !ok {
		t.Fatal("no-op performance change wrote the volume")
	}
}

// Size and performance are part of the request fingerprint; a request
// without them keeps the pre-#87 fingerprint, so stored tokens still match.
func TestModifyRequestFingerprintCoversSizeAndPerformance(t *testing.T) {
	id := NewVolumeID()
	base := ModifyVolumeRequest{Volume: id, SetLabels: true, Type: VolumeTypeGP2, Expected: 3}
	if got, want := base.fingerprint(), "modify-volume\n"+id.String()+"\n\ntrue\n-\n"+
		strconv.Itoa(int(VolumeTypeGP2))+"\n3"; got != want {
		t.Fatalf("fingerprint %q want %q", got, want)
	}
	seen := map[string]string{base.fingerprint(): "base"}
	for name, mut := range map[string]func(*ModifyVolumeRequest){
		"size":           func(r *ModifyVolumeRequest) { r.Size = 2 * GiB },
		"set iops":       func(r *ModifyVolumeRequest) { r.SetIops = true },
		"iops":           func(r *ModifyVolumeRequest) { r.SetIops, r.Iops = true, 4000 },
		"set throughput": func(r *ModifyVolumeRequest) { r.SetThroughput = true },
		"throughput":     func(r *ModifyVolumeRequest) { r.SetThroughput, r.Throughput = true, 200 },
	} {
		r := base
		mut(&r)
		f := r.fingerprint()
		if prev, dup := seen[f]; dup {
			t.Fatalf("%s has the fingerprint of %s", name, prev)
		}
		seen[f] = name
	}
	for _, bad := range []ModifyVolumeRequest{
		{Volume: id, Size: GiB / 2},
		{Volume: id, SetIops: true, Iops: -1},
		{Volume: id, SetThroughput: true, Throughput: -1},
	} {
		if bad.Validate() == nil {
			t.Fatalf("%+v validated", bad)
		}
	}
	for _, ok := range []ModifyVolumeRequest{
		{Volume: id, Size: 2 * GiB}, {Volume: id, SetIops: true}, {Volume: id, SetThroughput: true},
	} {
		if err := ok.Validate(); err != nil {
			t.Fatalf("%+v: %v", ok, err)
		}
	}
}
