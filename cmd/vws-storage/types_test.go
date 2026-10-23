package main

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/popododo0720/vws-storage/internal/backend/kube"
	"github.com/popododo0720/vws-storage/internal/model"
)

// -kube-volume-type maps one EBS type per flag. The legacy
// -kube-class-standard and -kube-class-ssd flags still map standard and gp2
// (the type ssd volumes read as), so an unchanged deployment serves what it
// served before (vws-storage#123).
func TestKubeVolumeTypeFlags(t *testing.T) {
	gp2, err := model.ParseVolumeType("gp2")
	if err != nil {
		t.Fatal(err)
	}
	gp3, err := model.ParseVolumeType("gp3")
	if err != nil {
		t.Fatal(err)
	}
	types := typeFlags{}
	if err := types.Set("gp3=fast-rbd/Block/fast-snap"); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"gp3=other", "nvme=x", "gp3"} {
		if err := types.Set(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	cfg := kube.DefaultConfig()
	applyTypes(&cfg, "hdd-class", "ssd-class", types)
	if cfg.Classes[model.VolumeTypeStandard] != "hdd-class" || cfg.Classes[gp2] != "ssd-class" || len(cfg.Classes) != 2 {
		t.Fatalf("classes %v", cfg.Classes)
	}
	want := kube.Class{StorageClass: "fast-rbd", VolumeMode: corev1.PersistentVolumeBlock, SnapshotClass: "fast-snap"}
	if cfg.Types[gp3] != want || len(cfg.Types) != 1 {
		t.Fatalf("types %v", cfg.Types)
	}
	if err := run([]string{"-kube-volume-type=nvme=x"}); err == nil {
		t.Fatal("run accepted an unknown type")
	}
}
