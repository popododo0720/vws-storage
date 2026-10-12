package main

import (
	"path/filepath"
	"testing"

	"github.com/popododo0720/vws-storage/internal/backend/kube"
)

func TestDefaultZoneIsVWS1A(t *testing.T) {
	zones, err := parseZones(defaultZones)
	if err != nil {
		t.Fatal(err)
	}
	if len(zones) != 1 || zones[0].String() != "vws-1a" {
		t.Fatalf("zones %v", zones)
	}
}

func TestBlockBackendSelection(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	for name, args := range map[string][]string{
		"unknown backend":    {"-block-backend=nope"},
		"kube without creds": {"-block-backend=kube", "-kube-config=" + missing},
	} {
		if err := run(args); err == nil {
			t.Fatalf("%s: run succeeded", name)
		}
	}
	blk, err := openBlock("fake", kube.DefaultConfig())
	if err != nil || blk == nil {
		t.Fatalf("fake: %v", err)
	}
}
