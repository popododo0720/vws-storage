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

func TestObjectBackendSelection(t *testing.T) {
	t.Setenv("VWS_S3_ACCESS_KEY", "")
	t.Setenv("VWS_S3_SECRET_KEY", "")
	for name, args := range map[string][]string{
		"unknown backend": {"-object-backend=nope"},
		"unknown profile": {"-object-backend=s3", "-s3-profile=ceph", "-s3-endpoint=http://127.0.0.1:1"},
		"no endpoint":     {"-object-backend=s3"},
		"no keys":         {"-object-backend=s3", "-s3-endpoint=http://127.0.0.1:1"},
	} {
		if err := run(args); err == nil {
			t.Fatalf("%s: run succeeded", name)
		}
	}
	t.Setenv("VWS_S3_ACCESS_KEY", "k")
	t.Setenv("VWS_S3_SECRET_KEY", "s")
	o, err := openObjects("s3", "", "minio", "http://127.0.0.1:1", "")
	if err != nil || o.handler != nil || o.object == nil || o.signer == nil {
		t.Fatalf("s3: %+v %v", o, err)
	}
	f, err := openObjects("fake", "127.0.0.1:0", "", "", "")
	if err != nil || f.handler == nil {
		t.Fatalf("fake: %+v %v", f, err)
	}
}
