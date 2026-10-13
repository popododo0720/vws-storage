package server_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	storagev1 "github.com/popododo0720/vws-proto/gen/go/vws/storage/v1"

	"github.com/popododo0720/vws-storage/internal/backend/fake"
	"github.com/popododo0720/vws-storage/internal/server"
)

// attachingBlock is the fake block backend plus attachments by claim name,
// the way the kube backend reads them from the VirtualMachine spec.
type attachingBlock struct {
	*fake.Block
	mu   sync.Mutex
	vms  map[string]string
	fail bool
}

func (b *attachingBlock) Attachments(context.Context) (map[string]string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.fail {
		return nil, errors.New("forbidden: list virtualmachines")
	}
	out := map[string]string{}
	for k, v := range b.vms {
		out[k] = v
	}
	return out, nil
}

func (b *attachingBlock) attach(claim, vm string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.vms[claim] = vm
}

func TestVolumeStatusInstanceFollowsTheAttachment(t *testing.T) {
	blk := &attachingBlock{Block: fake.NewBlock(), vms: map[string]string{}}
	vols, _, _, _, ops := dialBlock(t, "", server.Config{}, blk)
	ctx := context.Background()
	for _, id := range []string{"boot", "data"} {
		op, err := vols.CreateVolume(ctx, &storagev1.CreateVolumeRequest{
			Parent: "projects/local/zones/az-1", VolumeId: id,
			Volume: &storagev1.Volume{Spec: &storagev1.VolumeSpec{SizeGib: 1, Type: storagev1.VolumeType_VOLUME_TYPE_STANDARD}},
		})
		if err != nil {
			t.Fatal(err)
		}
		if done := waitOp(t, ops, op.GetName()); done.GetError() != nil {
			t.Fatal(done.GetError())
		}
	}
	get := func(id string) *storagev1.Volume {
		t.Helper()
		v, err := vols.GetVolume(ctx, &storagev1.GetVolumeRequest{Name: "projects/local/zones/az-1/volumes/" + id})
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	if inst := get("data").GetStatus().GetInstance(); inst != "" {
		t.Fatalf("detached volume reports instance %q", inst)
	}
	blk.attach(get("data").GetStatus().GetClaimName(), "t1vm")

	const want = "projects/local/zones/az-1/instances/t1vm"
	if inst := get("data").GetStatus().GetInstance(); inst != want {
		t.Fatalf("GetVolume instance %q, want %q", inst, want)
	}
	list, err := vols.ListVolumes(ctx, &storagev1.ListVolumesRequest{Parent: "projects/local/zones/az-1"})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, v := range list.GetVolumes() {
		got[v.GetName()] = v.GetStatus().GetInstance()
	}
	if got["projects/local/zones/az-1/volumes/data"] != want || got["projects/local/zones/az-1/volumes/boot"] != "" {
		t.Fatalf("ListVolumes instances %v", got)
	}

	// A backend that cannot answer (no RBAC) leaves the field empty; reads still work.
	blk.mu.Lock()
	blk.fail = true
	blk.mu.Unlock()
	if inst := get("data").GetStatus().GetInstance(); inst != "" {
		t.Fatalf("failed lookup reports %q", inst)
	}
}
