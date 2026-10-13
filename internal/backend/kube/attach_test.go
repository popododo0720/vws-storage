package kube_test

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/popododo0720/vws-storage/internal/backend/kube"
	"github.com/popododo0720/vws-storage/internal/model"
)

var vmGVR = schema.GroupVersionResource{Group: "kubevirt.io", Version: "v1", Resource: "virtualmachines"}

// createVM writes a VirtualMachine shaped like vws-compute's: a boot claim and
// hot-plugged claims in spec.template.spec.volumes (vws-test 25h).
func createVM(t *testing.T, name string, managed bool, claims ...string) {
	t.Helper()
	var vols []any
	for i, c := range claims {
		pvc := map[string]any{"claimName": c}
		if i > 0 {
			pvc["hotpluggable"] = true
		}
		vols = append(vols, map[string]any{"name": "v" + name + string(rune('a'+i)), "persistentVolumeClaim": pvc})
	}
	vm := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "kubevirt.io/v1", "kind": "VirtualMachine",
		"metadata": map[string]any{"name": name, "namespace": ns},
		"spec":     map[string]any{"runStrategy": "Always", "template": map[string]any{"spec": map[string]any{"volumes": vols}}},
	}}
	if managed {
		vm.SetLabels(map[string]string{"vws.io/managed": "true"})
	}
	if _, err := dyn.Resource(vmGVR).Namespace(ns).Create(context.Background(), vm, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = dyn.Resource(vmGVR).Namespace(ns).Delete(context.Background(), name, metav1.DeleteOptions{})
	})
}

func TestAttachmentsReadTheVirtualMachineSpec(t *testing.T) {
	b := newBlock(kube.DefaultClasses())
	boot, data, free, foreign := model.NewVolumeID(), model.NewVolumeID(), model.NewVolumeID(), model.NewVolumeID()
	createVM(t, "t1vm", true, b.ClaimName(boot), b.ClaimName(data))
	createVM(t, "other", false, b.ClaimName(foreign))

	got, err := b.Attachments(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got[b.ClaimName(boot)] != "t1vm" || got[b.ClaimName(data)] != "t1vm" {
		t.Fatalf("attachments %v", got)
	}
	if _, ok := got[b.ClaimName(free)]; ok {
		t.Fatal("an unused claim is attached")
	}
	if _, ok := got[b.ClaimName(foreign)]; ok {
		t.Fatal("a VM vws-compute does not manage counts")
	}
}
