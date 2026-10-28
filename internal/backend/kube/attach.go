package kube

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var vmGVR = schema.GroupVersionResource{Group: "kubevirt.io", Version: "v1", Resource: "virtualmachines"}

// managedLabel marks the VirtualMachines vws-compute owns.
const managedLabel = "vws.io/managed=true"

// Attachments maps a claim name to the name of the VirtualMachine whose spec
// uses it, boot disk and hot-plugged volumes alike. The VM spec is what
// vws-compute writes when it attaches a volume and what its own "already
// attached" check reads, so a stopped instance still holds its volumes. A
// VirtualMachine that is terminating has no instance left to report: compute
// destroys the machine when it deletes the instance, and the object stays
// behind its finalizers until the guest is gone.
func (b *Block) Attachments(ctx context.Context) (map[string]string, error) {
	l, err := b.dyn.Resource(vmGVR).Namespace(b.ns).List(ctx, metav1.ListOptions{LabelSelector: managedLabel})
	if err != nil {
		return nil, fmt.Errorf("kube: list virtual machines: %w", err)
	}
	out := map[string]string{}
	for _, vm := range l.Items {
		if vm.GetDeletionTimestamp() != nil {
			continue
		}
		vols, _, _ := unstructured.NestedSlice(vm.Object, "spec", "template", "spec", "volumes") //nolint:errcheck // a malformed spec has no claims
		for _, v := range vols {
			m, ok := v.(map[string]any)
			if !ok {
				continue
			}
			if claim, _, _ := unstructured.NestedString(m, "persistentVolumeClaim", "claimName"); claim != "" { //nolint:errcheck // absent is ""
				out[claim] = vm.GetName()
			}
		}
	}
	return out, nil
}
