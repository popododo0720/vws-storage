package server_test

import (
	"context"
	"testing"
	"time"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc/codes"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	commonv1 "github.com/popododo0720/vws-proto/gen/go/vws/common/v1"
	storagev1 "github.com/popododo0720/vws-proto/gen/go/vws/storage/v1"

	"github.com/popododo0720/vws-storage/internal/backend/kube"
	"github.com/popododo0720/vws-storage/internal/server"
)

const (
	refusedImage   = "projects/a123456789012/regions/vws-1/images/ami-0123456789abcdef0"
	refusedImageID = "ami-0123456789abcdef0"
	refusedMessage = "image " + refusedImage + " not found"
	imageNamespace = "vws-images"
)

var imageDataSourceGVR = schema.GroupVersionResource{Group: "cdi.kubevirt.io", Version: "v1beta1", Resource: "datasources"}

func TestRefusedImageCloneOperationIsNotFound(t *testing.T) {
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		{Group: "kubevirt.io", Version: "v1", Resource: "virtualmachines"}: "VirtualMachineList",
	})
	block := kube.NewBlock(nil, dyn, "volumes", kube.DefaultClasses(), "").WithImageNamespace(imageNamespace)
	vols, _, _, _, ops := dialBlock(t, "", server.Config{}, block)
	ctx := context.Background()

	var firstName string
	var firstMessage string
	check := func(id, requestID string) {
		t.Helper()
		op, err := vols.CreateVolume(ctx, imageVolumeRequest(id, requestID))
		if err != nil {
			t.Fatal(err)
		}
		done := waitRefused(ctx, t, ops, op.GetName())
		st := done.GetError()
		if st.GetCode() != int32(codes.NotFound) || st.GetMessage() != refusedMessage {
			t.Fatalf("%s: code %d message %q", id, st.GetCode(), st.GetMessage())
		}
		if firstMessage == "" {
			firstMessage = st.GetMessage()
			firstName = op.GetName()
		} else if st.GetMessage() != firstMessage {
			t.Fatalf("%s: message %q, first was %q", id, st.GetMessage(), firstMessage)
		}
		vol, err := vols.GetVolume(ctx, &storagev1.GetVolumeRequest{
			Name: "projects/a123456789012/zones/az-1/volumes/" + id,
		})
		if err != nil {
			t.Fatal(err)
		}
		obs := vol.GetStatus().GetObserved()
		if obs.GetState() != commonv1.ReconciliationState_RECONCILIATION_STATE_ERROR || obs.GetMessage() != refusedMessage {
			t.Fatalf("%s: observed %+v", id, obs)
		}
	}

	check("boot-absent", "11111111-1111-4111-8111-111111111111")

	again, err := vols.CreateVolume(ctx, imageVolumeRequest("boot-absent", "11111111-1111-4111-8111-111111111111"))
	if err != nil {
		t.Fatal(err)
	}
	if again.GetName() != firstName {
		t.Fatalf("replay name %s, want %s", again.GetName(), firstName)
	}
	replayed := waitRefused(ctx, t, ops, again.GetName())
	if replayed.GetError().GetCode() != int32(codes.NotFound) || replayed.GetError().GetMessage() != refusedMessage {
		t.Fatalf("replay code %d message %q", replayed.GetError().GetCode(), replayed.GetError().GetMessage())
	}

	putDataSource(t, dyn, map[string]string{kube.LabelProject: "b999999999999", kube.LabelRegion: "vws-1"})
	check("boot-foreign", "22222222-2222-4222-8222-222222222222")

	relabelDataSource(t, dyn, map[string]string{kube.LabelProject: "a123456789012", kube.LabelRegion: "vws-2"})
	check("boot-region", "33333333-3333-4333-8333-333333333333")
}

func waitRefused(ctx context.Context, t *testing.T, ops longrunningpb.OperationsClient, name string) *longrunningpb.Operation {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		got, err := ops.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: name})
		if err != nil {
			t.Fatal(err)
		}
		if got.GetDone() {
			return got
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("operation not done")
	return nil
}

func imageVolumeRequest(id, requestID string) *storagev1.CreateVolumeRequest {
	return &storagev1.CreateVolumeRequest{
		Parent: "projects/a123456789012/zones/az-1", VolumeId: id, RequestId: requestID,
		Volume: &storagev1.Volume{Spec: &storagev1.VolumeSpec{
			SizeGib: 1,
			Type:    storagev1.VolumeType_VOLUME_TYPE_SSD,
			Source:  &storagev1.VolumeSpec_Image{Image: refusedImage},
		}},
	}
}

func putDataSource(t *testing.T, dyn dynamic.Interface, labels map[string]string) {
	t.Helper()
	lab := map[string]any{}
	for k, v := range labels {
		lab[k] = v
	}
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "cdi.kubevirt.io/v1beta1",
		"kind":       "DataSource",
		"metadata": map[string]any{
			"name":      refusedImageID,
			"namespace": imageNamespace,
			"labels":    lab,
		},
		"spec": map[string]any{"source": map[string]any{"pvc": map[string]any{"name": refusedImageID, "namespace": imageNamespace}}},
	}}
	if _, err := dyn.Resource(imageDataSourceGVR).Namespace(imageNamespace).Create(context.Background(), obj, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func relabelDataSource(t *testing.T, dyn dynamic.Interface, labels map[string]string) {
	t.Helper()
	client := dyn.Resource(imageDataSourceGVR).Namespace(imageNamespace)
	obj, err := client.Get(context.Background(), refusedImageID, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	obj.SetLabels(labels)
	if _, err := client.Update(context.Background(), obj, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}
