package main

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientfake "k8s.io/client-go/kubernetes/fake"

	"github.com/popododo0720/vws-storage/internal/backend/kube"
	"github.com/popododo0720/vws-storage/internal/model"

	"github.com/popododo0720/vws-storage/internal/backend/fake"
)

func TestVolumeTypesConfigMapRequiresKubeBackend(t *testing.T) {
	stop, err := startVolumeTypes(context.Background(), fake.NewBlock(), "release", "")
	if err != nil {
		t.Fatal(err)
	}
	stop()
	if _, err := startVolumeTypes(context.Background(), fake.NewBlock(), "release", "types"); err == nil {
		t.Fatal("accepted ConfigMap for fake backend")
	}
}

func TestVolumeTypesPollsAndStops(t *testing.T) {
	ctx := context.Background()
	gp3, err := model.ParseVolumeType("gp3")
	if err != nil {
		t.Fatal(err)
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "types", Namespace: "release"},
		Data:       map[string]string{"gp3": "fast"},
	}
	kc := clientfake.NewClientset(cm)
	b := kube.NewBlock(kc, nil, "vms", kube.DefaultClasses(), "")
	stop, err := startVolumeTypes(ctx, b, "release", "types")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stop)
	if err := b.ServeType(gp3); err != nil {
		t.Fatalf("startup read: %v", err)
	}
	cm.Data["gp3"] = "ceph-block"
	if _, err := kc.CoreV1().ConfigMaps("release").Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(8 * time.Second)
	for b.Retype(model.VolumeTypeStandard, gp3) != nil {
		if time.Now().After(deadline) {
			t.Fatal("mapping did not reload without restart")
		}
		time.Sleep(10 * time.Millisecond)
	}
	stop()
	restarted := kube.NewBlock(kc, nil, "vms", kube.DefaultClasses(), "")
	stopAgain, err := startVolumeTypes(ctx, restarted, "release", "types")
	if err != nil {
		t.Fatal(err)
	}
	stopAgain()
	if err := restarted.Retype(model.VolumeTypeStandard, gp3); err != nil {
		t.Fatalf("restart read: %v", err)
	}
}
