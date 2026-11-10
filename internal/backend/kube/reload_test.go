package kube_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientfake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"github.com/popododo0720/vws-storage/internal/backend"
	"github.com/popododo0720/vws-storage/internal/backend/kube"
	"github.com/popododo0720/vws-storage/internal/model"
)

func TestReloadVolumeTypesChangeInvalidAndDelete(t *testing.T) {
	ctx := context.Background()
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "vws-volume-types", Namespace: "release"},
		Data:       map[string]string{"gp3": "fast/Block/snap"},
	}
	kc := clientfake.NewClientset(cm)
	b := kube.NewBlock(kc, nil, "vms", kube.DefaultClasses(), "")
	gp3 := vtype(t, "gp3")
	update := func(data map[string]string) {
		t.Helper()
		cm.Data = data
		if _, err := kc.CoreV1().ConfigMaps("release").Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	reload := func(wantChanged bool, wantErr bool) {
		t.Helper()
		changed, err := b.ReloadVolumeTypes(ctx, "release", cm.Name)
		if changed != wantChanged || (err != nil) != wantErr {
			t.Fatalf("reload = %v, %v", changed, err)
		}
	}
	if err := b.ServeType(gp3); !errors.Is(err, backend.ErrInvalid) {
		t.Fatalf("unmapped = %v", err)
	}
	reload(true, false)
	if err := b.ServeType(gp3); err != nil {
		t.Fatal(err)
	}
	reload(false, false)
	update(map[string]string{"gp3": "ceph-block"})
	reload(true, false)
	if err := b.Retype(model.VolumeTypeStandard, gp3); err != nil {
		t.Fatalf("changed mapping: %v", err)
	}
	for _, bad := range []map[string]string{
		{"gp3": "bad/Raw"},
		{"unknown": "ceph-block"},
		{"gp3": ""},
		{"gp2": "ceph-block", "ssd": "other"},
	} {
		update(bad)
		reload(false, true)
		if err := b.Retype(model.VolumeTypeStandard, gp3); err != nil {
			t.Fatalf("last good mapping lost: %v", err)
		}
	}
	if err := kc.CoreV1().ConfigMaps("release").Delete(ctx, cm.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	reload(false, true)
	if err := b.Retype(model.VolumeTypeStandard, gp3); err != nil {
		t.Fatalf("deleted ConfigMap lost last good mapping: %v", err)
	}
	cm.Data = map[string]string{}
	if _, err := kc.CoreV1().ConfigMaps("release").Create(ctx, cm, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	reload(true, false)
	if err := b.ServeType(gp3); !errors.Is(err, backend.ErrInvalid) {
		t.Fatalf("removed mapping = %v", err)
	}
	for _, typ := range []model.VolumeType{model.VolumeTypeStandard, model.VolumeTypeGP2} {
		if err := b.ServeType(typ); err != nil {
			t.Fatalf("legacy fallback lost: %v", err)
		}
	}
	for _, action := range kc.Actions() {
		if action.GetVerb() == "list" || action.GetVerb() == "watch" {
			t.Fatalf("reader requires wider RBAC: %v", action)
		}
		if action.GetVerb() == "get" && action.GetNamespace() != "release" {
			t.Fatalf("read wrong namespace: %v", action)
		}
	}
}

func TestReloadVolumeTypesReadFailureKeepsFlags(t *testing.T) {
	kc := clientfake.NewClientset()
	kc.PrependReactor("get", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("unavailable")
	})
	b := kube.NewBlock(kc, nil, "vms", kube.DefaultClasses(), "").
		WithClass(vtype(t, "gp3"), kube.Class{StorageClass: "fast"})
	if changed, err := b.ReloadVolumeTypes(context.Background(), "release", "vws-volume-types"); err == nil || changed {
		t.Fatalf("read failure = %v, %v", changed, err)
	}
	if err := b.ServeType(vtype(t, "gp3")); err != nil {
		t.Fatal(err)
	}
}

func TestReloadVolumeTypesConcurrentReaders(t *testing.T) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "types", Namespace: "release"},
		Data:       map[string]string{"gp3": "ceph-block"},
	}
	kc := clientfake.NewClientset(cm)
	gp3 := vtype(t, "gp3")
	b := kube.NewBlock(kc, nil, "vms", kube.DefaultClasses(), "")
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Go(func() {
			for j := 0; j < 100; j++ {
				_, _ = b.ReloadVolumeTypes(context.Background(), "release", "types")
				_ = b.ServeType(gp3)
				_ = b.Retype(model.VolumeTypeStandard, gp3)
			}
		})
	}
	wg.Wait()
}
