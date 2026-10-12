package kube_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"github.com/popododo0720/vws-storage/internal/backend"
	"github.com/popododo0720/vws-storage/internal/backend/kube"
	"github.com/popododo0720/vws-storage/internal/model"
)

func writeKubeconfig(t *testing.T) string {
	t.Helper()
	c := clientcmdapi.NewConfig()
	c.Clusters["envtest"] = &clientcmdapi.Cluster{Server: rc.Host, CertificateAuthorityData: rc.CAData}
	c.AuthInfos["envtest"] = &clientcmdapi.AuthInfo{ClientCertificateData: rc.CertData, ClientKeyData: rc.KeyData}
	c.Contexts["envtest"] = &clientcmdapi.Context{Cluster: "envtest", AuthInfo: "envtest"}
	c.CurrentContext = "envtest"
	path := filepath.Join(t.TempDir(), "kubeconfig")
	if err := clientcmd.WriteToFile(*c, path); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestOpenUsesConfiguredNames(t *testing.T) {
	cfg := kube.DefaultConfig()
	cfg.Kubeconfig = writeKubeconfig(t)
	cfg.Classes = kube.Classes{model.VolumeTypeStandard: "slow-class", model.VolumeTypeSSD: "fast-class"}
	cfg.SnapshotClass = "custom-snapclass"
	b, err := kube.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	id := model.NewVolumeID()
	if err := b.CreateVolume(ctx, id, spec(t, model.VolumeTypeSSD, model.GiB)); err != nil {
		t.Fatal(err)
	}
	pvcs, err := kc.CoreV1().PersistentVolumeClaims(cfg.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var got string
	for _, p := range pvcs.Items {
		if p.Annotations[kube.VolumeAnnotation] == id.String() {
			got = *p.Spec.StorageClassName
		}
	}
	if got != "fast-class" {
		t.Fatalf("storage class %q", got)
	}
	sid := model.NewSnapshotID()
	if err := b.CreateSnapshot(ctx, sid, id); !errors.Is(err, backend.ErrBusy) {
		t.Fatalf("snapshot before ready: %v", err)
	}
	class, _, err := unstructured.NestedString(snapshotFor(t, sid).Object, "spec", "volumeSnapshotClassName")
	if err != nil || class != "custom-snapclass" {
		t.Fatalf("snapshot class %q err %v", class, err)
	}
}

func TestOpenRejectsIncompleteConfig(t *testing.T) {
	cfg := kube.DefaultConfig()
	cfg.Kubeconfig = writeKubeconfig(t)
	for name, mut := range map[string]func(*kube.Config){
		"namespace":     func(c *kube.Config) { c.Namespace = "" },
		"snapshotClass": func(c *kube.Config) { c.SnapshotClass = "" },
		"ssdClass":      func(c *kube.Config) { c.Classes = kube.Classes{model.VolumeTypeStandard: "a"} },
		"kubeconfig":    func(c *kube.Config) { c.Kubeconfig = filepath.Join(t.TempDir(), "missing") },
	} {
		c := cfg
		mut(&c)
		if _, err := kube.Open(c); err == nil {
			t.Fatalf("%s: opened", name)
		}
	}
}
