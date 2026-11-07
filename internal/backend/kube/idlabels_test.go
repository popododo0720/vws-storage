package kube_test

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/popododo0720/vws-storage/internal/backend"
	"github.com/popododo0720/vws-storage/internal/backend/kube"
	"github.com/popododo0720/vws-storage/internal/model"
)

var vsGVR = schema.GroupVersionResource{Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshots"}

func project(t *testing.T, s string) model.Project {
	t.Helper()
	p, err := model.ParseProject(s)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func selectPVCs(t *testing.T, selector string) []corev1.PersistentVolumeClaim {
	t.Helper()
	opts := metav1.ListOptions{LabelSelector: selector}
	l, err := kc.CoreV1().PersistentVolumeClaims(ns).List(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	return l.Items
}

// The PVC and the VolumeSnapshot are found by their AWS ID through a label,
// and the ID read back from the object is the one asked for. The same
// client-chosen ID in two projects is two objects, told apart by the project
// label (#141 AC 3).
func TestVolumeAndSnapshotAreFoundByTheirAWSIDs(t *testing.T) {
	ctx := context.Background()
	b := newBlock(kube.DefaultClasses())
	bare := model.NewVolumeID()
	p1, p2 := project(t, "a111111111111"), project(t, "a222222222222")
	vols := []model.VolumeID{bare.WithProject(p1), bare.WithProject(p2)}
	for _, v := range vols {
		if err := b.CreateVolume(ctx, v, spec(t, model.VolumeTypeStandard, model.GiB)); err != nil {
			t.Fatal(err)
		}
	}
	if got := selectPVCs(t, kube.VolumeIDLabel+"="+bare.ID()); len(got) != 2 {
		t.Fatalf("claims of %s: %d, want one per project", bare.ID(), len(got))
	}
	for _, want := range vols {
		got := selectPVCs(t, kube.VolumeIDLabel+"="+want.ID()+","+kube.ProjectIDLabel+"="+want.Project().String())
		if len(got) != 1 {
			t.Fatalf("claims of %s: %d, want 1", want, len(got))
		}
		c := got[0]
		if c.Name != b.ClaimName(want) {
			t.Errorf("claim %s, want %s", c.Name, b.ClaimName(want))
		}
		fromLabels, err := model.ParseVolumeID(
			"projects/" + c.Labels[kube.ProjectIDLabel] + "/volumes/" + c.Labels[kube.VolumeIDLabel])
		if err != nil || fromLabels != want {
			t.Errorf("id from labels %v %v, want %v", fromLabels, err, want)
		}
		if fromAnn, err := model.ParseVolumeID(c.Annotations[kube.VolumeAnnotation]); err != nil || fromAnn != want {
			t.Errorf("id from annotation %v %v, want %v", fromAnn, err, want)
		}
	}

	snap := model.NewSnapshotID().WithProject(p1)
	if err := b.CreateSnapshot(ctx, snap, vols[0]); !errors.Is(err, backend.ErrBusy) {
		t.Fatalf("snapshot before ready: %v", err)
	}
	l, err := dyn.Resource(vsGVR).Namespace(ns).List(ctx, metav1.ListOptions{
		LabelSelector: kube.SnapshotIDLabel + "=" + snap.ID() + "," + kube.ProjectIDLabel + "=" + p1.String(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(l.Items) != 1 {
		t.Fatalf("snapshots of %s: %d, want 1", snap, len(l.Items))
	}
	vs := l.Items[0]
	labels := vs.GetLabels()
	fromLabels, err := model.ParseSnapshotID(
		"projects/" + labels[kube.ProjectIDLabel] + "/snapshots/" + labels[kube.SnapshotIDLabel])
	if err != nil || fromLabels != snap {
		t.Errorf("snapshot id from labels %v %v, want %v", fromLabels, err, snap)
	}
	if fromAnn, err := model.ParseSnapshotID(vs.GetAnnotations()[kube.SnapshotAnnotation]); err != nil || fromAnn != snap {
		t.Errorf("snapshot id from annotation %v %v, want %v", fromAnn, err, snap)
	}
}

// A Filesystem volume is a DataVolume, and CDI copies its labels to the
// claim it makes, so the DataVolume carries the ID labels.
func TestDataVolumeCarriesTheIDLabels(t *testing.T) {
	ctx := context.Background()
	b := kube.NewBlock(kc, dyn, ns, nfsClass(t, "nfs-csi"), "").WithVolumeMode(corev1.PersistentVolumeFilesystem)
	id := model.NewVolumeID().WithProject(project(t, "a333333333333"))
	if err := b.CreateVolume(ctx, id, spec(t, model.VolumeTypeStandard, model.GiB)); !errors.Is(err, backend.ErrBusy) {
		t.Fatalf("create: %v", err)
	}
	got := dataVolume(t, b, id).GetLabels()
	if got[kube.VolumeIDLabel] != id.ID() || got[kube.ProjectIDLabel] != id.Project().String() {
		t.Fatalf("data volume labels %v", got)
	}
}

// Two volume IDs never share a claim: a claim name is a hash of the full
// volume name, and a create that finds a claim made for another spec is a
// conflict, not a second volume on the same object.
func TestAClaimBelongsToOneVolume(t *testing.T) {
	ctx := context.Background()
	b := newBlock(kube.DefaultClasses())
	bare := model.NewVolumeID()
	p1, p2 := bare.WithProject(project(t, "a444444444444")), bare.WithProject(project(t, "a555555555555"))
	if b.ClaimName(p1) == b.ClaimName(p2) || b.ClaimName(p1) == b.ClaimName(bare) {
		t.Fatal("two volume names map to one claim")
	}
	if err := b.CreateVolume(ctx, p1, spec(t, model.VolumeTypeStandard, model.GiB)); err != nil {
		t.Fatal(err)
	}
	other := spec(t, model.VolumeTypeStandard, model.GiB)
	other.Zone = zoneNamed(t, "az-2")
	if err := b.CreateVolume(ctx, p1, other); !errors.Is(err, backend.ErrConflict) {
		t.Fatalf("second spec on one claim: %v, want ErrConflict", err)
	}
}

func zoneNamed(t *testing.T, s string) model.Zone {
	t.Helper()
	z, err := model.ParseZone(s)
	if err != nil {
		t.Fatal(err)
	}
	return z
}
