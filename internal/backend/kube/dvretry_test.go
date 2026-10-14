package kube_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/popododo0720/vws-storage/internal/backend"
	"github.com/popododo0720/vws-storage/internal/backend/kube"
	"github.com/popododo0720/vws-storage/internal/model"
)

const attemptAnnotation = "vws.popododo0720.io/dv-attempt"

// setPhase plays CDI: no controller runs in envtest.
func setPhase(t *testing.T, b *kube.Block, id model.VolumeID, phase string) {
	t.Helper()
	dv := dataVolume(t, b, id)
	if err := unstructured.SetNestedField(dv.Object, phase, "status", "phase"); err != nil {
		t.Fatal(err)
	}
	if _, err := dyn.Resource(dvGVR).Namespace(ns).Update(context.Background(), dv, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func fsBlock(retries int, timeout time.Duration) *kube.Block {
	return kube.NewBlock(kc, dyn, ns, kube.DefaultClasses(), "").
		WithVolumeMode(corev1.PersistentVolumeFilesystem).WithDataVolumeRetry(retries, timeout)
}

func createBusy(t *testing.T, b *kube.Block, id model.VolumeID, s model.VolumeSpec) error {
	t.Helper()
	err := b.CreateVolume(context.Background(), id, s)
	if err != nil && !errors.Is(err, backend.ErrBusy) {
		return err
	}
	return err
}

// A Failed DataVolume is deleted and made again (CDI rejects spec updates and
// documents delete and recreate). Two failures within the limit of 3 retries
// end in a volume once CDI succeeds.
func TestFailedDataVolumeIsRecreatedUntilItSucceeds(t *testing.T) {
	b := fsBlock(3, time.Hour)
	id := model.NewVolumeID()
	s := spec(t, model.VolumeTypeStandard, model.GiB)
	_ = createBusy(t, b, id, s)
	firstUID := dataVolume(t, b, id).GetUID()
	for want := 2; want <= 3; want++ {
		setPhase(t, b, id, "Failed")
		if err := createBusy(t, b, id, s); !errors.Is(err, backend.ErrBusy) || backend.IsPermanent(err) {
			t.Fatalf("failure %d: %v", want-1, err)
		}
		dv := dataVolume(t, b, id)
		if dv.GetUID() == firstUID {
			t.Fatal("the Failed DataVolume was not recreated")
		}
		if got := dv.GetAnnotations()[attemptAnnotation]; got != string(rune('0'+want)) {
			t.Fatalf("attempt annotation %q, want %d", got, want)
		}
	}
	setPhase(t, b, id, "Succeeded")
	if err := b.CreateVolume(context.Background(), id, s); err != nil {
		t.Fatalf("after CDI succeeded: %v", err)
	}
}

// Past the retry limit the volume fails for good, with the reason.
func TestFailedDataVolumePastTheRetryLimitIsPermanent(t *testing.T) {
	b := fsBlock(1, time.Hour)
	id := model.NewVolumeID()
	s := spec(t, model.VolumeTypeStandard, model.GiB)
	_ = createBusy(t, b, id, s)
	setPhase(t, b, id, "Failed")
	if err := createBusy(t, b, id, s); !errors.Is(err, backend.ErrBusy) {
		t.Fatalf("first failure is retried: %v", err)
	}
	setPhase(t, b, id, "Failed")
	err := b.CreateVolume(context.Background(), id, s)
	if !backend.IsPermanent(err) || !errors.Is(err, backend.ErrGaveUp) || !strings.Contains(err.Error(), "failed 2 times") {
		t.Fatalf("past the limit: %v", err)
	}
}

// A DataVolume that has not succeeded within the timeout fails for good,
// whatever its phase, and the clock survives a recreate.
func TestDataVolumePastTheTimeoutIsPermanent(t *testing.T) {
	b := fsBlock(3, 300*time.Millisecond)
	id := model.NewVolumeID()
	s := spec(t, model.VolumeTypeStandard, model.GiB)
	_ = createBusy(t, b, id, s)
	setPhase(t, b, id, "Failed")
	if err := createBusy(t, b, id, s); !errors.Is(err, backend.ErrBusy) {
		t.Fatalf("within the timeout: %v", err)
	}
	time.Sleep(400 * time.Millisecond)
	setPhase(t, b, id, "ImportInProgress")
	err := b.CreateVolume(context.Background(), id, s)
	if !backend.IsPermanent(err) || !errors.Is(err, backend.ErrGaveUp) || !strings.Contains(err.Error(), "within 300ms") {
		t.Fatalf("past the timeout: %v", err)
	}
}
