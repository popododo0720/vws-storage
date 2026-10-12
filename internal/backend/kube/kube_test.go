package kube_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/popododo0720/vws-storage/internal/backend"
	"github.com/popododo0720/vws-storage/internal/backend/fake"
	"github.com/popododo0720/vws-storage/internal/backend/kube"
	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/storage"
)

const ns = "default"

var (
	kc  kubernetes.Interface
	dyn dynamic.Interface
	rc  *rest.Config
)

func newBlock(c kube.Classes) *kube.Block { return kube.NewBlock(kc, dyn, ns, c, "vws-snapclass") }

func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		os.Stderr.WriteString("KUBEBUILDER_ASSETS unset: skipping envtest tests\n")
		os.Exit(0)
	}
	env := &envtest.Environment{CRDDirectoryPaths: []string{"testdata"}, ErrorIfCRDPathMissing: true}
	cfg, err := env.Start()
	if err != nil {
		panic(err)
	}
	rc = cfg
	kc = kubernetes.NewForConfigOrDie(cfg)
	dyn = dynamic.NewForConfigOrDie(cfg)
	expand := true
	sc := &storagev1.StorageClass{
		ObjectMeta:           metav1.ObjectMeta{Name: kube.DefaultClasses()[model.VolumeTypeStandard]},
		Provisioner:          "rbd.csi.ceph.com",
		AllowVolumeExpansion: &expand,
	}
	if _, err := kc.StorageV1().StorageClasses().Create(context.Background(), sc, metav1.CreateOptions{}); err != nil {
		panic(err)
	}
	code := m.Run()
	_ = env.Stop()
	os.Exit(code)
}

func zone(t *testing.T) model.Zone {
	t.Helper()
	z, err := model.ParseZone("az-1")
	if err != nil {
		t.Fatal(err)
	}
	return z
}

func spec(t *testing.T, typ model.VolumeType, size model.Bytes) model.VolumeSpec {
	return model.VolumeSpec{Size: size, Zone: zone(t), Type: typ, Source: model.NoSource()}
}

// bind marks the claim Bound. No controller runs in envtest, and the API
// server only accepts a resize on a bound claim.
func bind(t *testing.T, id model.VolumeID) {
	t.Helper()
	pvcs := kc.CoreV1().PersistentVolumeClaims(ns)
	l, err := pvcs.List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range l.Items {
		if p.Annotations[kube.VolumeAnnotation] == id.String() {
			p.Status.Phase = corev1.ClaimBound
			if _, err := pvcs.UpdateStatus(context.Background(), &p, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			return
		}
	}
	t.Fatal("no claim to bind")
}

func claimFor(t *testing.T, id model.VolumeID) *corev1.PersistentVolumeClaim {
	t.Helper()
	l, err := kc.CoreV1().PersistentVolumeClaims(ns).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range l.Items {
		if p.Annotations[kube.VolumeAnnotation] == id.String() {
			return &p
		}
	}
	return nil
}

func claims(t *testing.T) int {
	t.Helper()
	l, err := kc.CoreV1().PersistentVolumeClaims(ns).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return len(l.Items)
}

func TestCreateVolumeMapsTypeToStorageClass(t *testing.T) {
	classes := kube.Classes{model.VolumeTypeStandard: "hdd-class", model.VolumeTypeSSD: "ssd-class"}
	b := newBlock(classes)
	id := model.NewVolumeID()
	if err := b.CreateVolume(context.Background(), id, spec(t, model.VolumeTypeSSD, 2*model.GiB)); err != nil {
		t.Fatal(err)
	}
	l, err := kc.CoreV1().PersistentVolumeClaims(ns).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, p := range l.Items {
		if p.Annotations[kube.VolumeAnnotation] != id.String() {
			continue
		}
		found = true
		if *p.Spec.StorageClassName != "ssd-class" || p.Spec.Resources.Requests.Storage().Value() != int64(2*model.GiB) {
			t.Fatalf("class %q size %v", *p.Spec.StorageClassName, p.Spec.Resources.Requests.Storage())
		}
	}
	if !found {
		t.Fatal("no claim for volume")
	}
}

func TestCreateVolumeRepeatIsOneClaimAndChangedSpecConflicts(t *testing.T) {
	b := newBlock(kube.DefaultClasses())
	id := model.NewVolumeID()
	s := spec(t, model.VolumeTypeStandard, model.GiB)
	before := claims(t)
	for range 2 {
		if err := b.CreateVolume(context.Background(), id, s); err != nil {
			t.Fatal(err)
		}
	}
	if got := claims(t) - before; got != 1 {
		t.Fatalf("claims created %d", got)
	}
	s.Size = 2 * model.GiB
	err := b.CreateVolume(context.Background(), id, s)
	if !errors.Is(err, backend.ErrConflict) || !backend.IsPermanent(err) {
		t.Fatalf("err %v", err)
	}
}

func TestUnknownTypeIsInvalid(t *testing.T) {
	b := newBlock(kube.DefaultClasses())
	before := claims(t)
	err := b.CreateVolume(context.Background(), model.NewVolumeID(), spec(t, model.VolumeTypeUnspecified, model.GiB))
	if !errors.Is(err, backend.ErrInvalid) || !backend.IsPermanent(err) {
		t.Fatalf("err %v", err)
	}
	if claims(t) != before {
		t.Fatal("claim created")
	}
}

func TestExpandGrowsAndRejectsShrink(t *testing.T) {
	b := newBlock(kube.DefaultClasses())
	ctx := context.Background()
	id := model.NewVolumeID()
	if err := b.CreateVolume(ctx, id, spec(t, model.VolumeTypeStandard, 2*model.GiB)); err != nil {
		t.Fatal(err)
	}
	bind(t, id)
	if err := b.Expand(ctx, id, 4*model.GiB); err != nil {
		t.Fatal(err)
	}
	if err := b.Expand(ctx, id, 4*model.GiB); err != nil {
		t.Fatalf("same size: %v", err)
	}
	err := b.Expand(ctx, id, model.GiB)
	if !errors.Is(err, backend.ErrInvalid) {
		t.Fatalf("shrink err %v", err)
	}
	err = b.Expand(ctx, model.NewVolumeID(), model.GiB)
	if !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("missing err %v", err)
	}
	for _, p := range func() []string {
		l, _ := kc.CoreV1().PersistentVolumeClaims(ns).List(ctx, metav1.ListOptions{})
		var out []string
		for _, c := range l.Items {
			if c.Annotations[kube.VolumeAnnotation] == id.String() {
				out = append(out, c.Spec.Resources.Requests.Storage().String())
			}
		}
		return out
	}() {
		if p != "4Gi" {
			t.Fatalf("size %s", p)
		}
	}
}

func TestDeleteVolumeIsIdempotent(t *testing.T) {
	b := newBlock(kube.DefaultClasses())
	ctx := context.Background()
	id := model.NewVolumeID()
	if err := b.CreateVolume(ctx, id, spec(t, model.VolumeTypeStandard, model.GiB)); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := b.DeleteVolume(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.DeleteVolume(ctx, model.NewVolumeID()); err != nil {
		t.Fatalf("absent: %v", err)
	}
}

func TestCreateVolumeWithSameClientTokenMakesOneClaim(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	objs := fake.NewObjects(fake.ObjectOptions{})
	svc, err := storage.Open(ctx, storage.Options{
		Store: storage.Memory(), Block: newBlock(kube.DefaultClasses()),
		Object: objs, Signer: objs, Zones: []model.Zone{zone(t)},
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = svc.Run(ctx); close(done) }()
	defer func() { cancel(); <-done; _ = svc.Close() }()

	tok, err := model.ParseClientToken("kube-token-1")
	if err != nil {
		t.Fatal(err)
	}
	req := model.CreateVolumeRequest{Spec: spec(t, model.VolumeTypeStandard, model.GiB)}
	before := claims(t)
	var ops [2]model.OperationID
	for i := range ops {
		op, err := svc.Submit(ctx, tok, req)
		if err != nil {
			t.Fatal(err)
		}
		ops[i] = op.ID
	}
	if ops[0] != ops[1] {
		t.Fatalf("operations %s %s", ops[0], ops[1])
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		op, err := svc.Operation(ctx, ops[0])
		if err != nil {
			t.Fatal(err)
		}
		if op.Terminal() {
			if op.Phase() != model.PhaseSucceeded {
				t.Fatalf("phase %s err %+v", op.Phase(), op.Err())
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("operation did not finish")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := claims(t) - before; got != 1 {
		t.Fatalf("claims created %d", got)
	}
}

func TestCreateVolumeOverClaimWithoutClassIsConflict(t *testing.T) {
	b := newBlock(kube.DefaultClasses())
	id := model.NewVolumeID()
	size := resource.MustParse("1Gi")
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: kube.PVCName(id), Namespace: ns,
			Annotations: map[string]string{kube.ZoneAnnotation: zone(t).String(), kube.SourceAnnotation: ""},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: size}},
		},
	}
	if _, err := kc.CoreV1().PersistentVolumeClaims(ns).Create(context.Background(), pvc, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	err := b.CreateVolume(context.Background(), id, spec(t, model.VolumeTypeStandard, model.GiB))
	if !errors.Is(err, backend.ErrConflict) || !backend.IsPermanent(err) {
		t.Fatalf("err %v", err)
	}
}

func TestCreateVolumeRepeatAfterExpandIsSameVolume(t *testing.T) {
	b := newBlock(kube.DefaultClasses())
	ctx := context.Background()
	id := model.NewVolumeID()
	if err := b.CreateVolume(ctx, id, spec(t, model.VolumeTypeStandard, 2*model.GiB)); err != nil {
		t.Fatal(err)
	}
	bind(t, id)
	if err := b.Expand(ctx, id, 4*model.GiB); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateVolume(ctx, id, spec(t, model.VolumeTypeStandard, 2*model.GiB)); err != nil {
		t.Fatalf("repeat with the original size: %v", err)
	}
	err := b.CreateVolume(ctx, id, spec(t, model.VolumeTypeStandard, 8*model.GiB))
	if !errors.Is(err, backend.ErrConflict) {
		t.Fatalf("larger than the claim: %v", err)
	}
}

func TestClaimAndSnapshotObjectNamesMatchTheObjectsCreated(t *testing.T) {
	b := newBlock(kube.DefaultClasses())
	ctx := context.Background()
	vol := newVolume(t, b)
	pvc := claimFor(t, vol)
	if pvc == nil || b.ClaimName(vol) != pvc.Name {
		t.Fatalf("ClaimName %q, claim %v", b.ClaimName(vol), pvc)
	}
	if _, err := kc.CoreV1().PersistentVolumeClaims(ns).Get(ctx, b.ClaimName(vol), metav1.GetOptions{}); err != nil {
		t.Fatalf("no claim named %q: %v", b.ClaimName(vol), err)
	}
	snap := model.NewSnapshotID()
	_ = b.CreateSnapshot(ctx, snap, vol)
	vs := snapshotFor(t, snap)
	if vs == nil || b.SnapshotObjectName(snap) != vs.GetName() {
		t.Fatalf("SnapshotObjectName %q, snapshot %v", b.SnapshotObjectName(snap), vs)
	}
}
