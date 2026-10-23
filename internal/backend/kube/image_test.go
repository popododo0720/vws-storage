package kube_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/popododo0720/vws-storage/internal/backend"
	"github.com/popododo0720/vws-storage/internal/backend/kube"
	"github.com/popododo0720/vws-storage/internal/model"
)

var dsGVR = schema.GroupVersionResource{Group: "cdi.kubevirt.io", Version: "v1beta1", Resource: "datasources"}

const imageNS = "vws-images"

func ensureNamespace(t *testing.T, name string) {
	t.Helper()
	_, err := kc.CoreV1().Namespaces().Create(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatal(err)
	}
}

// imageSource makes the DataSource vws-compute makes for an image: named by
// the image id, in the image namespace, pointing at the image's claim.
func imageSource(t *testing.T, imageID string) model.Source {
	t.Helper()
	ownedDataSource(t, imageID, map[string]any{kube.LabelProject: "a123456789012", kube.LabelRegion: "vws-1"})
	return imageRef(t, imageID)
}

// ownedDataSource makes the DataSource with the given owner labels (nil for
// none).
func ownedDataSource(t *testing.T, imageID string, labels map[string]any) {
	t.Helper()
	ensureNamespace(t, imageNS)
	meta := map[string]any{"name": imageID, "namespace": imageNS}
	if labels != nil {
		meta["labels"] = labels
	}
	ds := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "cdi.kubevirt.io/v1beta1", "kind": "DataSource",
		"metadata": meta,
		"spec":     map[string]any{"source": map[string]any{"pvc": map[string]any{"name": imageID, "namespace": imageNS}}},
	}}
	if _, err := dyn.Resource(dsGVR).Namespace(imageNS).Create(context.Background(), ds, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatal(err)
	}
}

func imageRef(t *testing.T, imageID string) model.Source {
	t.Helper()
	img, err := model.ParseImageName("projects/a123456789012/regions/vws-1/images/" + imageID)
	if err != nil {
		t.Fatal(err)
	}
	src, err := model.FromImage(img)
	if err != nil {
		t.Fatal(err)
	}
	return src
}

func imageSpec(t *testing.T, src model.Source) model.VolumeSpec {
	t.Helper()
	s := spec(t, model.VolumeTypeGP2, 8*model.GiB)
	s.Source = src
	return s
}

func imageBlock(mode corev1.PersistentVolumeMode, retries int) *kube.Block {
	return kube.NewBlock(kc, dyn, ns, kube.DefaultClasses(), "").WithVolumeMode(mode).
		WithDataVolumeRetry(retries, time.Hour).WithImageNamespace(imageNS)
}

// An image volume is a DataVolume cloning from the image's DataSource, in
// either volume mode, and is busy until CDI reports Succeeded.
func TestImageVolumeClonesTheImageDataSource(t *testing.T) {
	for _, mode := range []corev1.PersistentVolumeMode{corev1.PersistentVolumeBlock, corev1.PersistentVolumeFilesystem} {
		t.Run(string(mode), func(t *testing.T) {
			b := imageBlock(mode, 3)
			id := model.NewVolumeID()
			s := imageSpec(t, imageSource(t, "ami-0123456789abcdef0"))
			if err := b.CreateVolume(context.Background(), id, s); !errors.Is(err, backend.ErrBusy) || backend.IsPermanent(err) {
				t.Fatalf("first create: %v", err)
			}
			dv := dataVolume(t, b, id)
			ref, _, _ := unstructured.NestedStringMap(dv.Object, "spec", "sourceRef")
			if ref["kind"] != "DataSource" || ref["name"] != "ami-0123456789abcdef0" || ref["namespace"] != imageNS {
				t.Fatalf("sourceRef %v", ref)
			}
			if got, _, _ := unstructured.NestedString(dv.Object, "spec", "storage", "volumeMode"); got != string(mode) {
				t.Fatalf("volumeMode %q", got)
			}
			if got, _, _ := unstructured.NestedString(dv.Object, "spec", "storage", "resources", "requests", "storage"); got != "8Gi" {
				t.Fatalf("request %q", got)
			}
			setPhase(t, b, id, "CloneInProgress")
			if err := b.CreateVolume(context.Background(), id, s); !errors.Is(err, backend.ErrBusy) {
				t.Fatalf("while cloning: %v", err)
			}
			setPhase(t, b, id, "Succeeded")
			if err := b.CreateVolume(context.Background(), id, s); err != nil {
				t.Fatalf("after Succeeded: %v", err)
			}
			if err := b.DeleteVolume(context.Background(), id); err != nil {
				t.Fatal(err)
			}
			if _, err := dyn.Resource(dvGVR).Namespace(ns).Get(context.Background(), b.ClaimName(id), metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				t.Fatalf("DataVolume after delete: %v", err)
			}
		})
	}
}

// A DataSource of another project or region, or with no owner, is not the
// image: the create fails for good with the same reason as a missing one,
// and makes no DataVolume (#89).
func TestImageOfAnotherOwnerIsNotFound(t *testing.T) {
	b := imageBlock(corev1.PersistentVolumeBlock, 3)
	for id, labels := range map[string]map[string]any{
		"img-other-project": {kube.LabelProject: "b999999999999", kube.LabelRegion: "vws-1"},
		"img-other-region":  {kube.LabelProject: "a123456789012", kube.LabelRegion: "vws-2"},
		"img-pub-region":    {kube.LabelProject: "b999999999999", kube.LabelRegion: "vws-2", kube.LabelPublic: "true"},
		"img-unowned":       nil,
		"img-absent":        {"skip": "true"},
	} {
		t.Run(id, func(t *testing.T) {
			if labels["skip"] == nil {
				ownedDataSource(t, id, labels)
			}
			vol := model.NewVolumeID()
			err := b.CreateVolume(context.Background(), vol, imageSpec(t, imageRef(t, id)))
			want := "image projects/a123456789012/regions/vws-1/images/" + id + " not found"
			if !errors.Is(err, backend.ErrNotFound) ||
				errors.Is(err, backend.ErrInvalid) ||
				!backend.IsPermanent(err) ||
				err.Error() != want {
				t.Fatalf("got %v", err)
			}
			if strings.Contains(err.Error(), "b999999999999") || strings.Contains(err.Error(), "vws-2") {
				t.Fatalf("message names another owner: %v", err)
			}
			if _, err := dyn.Resource(dvGVR).Namespace(ns).Get(context.Background(), b.ClaimName(vol), metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				t.Fatalf("DataVolume made: %v", err)
			}
		})
	}
}

// A public DataSource of another project in the same region is cloned under
// the caller's own image name, as an AWS public AMI is.
func TestPublicImageOfAnotherProjectIsCloned(t *testing.T) {
	b := imageBlock(corev1.PersistentVolumeBlock, 3)
	ownedDataSource(t, "img-public001", map[string]any{kube.LabelProject: "b999999999999", kube.LabelRegion: "vws-1", kube.LabelPublic: "true"})
	vol := model.NewVolumeID()
	if err := b.CreateVolume(context.Background(), vol, imageSpec(t, imageRef(t, "img-public001"))); err != nil && (backend.IsPermanent(err) || !errors.Is(err, backend.ErrBusy)) {
		t.Fatalf("public clone refused: %v", err)
	}
	dv, err := dyn.Resource(dvGVR).Namespace(ns).Get(context.Background(), b.ClaimName(vol), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("no DataVolume: %v", err)
	}
	if ref, _, _ := unstructured.NestedStringMap(dv.Object, "spec", "sourceRef"); ref["name"] != "img-public001" {
		t.Fatalf("sourceRef %v", ref)
	}
}

// A Failed clone is made again, as a blank DataVolume is, and then succeeds.
func TestFailedImageCloneIsRetried(t *testing.T) {
	b := imageBlock(corev1.PersistentVolumeBlock, 3)
	id := model.NewVolumeID()
	s := imageSpec(t, imageSource(t, "img-retry0001"))
	_ = createBusy(t, b, id, s)
	first := dataVolume(t, b, id).GetUID()
	setPhase(t, b, id, "Failed")
	if err := createBusy(t, b, id, s); !errors.Is(err, backend.ErrBusy) || backend.IsPermanent(err) {
		t.Fatalf("after Failed: %v", err)
	}
	dv := dataVolume(t, b, id)
	if dv.GetUID() == first || dv.GetAnnotations()[attemptAnnotation] != "2" {
		t.Fatalf("not recreated: uid same %v attempt %q", dv.GetUID() == first, dv.GetAnnotations()[attemptAnnotation])
	}
	if ref, _, _ := unstructured.NestedStringMap(dv.Object, "spec", "sourceRef"); ref["name"] != "img-retry0001" {
		t.Fatalf("retry lost the source: %v", ref)
	}
	setPhase(t, b, id, "Succeeded")
	if err := b.CreateVolume(context.Background(), id, s); err != nil {
		t.Fatalf("after Succeeded: %v", err)
	}
}

// Past the retry limit a clone fails for good with the reason.
func TestImageClonePastTheRetryLimitIsPermanent(t *testing.T) {
	b := imageBlock(corev1.PersistentVolumeBlock, 1)
	id := model.NewVolumeID()
	s := imageSpec(t, imageSource(t, "img-limit0001"))
	_ = createBusy(t, b, id, s)
	setPhase(t, b, id, "Failed")
	_ = createBusy(t, b, id, s)
	setPhase(t, b, id, "Failed")
	err := b.CreateVolume(context.Background(), id, s)
	if !backend.IsPermanent(err) || !errors.Is(err, backend.ErrGaveUp) || !strings.Contains(err.Error(), "failed 2 times") {
		t.Fatalf("past the limit: %v", err)
	}
}

// An image with no DataSource fails at once, permanently, with the reason,
// and makes no DataVolume.
func TestImageWithoutDataSourceIsPermanent(t *testing.T) {
	ensureNamespace(t, imageNS)
	b := imageBlock(corev1.PersistentVolumeBlock, 3)
	id := model.NewVolumeID()
	err := b.CreateVolume(context.Background(), id, imageSpec(t, imageRef(t, "img-missing0001")))
	want := "image projects/a123456789012/regions/vws-1/images/img-missing0001 not found"
	if !backend.IsPermanent(err) ||
		!errors.Is(err, backend.ErrNotFound) ||
		errors.Is(err, backend.ErrInvalid) ||
		err.Error() != want {
		t.Fatalf("missing source: %v", err)
	}
	if _, err := dyn.Resource(dvGVR).Namespace(ns).Get(context.Background(), b.ClaimName(id), metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("a DataVolume was made: %v", err)
	}
}

// A caller that may not clone from the image namespace (CDI's check:
// create datavolumes/source, or create pods, there) fails permanently before
// any DataVolume, with the permission it lacks.
func TestImageCloneWithoutPermissionIsPermanent(t *testing.T) {
	imageSource(t, "img-noperm0001")
	ctx := context.Background()
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: "vws-storage-test", Namespace: ns},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{"cdi.kubevirt.io"}, Resources: []string{"datavolumes"}, Verbs: []string{"get", "create", "delete"}},
			{APIGroups: []string{""}, Resources: []string{"persistentvolumeclaims"}, Verbs: []string{"get", "create", "delete"}},
		},
	}
	imgRole := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: "vws-storage-test", Namespace: imageNS},
		Rules:      []rbacv1.PolicyRule{{APIGroups: []string{"cdi.kubevirt.io"}, Resources: []string{"datasources"}, Verbs: []string{"get"}}},
	}
	user, err := testEnv.AddUser(envtest.User{Name: "vws-storage-test"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range []*rbacv1.Role{role, imgRole} {
		if _, err := kc.RbacV1().Roles(r.Namespace).Create(ctx, r, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			t.Fatal(err)
		}
		rb := &rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "vws-storage-test", Namespace: r.Namespace},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: r.Name},
			Subjects:   []rbacv1.Subject{{Kind: "User", Name: "vws-storage-test"}},
		}
		if _, err := kc.RbacV1().RoleBindings(r.Namespace).Create(ctx, rb, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			t.Fatal(err)
		}
	}
	cfg := user.Config()
	b := kube.NewBlock(kubernetes.NewForConfigOrDie(cfg), dynamic.NewForConfigOrDie(cfg), ns, kube.DefaultClasses(), "").
		WithDataVolumeRetry(3, time.Hour).WithImageNamespace(imageNS)
	id := model.NewVolumeID()
	err = b.CreateVolume(ctx, id, imageSpec(t, imageRef(t, "img-noperm0001")))
	if !backend.IsPermanent(err) || !errors.Is(err, backend.ErrInvalid) || !strings.Contains(err.Error(), "datavolumes.cdi.kubevirt.io/source") {
		t.Fatalf("no clone permission: %v", err)
	}
	if _, err := dyn.Resource(dvGVR).Namespace(ns).Get(ctx, b.ClaimName(id), metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("a DataVolume was made: %v", err)
	}
	// With the permission CDI checks, the same caller gets a DataVolume.
	imgRole.Rules = append(imgRole.Rules, rbacv1.PolicyRule{APIGroups: []string{"cdi.kubevirt.io"}, Resources: []string{"datavolumes/source"}, Verbs: []string{"create"}})
	if _, err := kc.RbacV1().Roles(imageNS).Update(ctx, imgRole, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		err = b.CreateVolume(ctx, id, imageSpec(t, imageRef(t, "img-noperm0001")))
		if errors.Is(err, backend.ErrBusy) || time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !errors.Is(err, backend.ErrBusy) || backend.IsPermanent(err) {
		t.Fatalf("with datavolumes/source: %v", err)
	}
}
