package kube

import (
	"context"
	"fmt"

	authorizationv1 "k8s.io/api/authorization/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/popododo0720/vws-storage/internal/backend"
	"github.com/popododo0720/vws-storage/internal/model"
)

var dataSourceGVR = schema.GroupVersionResource{Group: "cdi.kubevirt.io", Version: "v1beta1", Resource: "datasources"}

// DefaultImageNamespace is where vws-compute keeps image DataSources
// (vws-compute -image-namespace).
const DefaultImageNamespace = "vws-images"

// immediateBindAnnotation asks CDI to bind the clone target without waiting
// for a consumer. Nothing consumes the claim until the clone has Succeeded.
const immediateBindAnnotation = "cdi.kubevirt.io/storage.bind.immediate.requested"

// LabelProject and LabelRegion name the owner of an image DataSource. The
// image id alone is not unique across projects, so a DataSource is only the
// image projects/{project}/regions/{region}/images/{id} when both labels
// match (#89). vws-compute sets them; a DataSource without them is no
// project's image.
//
// LabelPublic=true marks a public image, as an AWS public AMI: every project
// in the image's region may clone it, under its own project's image name.
const (
	LabelProject = "vws.io/project"
	LabelRegion  = "vws.io/region"
	LabelPublic  = "vws.io/public"
)

// cloneAllowed reports whether a DataSource with labels is the image img: its
// project and region own it, or it is public and in img's region.
func cloneAllowed(labels map[string]string, img model.ImageName) bool {
	region := img.Region().String()
	if labels[LabelRegion] != region {
		return false
	}
	return labels[LabelProject] == img.Project().String() || labels[LabelPublic] == "true"
}

// imageVolume makes an image volume a DataVolume that clones the image.
//
// vws-compute stores an image as a CDI DataVolume and a DataSource, both
// named by the image id, in the image namespace, the DataSource pointing at
// the image's claim. The image name is projects/*/regions/*/images/{id}, so
// the volume clones from DataSource {id} there, with sourceRef as a
// VirtualMachine root disk does.
//
// Before the first DataVolume it does what CDI's DataVolume webhook would
// refuse later, so the reason is the backend's own sentence: the DataSource
// must exist, and the caller must be allowed to clone from its source
// namespace (create datavolumes/source, or create pods, there).
func (b *Block) imageVolume(ctx context.Context, id model.VolumeID, spec model.VolumeSpec, class string, img model.ImageName) error {
	name := img.ID()
	if errs := validation.IsDNS1123Label(name); len(errs) > 0 {
		return backend.Permanent(fmt.Errorf("%w: image id %q is not an object name: %s", backend.ErrInvalid, name, errs[0]))
	}
	if err := b.checkImageSource(ctx, img); err != nil {
		return err
	}
	src := dvSource{
		field: "sourceRef",
		value: map[string]any{"kind": "DataSource", "name": name, "namespace": b.imageNS},
		key:   img.String(),
		mode:  b.mode,
		annotations: map[string]string{
			immediateBindAnnotation: "true",
		},
	}
	return b.dataVolume(ctx, id, spec, class, src)
}

// checkImageSource fails permanently when the image has no DataSource, the
// DataSource belongs to another region, or to another project and is not
// public, or the backend may not
// clone from where it points. Absent and foreign read the same, so the
// reason does not tell one project about another's images.
func (b *Block) checkImageSource(ctx context.Context, img model.ImageName) error {
	name := img.ID()
	notFound := backend.MissingImage(img)
	ds, err := b.dyn.Resource(dataSourceGVR).Namespace(b.imageNS).Get(ctx, name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		return notFound
	case meta.IsNoMatchError(err):
		return fmt.Errorf("%w: the CDI DataSource API is not installed", backend.ErrBusy)
	case apierrors.IsForbidden(err):
		return backend.Permanent(fmt.Errorf("%w: may not get DataSource %s/%s: needs get on datasources.cdi.kubevirt.io there", backend.ErrInvalid, b.imageNS, name))
	case err != nil:
		return fmt.Errorf("kube: get data source: %w", err)
	}
	if !cloneAllowed(ds.GetLabels(), img) {
		return notFound
	}
	// CDI checks the namespace of the claim (or snapshot) the DataSource
	// names, defaulting to the DataSource's own.
	srcNS := b.imageNS
	for _, kind := range []string{"pvc", "snapshot"} {
		if v, ok, _ := unstructured.NestedString(ds.Object, "spec", "source", kind, "namespace"); ok && v != "" { //nolint:errcheck // absent is the default
			srcNS = v
		}
	}
	if srcNS == b.ns {
		return nil
	}
	for _, ra := range []authorizationv1.ResourceAttributes{
		{Namespace: srcNS, Verb: "create", Group: "cdi.kubevirt.io", Resource: "datavolumes", Subresource: "source"},
		{Namespace: srcNS, Verb: "create", Resource: "pods"},
	} {
		sar := &authorizationv1.SelfSubjectAccessReview{Spec: authorizationv1.SelfSubjectAccessReviewSpec{ResourceAttributes: &ra}}
		got, err := b.kc.AuthorizationV1().SelfSubjectAccessReviews().Create(ctx, sar, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("kube: access review: %w", err)
		}
		if got.Status.Allowed {
			return nil
		}
	}
	return backend.Permanent(fmt.Errorf("%w: may not clone image %s from namespace %s: needs create on datavolumes.cdi.kubevirt.io/source there", backend.ErrInvalid, name, srcNS))
}
