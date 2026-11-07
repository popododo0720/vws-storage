package kube

import "github.com/popododo0720/vws-storage/internal/model"

const annotationPrefix = "vws.popododo0720.io/"

// The prefix is shared with vws-network, which owns "id" and "project".
// "source" is the snapshot a PVC restores from, or the volume a VolumeSnapshot
// captures. The object kind says which.
const (
	// VolumeAnnotation holds the VWS volume name on the PVC.
	VolumeAnnotation = annotationPrefix + "volume"
	// SnapshotAnnotation holds the VWS snapshot name on the VolumeSnapshot.
	SnapshotAnnotation = annotationPrefix + "snapshot"
	zoneAnnotation     = annotationPrefix + "zone"
	// VolumeTypeAnnotation on a claim or DataVolume is the EC2 volume type it
	// was made for, which picks its VolumeSnapshotClass. Claims made before
	// vws-storage#123 have none.
	VolumeTypeAnnotation = annotationPrefix + "volume-type"
	sourceAnnotation     = annotationPrefix + "source"
	// errorSeenAnnotation holds when a VolumeSnapshot first reported status.error.
	errorSeenAnnotation = annotationPrefix + "error-first-seen"
	// sourceModeAnnotation on a VolumeSnapshot is the volumeMode of the claim
	// it was taken from. A restore uses the same mode. A snapshot without it
	// predates Block volumes, so its source was Filesystem.
	sourceModeAnnotation = annotationPrefix + "source-volume-mode"
	// sizeAnnotation on a claim is the volume size in bytes when the claim
	// requests more than that: CDI adds the filesystem overhead to the claim
	// of a Filesystem DataVolume. Expand writes it.
	sizeAnnotation = annotationPrefix + "size"
	// dvAttemptAnnotation on a DataVolume is which attempt it is (1 is the
	// first). dvFirstAnnotation is when the first attempt was created
	// (RFC 3339). A recreate after Failed carries both forward.
	dvAttemptAnnotation = annotationPrefix + "dv-attempt"
	dvFirstAnnotation   = annotationPrefix + "dv-first-created"
)

// The labels carry the AWS IDs, so a selector finds the PVC, DataVolume or
// VolumeSnapshot of an ID: kubectl get pvc -l vws.popododo0720.io/volume-id=vol-0a1b.
// A client may choose an ID, so the same one can exist in two projects; the
// project label tells them apart. Every resource id fits a label value.
const (
	VolumeIDLabel   = annotationPrefix + "volume-id"
	SnapshotIDLabel = annotationPrefix + "snapshot-id"
	ProjectIDLabel  = annotationPrefix + "project-id"
)

// idLabels are the labels of the object behind resource id of project p.
// idLabel is VolumeIDLabel or SnapshotIDLabel.
func idLabels(idLabel, id string, p model.Project) map[string]string {
	return map[string]string{idLabel: id, ProjectIDLabel: p.String()}
}
