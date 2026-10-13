package kube

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
	sourceAnnotation   = annotationPrefix + "source"
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
)
