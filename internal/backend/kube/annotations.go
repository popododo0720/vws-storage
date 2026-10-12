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
)
