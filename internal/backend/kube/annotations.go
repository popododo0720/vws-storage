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
)

const (
	legacyZone           = "vws.storage/zone"
	legacySourceSnapshot = "vws.storage/source-snapshot"
	legacySourceVolume   = "vws.storage/source-volume"
)

// annotation reads key and falls back to its vws.storage/ name, so a repeated
// create on an object from before the prefix move is still the same resource.
func annotation(m map[string]string, key, legacy string) string {
	if v, ok := m[key]; ok {
		return v
	}
	return m[legacy]
}
