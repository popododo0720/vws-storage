package kube

import "time"

var PVCName = pvcName

const (
	ZoneAnnotation      = zoneAnnotation
	SourceAnnotation    = sourceAnnotation
	ErrorSeenAnnotation = errorSeenAnnotation
)

func SetSnapshotErrorGrace(d time.Duration) (restore func()) {
	old := snapshotErrorGrace
	snapshotErrorGrace = d
	return func() { snapshotErrorGrace = old }
}
