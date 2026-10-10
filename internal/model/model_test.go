package model

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestVolumeCreateThenReady(t *testing.T) {
	now := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	zone, err := ParseZone("az-1")
	if err != nil {
		t.Fatal(err)
	}
	req := CreateVolumeRequest{Spec: VolumeSpec{Size: GiB, Zone: zone, Type: VolumeTypeSSD, Source: NoSource()}}
	if err := req.Validate(); err != nil {
		t.Fatal(err)
	}
	tok, err := ParseClientToken("vol-create-1")
	if err != nil {
		t.Fatal(err)
	}
	claim, err := Claim(tok, req)
	if err != nil {
		t.Fatal(err)
	}
	st := Start{Op: NewOperationID(), Claim: claim, Now: now}
	admitted, err := NewVolume(st, NewVolumeID(), req)
	if err != nil {
		t.Fatal(err)
	}
	vol := putNext(t, admitted.Change, "volume").(Volume)
	creating, ok := vol.AsCreating()
	if !ok || creating.st.Op != st.Op {
		t.Fatalf("state %v", vol.State)
	}
	run, ok := AsRunning[VolumeCreation](admitted.Op)
	if !ok {
		t.Fatal("not running")
	}
	settled, err := creating.Created(run, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	ready := putNext(t, settled, "volume").(Volume)
	if _, ok := ready.AsAvailable(); !ok {
		t.Fatalf("state %v", ready.State)
	}
	op := putNext(t, settled, "operation").(Operation)
	if op.Phase() != PhaseSucceeded {
		t.Fatalf("phase %s", op.Phase())
	}
	again, err := HashRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	if again != claim.Fingerprint {
		t.Fatal("fingerprint changed")
	}
	other := req
	other.Spec.Size = 2 * GiB
	otherFP, err := HashRequest(other)
	if err != nil {
		t.Fatal(err)
	}
	if otherFP == claim.Fingerprint {
		t.Fatal("different spec hashed the same")
	}
}

func TestBlankSourceIsNotASnapshot(t *testing.T) {
	if _, ok := NoSource().Snapshot(); ok {
		t.Fatal("empty source looked like a snapshot")
	}
	if _, err := FromSnapshot(SnapshotID{}); err == nil {
		t.Fatal("zero snapshot id was accepted")
	}
}

func TestDeleteFailureRestoresVolume(t *testing.T) {
	now := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	vol := availableVolume(t, now)
	avail, ok := vol.AsAvailable()
	if !ok {
		t.Fatal("not available")
	}
	admitted, err := avail.Delete(Start{Op: NewOperationID(), Now: now})
	if err != nil {
		t.Fatal(err)
	}
	deleting := putNext(t, admitted.Change, "volume").(Volume)
	del, ok := deleting.AsDeleting()
	if !ok {
		t.Fatal("not deleting")
	}
	run, ok := AsRunning[VolumeDeletion](admitted.Op)
	if !ok {
		t.Fatal("not running")
	}
	ch, err := del.DeleteFailed(run, Status{Code: CodeUnavailable, Message: "down"}, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	back := putNext(t, ch, "volume").(Volume)
	if _, ok := back.AsAvailable(); !ok {
		t.Fatalf("state %v", back.State)
	}
	op := putNext(t, ch, "operation").(Operation)
	if op.Phase() != PhaseFailed || op.Err().Code != CodeUnavailable {
		t.Fatalf("op %+v", op.Err())
	}
}

func TestCommitUploadReplacesOlderHead(t *testing.T) {
	now := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	bucket := mustBucket(t)
	key := mustKey(t, "a.bin")
	path := ObjectPath{Bucket: bucket, Key: key}
	headGen := NewGeneration()
	head := &Object{
		Path: path, Generation: headGen, Size: 4, ETag: "old",
		UploadedAt: now, Meta: fresh(now),
	}
	upGen := NewGeneration()
	work := ObjectUpload{
		Object: ObjectRef{Path: path, Generation: upGen}, Size: 4,
		Deadline: now.Add(time.Hour),
	}
	op := Operation{ID: NewOperationID(), Work: work, State: OpRunning{}, Meta: fresh(now)}
	run, ok := AsRunning[ObjectUpload](op)
	if !ok {
		t.Fatal("not running")
	}
	obs := ObservedObject{Size: 4, ETag: "new", UploadedAt: now.Add(time.Second)}
	ch, err := CommitUpload(run, head, obs, NewOperationID(), now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	next := putNext(t, ch, "object").(Object)
	if next.Generation != upGen || next.ETag != "new" {
		t.Fatalf("head %+v", next)
	}
	var sawPurge bool
	for _, w := range ch.Writes() {
		put, ok := w.(Put)
		if !ok || put.Prev != nil {
			continue
		}
		nop, ok := put.Next.(Operation)
		if !ok {
			continue
		}
		purge, ok := nop.Work.(ObjectPurge)
		if ok && purge.Object.Generation == headGen {
			sawPurge = true
		}
	}
	if !sawPurge {
		t.Fatal("old generation was not purged")
	}
}

func TestCommitUploadReplacesNamedGeneration(t *testing.T) {
	now := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	path := ObjectPath{Bucket: mustBucket(t), Key: mustKey(t, "a.bin")}
	headGen := NewGeneration()
	head := &Object{
		Path: path, Generation: headGen, Size: 4, ETag: "old",
		UploadedAt: now.Add(time.Hour), Meta: fresh(now),
	}
	upGen := NewGeneration()
	work := ObjectUpload{
		Object: ObjectRef{Path: path, Generation: upGen}, Replaces: headGen, Size: 4,
		Deadline: now.Add(2 * time.Hour),
	}
	op := Operation{ID: NewOperationID(), Work: work, State: OpRunning{}, Meta: fresh(now)}
	run, ok := AsRunning[ObjectUpload](op)
	if !ok {
		t.Fatal("not running")
	}
	obs := ObservedObject{Size: 4, ETag: "new", UploadedAt: now}
	ch, err := CommitUpload(run, head, obs, NewOperationID(), now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	next := putNext(t, ch, "object").(Object)
	if next.Generation != upGen || next.ETag != "new" {
		t.Fatalf("head %+v", next)
	}
	var sawPurge bool
	for _, w := range ch.Writes() {
		put, ok := w.(Put)
		if !ok || put.Prev != nil {
			continue
		}
		nop, ok := put.Next.(Operation)
		if !ok {
			continue
		}
		purge, ok := nop.Work.(ObjectPurge)
		if ok && purge.Object.Generation == headGen {
			sawPurge = true
		}
	}
	if !sawPurge {
		t.Fatal("old generation was not purged")
	}
}

func TestReplaceRejectsSameGeneration(t *testing.T) {
	now := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	bucket := Bucket{Name: mustBucket(t), State: Available{}, Meta: fresh(now)}
	avail, ok := bucket.AsAvailable()
	if !ok {
		t.Fatal("bucket")
	}
	head := Object{
		Path: ObjectPath{Bucket: bucket.Name, Key: mustKey(t, "a.bin")}, Generation: NewGeneration(), Meta: fresh(now),
	}
	_, err := avail.Replace(Start{Op: NewOperationID(), Now: now}, head, head.Generation, UpdateObjectRequest{
		Path: head.Path, HasContents: true, Size: 1,
	}, now.Add(time.Hour))
	if err == nil {
		t.Fatal("same generation")
	}
}

func TestDeleteKeepsHeadUntilPurge(t *testing.T) {
	now := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	obj := Object{
		Path: ObjectPath{Bucket: mustBucket(t), Key: mustKey(t, "a.bin")}, Generation: NewGeneration(),
		Size: 1, Meta: fresh(now),
	}
	admitted, err := obj.Delete(Start{Op: NewOperationID(), Now: now})
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range admitted.Change.Writes() {
		if _, ok := w.(Remove); ok {
			t.Fatal("delete removed the head")
		}
	}
	run, ok := AsRunning[ObjectPurge](admitted.Op)
	if !ok {
		t.Fatal("not running")
	}
	ch, err := Purged(run, &obj, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var removed bool
	for _, w := range ch.Writes() {
		rm, ok := w.(Remove)
		if ok {
			removed = rm.Prev.(Object).Generation == obj.Generation
		}
	}
	if !removed {
		t.Fatal("purge left the head")
	}
	other := obj
	other.Generation = NewGeneration()
	ch, err = Purged(run, &other, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range ch.Writes() {
		if _, ok := w.(Remove); ok {
			t.Fatal("purge removed a newer head")
		}
	}
}

func TestRoundTripEveryKind(t *testing.T) {
	now := time.Date(2026, 10, 9, 1, 2, 3, 4000, time.UTC)
	records := sampleRecords(t, now)
	if len(records) < len(Kinds())+3 {
		t.Fatalf("samples %d", len(records))
	}
	for _, rec := range records {
		raw, err := MarshalRecord(rec)
		if err != nil {
			t.Fatal(err)
		}
		got, err := UnmarshalRecord(raw)
		if err != nil {
			t.Fatal(err)
		}
		if !SameRecord(rec, got) {
			t.Fatalf("round trip %T\n%s", rec, raw)
		}
	}
}

func TestTokenReuse(t *testing.T) {
	tok, err := ParseClientToken("same-token")
	if err != nil {
		t.Fatal(err)
	}
	zone, _ := ParseZone("az-1")
	req := CreateVolumeRequest{Spec: VolumeSpec{Size: GiB, Zone: zone, Type: VolumeTypeStandard, Source: NoSource()}}
	claim, err := Claim(tok, req)
	if err != nil {
		t.Fatal(err)
	}
	rec := TokenRecord{Claim: claim, Op: NewOperationID()}
	id, err := rec.Match(claim)
	if err != nil || id != rec.Op {
		t.Fatal(err)
	}
	other := req
	other.Spec.Type = VolumeTypeSSD
	otherClaim, err := Claim(tok, other)
	if err != nil {
		t.Fatal(err)
	}
	_, err = rec.Match(otherClaim)
	if !errors.Is(err, ErrTokenReused) {
		t.Fatal(err)
	}
}

func putNext(t *testing.T, ch Change, kind string) Record {
	t.Helper()
	for _, w := range ch.Writes() {
		put, ok := w.(Put)
		if !ok {
			continue
		}
		raw, err := MarshalRecord(put.Next)
		if err != nil {
			t.Fatal(err)
		}
		var env envelope
		if err := json.Unmarshal(raw, &env); err != nil {
			t.Fatal(err)
		}
		if env.Record == kind {
			return put.Next
		}
	}
	t.Fatalf("no %s put", kind)
	return nil
}

func availableVolume(t *testing.T, now time.Time) Volume {
	t.Helper()
	zone, err := ParseZone("az-1")
	if err != nil {
		t.Fatal(err)
	}
	return Volume{
		ID:    NewVolumeID(),
		Spec:  VolumeSpec{Size: GiB, Zone: zone, Type: VolumeTypeSSD, Source: NoSource()},
		State: Available{},
		Meta:  fresh(now),
	}
}

func mustBucket(t *testing.T) BucketName {
	t.Helper()
	b, err := ParseBucketName("logs")
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func mustKey(t *testing.T, s string) ObjectKey {
	t.Helper()
	k, err := ParseObjectKey(s)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func sampleRecords(t *testing.T, now time.Time) []Record {
	t.Helper()
	vol := availableVolume(t, now)
	creating := vol
	creating.State = Creating{Op: NewOperationID()}
	snap := Snapshot{ID: NewSnapshotID(), Source: vol.ID, Size: vol.Spec.Size, State: Available{}, Meta: fresh(now)}
	bucket := Bucket{Name: mustBucket(t), State: Available{}, Meta: fresh(now)}
	obj := Object{
		Path: ObjectPath{Bucket: bucket.Name, Key: mustKey(t, "k")}, Generation: NewGeneration(),
		Size: 3, ETag: "etag", UploadedAt: now, Meta: fresh(now),
	}
	tok, err := ParseClientToken("tok-1")
	if err != nil {
		t.Fatal(err)
	}
	claim, err := Claim(tok, CreateBucketRequest{Bucket: bucket.Name})
	if err != nil {
		t.Fatal(err)
	}
	out := []Record{creating, vol, snap, bucket, obj, TokenRecord{Claim: claim, Op: NewOperationID(), Meta: fresh(now)}}
	for _, k := range Kinds() {
		out = append(out, Operation{
			ID: NewOperationID(), Work: sampleWork(t, k, vol, snap, bucket, obj),
			State: OpRunning{Attempts: 1, NotBefore: now, LastError: Status{Code: CodeUnavailable, Message: "later"}},
			Claim: claim, Meta: fresh(now),
		})
	}
	out = append(out, Operation{
		ID: NewOperationID(), Work: VolumeDeletion{Volume: vol.ID},
		State: OpFailed{Cause: Status{Code: CodeInternal, Message: "x"}}, Meta: fresh(now),
	})
	out = append(out, Operation{
		ID: NewOperationID(), Work: VolumeCreation{Volume: vol.ID, Spec: vol.Spec},
		State: OpCompensating{Cause: Status{Code: CodeUnavailable, Message: "down"}, Attempts: 2, NotBefore: now},
		Meta:  fresh(now),
	})
	out = append(out, Operation{
		ID: NewOperationID(), Work: BucketCreation{Bucket: bucket.Name},
		State: OpSucceeded{}, Meta: fresh(now),
	})
	return out
}

func TestParseLabels(t *testing.T) {
	empty, err := ParseLabels(nil)
	if err != nil || empty.Map() != nil {
		t.Fatalf("empty %+v %v", empty, err)
	}
	a, err := ParseLabels(map[string]string{"team": "storage", "env": "prod", "eq": "a=b"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := ParseLabels(map[string]string{"eq": "a=b", "env": "prod", "team": "storage"})
	if err != nil || a != b {
		t.Fatalf("order %v %v %v", a, b, err)
	}
	got := a.Map()
	if got["env"] != "prod" || got["team"] != "storage" || got["eq"] != "a=b" || len(got) != 3 {
		t.Fatalf("map %v", got)
	}
	if _, err := ParseLabels(map[string]string{"Bad": "x"}); err == nil {
		t.Fatal("uppercase key")
	}
	desc, err := ParseDescription("nightly copy")
	if err != nil || desc != "nightly copy" {
		t.Fatalf("description %q %v", desc, err)
	}
	if _, err := ParseDescription("bad\n"); err == nil {
		t.Fatal("control character")
	}
}

func TestLabelsSurviveCodec(t *testing.T) {
	now := time.Date(2026, 10, 9, 1, 2, 3, 4000, time.UTC)
	labels, err := ParseLabels(map[string]string{"env": "prod"})
	if err != nil {
		t.Fatal(err)
	}
	vol := availableVolume(t, now)
	vol.Labels = labels
	snap := Snapshot{
		ID: NewSnapshotID(), Source: vol.ID, Labels: labels, Description: "nightly",
		Size: vol.Spec.Size, State: Available{}, Meta: fresh(now),
	}
	bucket := Bucket{Name: mustBucket(t), Labels: labels, State: Available{}, Meta: fresh(now)}
	obj := Object{
		Path: ObjectPath{Bucket: bucket.Name, Key: mustKey(t, "k")}, Generation: NewGeneration(),
		Size: 3, Labels: labels, UploadedAt: now, Meta: fresh(now),
	}
	op := Operation{
		ID: NewOperationID(),
		Work: ObjectUpload{
			Object: ObjectRef{Path: obj.Path, Generation: obj.Generation}, Size: obj.Size,
			Labels: labels, Deadline: now,
		},
		State: OpSucceeded{}, Meta: fresh(now),
	}
	for _, rec := range []Record{vol, snap, bucket, obj, op} {
		raw, err := MarshalRecord(rec)
		if err != nil {
			t.Fatal(err)
		}
		got, err := UnmarshalRecord(raw)
		if err != nil {
			t.Fatal(err)
		}
		if !SameRecord(rec, got) {
			t.Fatalf("round trip %T\n%s", rec, raw)
		}
	}
}

func TestResourceNamesRoundTrip(t *testing.T) {
	vol := NewVolumeID()
	if got, err := ParseVolumeID(vol.String()); err != nil || got != vol {
		t.Fatalf("volume %v %v", got, err)
	}
	snap := NewSnapshotID()
	if got, err := ParseSnapshotID(snap.String()); err != nil || got != snap {
		t.Fatalf("snapshot %v %v", got, err)
	}
	op := NewOperationID()
	if got, err := ParseOperationID(op.String()); err != nil || got != op {
		t.Fatalf("operation %v %v", got, err)
	}
	bucket, err := ParseBucketName("logs")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := ParseBucketName(bucket.String()); err != nil || got != bucket {
		t.Fatalf("bucket %v %v", got, err)
	}
	id, err := ParseVolumeID("data-1")
	if err != nil || id.ID() != "data-1" || id.String() != "volumes/data-1" {
		t.Fatalf("resource id %v %v", id, err)
	}
	project, err := ParseProject("a123")
	if err != nil {
		t.Fatal(err)
	}
	scoped := id.WithProject(project)
	if got, err := ParseVolumeID(scoped.String()); err != nil || got != scoped {
		t.Fatalf("scoped volume %v %v", got, err)
	}
	legacy, err := ParseVolumeID("data-1")
	if err != nil || legacy.Project() != (Project{}) || legacy.String() != "volumes/data-1" {
		t.Fatalf("legacy %v %v", legacy, err)
	}
	if _, err := ParseVolumeID("Vol"); err == nil {
		t.Fatal("uppercase volume id")
	}
}

func sampleWork(t *testing.T, k Kind, vol Volume, snap Snapshot, bucket Bucket, obj Object) Work {
	t.Helper()
	switch k {
	case KindCreateVolume:
		return VolumeCreation{Volume: vol.ID, Spec: vol.Spec}
	case KindDeleteVolume:
		return VolumeDeletion{Volume: vol.ID}
	case KindCreateSnapshot:
		return SnapshotCreation{Snapshot: snap.ID, Source: vol.ID}
	case KindDeleteSnapshot:
		return SnapshotDeletion{Snapshot: snap.ID}
	case KindCreateBucket:
		return BucketCreation{Bucket: bucket.Name}
	case KindDeleteBucket:
		return BucketDeletion{Bucket: bucket.Name}
	case KindPutObject:
		return ObjectUpload{Object: ObjectRef{Path: obj.Path, Generation: obj.Generation}, Size: obj.Size, Deadline: vol.Meta.CreateTime}
	case KindDeleteObject:
		return ObjectPurge{Object: ObjectRef{Path: obj.Path, Generation: obj.Generation}}
	default:
		t.Fatalf("kind %s", k)
		return nil
	}
}
