package model

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestVolumeCreateThenReady(t *testing.T) {
	now := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	zone, err := ParseZone("az-1")
	if err != nil {
		t.Fatal(err)
	}
	req := CreateVolumeRequest{Spec: VolumeSpec{Size: GiB, Zone: zone, Type: VolumeTypeGP2, Source: NoSource()}}
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

func TestCreateVolumeFingerprintIsStable(t *testing.T) {
	zone, err := ParseZone("az-1")
	if err != nil {
		t.Fatal(err)
	}
	project, err := ParseProject("a123")
	if err != nil {
		t.Fatal(err)
	}
	id, err := ParseVolumeID("data-1")
	if err != nil {
		t.Fatal(err)
	}
	id = id.WithProject(project)
	blank := CreateVolumeRequest{ID: id, Spec: VolumeSpec{Size: GiB, Zone: zone, Type: VolumeTypeGP2, Source: NoSource()}}
	fp, err := HashRequest(blank)
	if err != nil {
		t.Fatal(err)
	}
	if got := fp.String(); got != "a018b471493fa31bacf0ea333722c6743a0d92142c373119af6cd082af13f2c1" {
		t.Fatalf("blank %s", got)
	}
	snapID, err := ParseSnapshotID("snap-1")
	if err != nil {
		t.Fatal(err)
	}
	src, err := FromSnapshot(snapID.WithProject(project))
	if err != nil {
		t.Fatal(err)
	}
	snapReq := blank
	snapReq.Spec.Source = src
	fp, err = HashRequest(snapReq)
	if err != nil {
		t.Fatal(err)
	}
	if got := fp.String(); got != "f308b1170e48ba7854e3290dd5f98fd3a27bf89482ca34c3b72c8041388a838e" {
		t.Fatalf("snapshot %s", got)
	}
	img, err := ParseImageName("projects/shared/regions/vws-1/images/ubuntu-24-04")
	if err != nil {
		t.Fatal(err)
	}
	imageSrc, err := FromImage(img)
	if err != nil {
		t.Fatal(err)
	}
	imageReq := blank
	imageReq.Spec.Source = imageSrc
	fp, err = HashRequest(imageReq)
	if err != nil {
		t.Fatal(err)
	}
	if fp.String() == "a018b471493fa31bacf0ea333722c6743a0d92142c373119af6cd082af13f2c1" || fp.String() == "f308b1170e48ba7854e3290dd5f98fd3a27bf89482ca34c3b72c8041388a838e" {
		t.Fatal("image hashed like a blank or snapshot volume")
	}
	otherImg, err := ParseImageName("projects/shared/regions/vws-1/images/ubuntu-22-04")
	if err != nil {
		t.Fatal(err)
	}
	otherSrc, err := FromImage(otherImg)
	if err != nil {
		t.Fatal(err)
	}
	otherReq := blank
	otherReq.Spec.Source = otherSrc
	otherFP, err := HashRequest(otherReq)
	if err != nil {
		t.Fatal(err)
	}
	if otherFP == fp {
		t.Fatal("two images hashed the same")
	}
}

func TestBlankSourceIsNotASnapshot(t *testing.T) {
	blank := NoSource()
	if _, ok := blank.Snapshot(); ok {
		t.Fatal("empty source looked like a snapshot")
	}
	if _, ok := blank.Image(); ok {
		t.Fatal("empty source looked like an image")
	}
	if _, err := FromSnapshot(SnapshotID{}); err == nil {
		t.Fatal("zero snapshot id was accepted")
	}
	if _, err := FromImage(ImageName{}); err == nil {
		t.Fatal("zero image name was accepted")
	}
	img, err := ParseImageName("projects/shared/regions/vws-1/images/ubuntu-24-04")
	if err != nil {
		t.Fatal(err)
	}
	image, err := FromImage(img)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := image.Snapshot(); ok {
		t.Fatal("image source looked like a snapshot")
	}
	if got, ok := image.Image(); !ok || got != img {
		t.Fatalf("image %v %v", got, ok)
	}
	project, err := ParseProject("a123")
	if err != nil {
		t.Fatal(err)
	}
	snapID, err := ParseSnapshotID("snap-1")
	if err != nil {
		t.Fatal(err)
	}
	snap, err := FromSnapshot(snapID.WithProject(project))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := snap.Image(); ok {
		t.Fatal("snapshot source looked like an image")
	}
	zone, err := ParseZone("az-1")
	if err != nil {
		t.Fatal(err)
	}
	base := VolumeSpec{Size: GiB, Zone: zone, Type: VolumeTypeGP2}
	blankSpec := base
	blankSpec.Source = blank
	snapSpec := base
	snapSpec.Source = snap
	imageSpec := base
	imageSpec.Source = image
	otherImg, err := ParseImageName("projects/shared/regions/vws-1/images/ubuntu-22-04")
	if err != nil {
		t.Fatal(err)
	}
	other, err := FromImage(otherImg)
	if err != nil {
		t.Fatal(err)
	}
	otherSpec := base
	otherSpec.Source = other
	if blankSpec == snapSpec || blankSpec == imageSpec || snapSpec == imageSpec || imageSpec == otherSpec {
		t.Fatal("distinct sources compared equal")
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

func TestScopedIDsSurviveCodec(t *testing.T) {
	now := time.Date(2026, 10, 9, 1, 2, 3, 4000, time.UTC)
	project, err := ParseProject("a123")
	if err != nil {
		t.Fatal(err)
	}
	vol := availableVolume(t, now)
	vol.ID = vol.ID.WithProject(project)
	opID := NewOperationID().WithProject(project)
	vol.State = Creating{Op: opID}
	snap := NewSnapshotID().WithProject(project)
	src, err := FromSnapshot(snap)
	if err != nil {
		t.Fatal(err)
	}
	vol.Spec.Source = src
	raw, err := MarshalRecord(vol)
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnmarshalRecord(raw)
	if err != nil {
		t.Fatal(err)
	}
	decoded := got.(Volume)
	if decoded.ID != vol.ID {
		t.Fatalf("id %s", decoded.ID)
	}
	st, ok := decoded.State.(Creating)
	if !ok || st.Op != opID {
		t.Fatalf("op %v", decoded.State)
	}
	id, ok := decoded.Spec.Source.Snapshot()
	if !ok || id != snap {
		t.Fatalf("source %v", decoded.Spec.Source)
	}
	legacy := availableVolume(t, now)
	legacyOp := NewOperationID()
	legacy.State = Deleting{Op: legacyOp}
	raw, err = MarshalRecord(legacy)
	if err != nil {
		t.Fatal(err)
	}
	got, err = UnmarshalRecord(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.(Volume).State.(Deleting).Op != legacyOp {
		t.Fatal("legacy op")
	}
}

func TestImageSourceSurvivesCodec(t *testing.T) {
	now := time.Date(2026, 10, 9, 1, 2, 3, 4000, time.UTC)
	img, err := ParseImageName("projects/shared/regions/vws-1/images/ubuntu-24-04")
	if err != nil {
		t.Fatal(err)
	}
	src, err := FromImage(img)
	if err != nil {
		t.Fatal(err)
	}
	vol := availableVolume(t, now)
	vol.Spec.Source = src
	raw, err := MarshalRecord(vol)
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnmarshalRecord(raw)
	if err != nil {
		t.Fatal(err)
	}
	decoded, ok := got.(Volume).Spec.Source.Image()
	if !ok || decoded != img {
		t.Fatalf("image %v", got.(Volume).Spec.Source)
	}
	op := Operation{
		ID: NewOperationID(), Work: VolumeCreation{Volume: vol.ID, Spec: vol.Spec},
		State: OpRunning{}, Meta: fresh(now),
	}
	raw, err = MarshalRecord(op)
	if err != nil {
		t.Fatal(err)
	}
	got, err = UnmarshalRecord(raw)
	if err != nil {
		t.Fatal(err)
	}
	work := got.(Operation).Work.(VolumeCreation)
	if decoded, ok = work.Spec.Source.Image(); !ok || decoded != img {
		t.Fatalf("op image %v", work.Spec.Source)
	}
	if refs := work.Refs(); len(refs) != 1 || refs[0] != vol.ID {
		t.Fatalf("refs %v", refs)
	}
	blank := availableVolume(t, now)
	raw, err = MarshalRecord(blank)
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{"", "snap-1", "projects/a123/snapshots/snap-1"} {
		body := bytesReplaceSource(t, raw, source)
		got, err = UnmarshalRecord(body)
		if err != nil {
			t.Fatalf("source %q: %v", source, err)
		}
		want := NoSource()
		if source != "" {
			id, err := ParseSnapshotID(source)
			if err != nil {
				t.Fatal(err)
			}
			want, err = FromSnapshot(id)
			if err != nil {
				t.Fatal(err)
			}
		}
		if got.(Volume).Spec.Source != want {
			t.Fatalf("source %q decoded %v", source, got.(Volume).Spec.Source)
		}
	}
	bad := bytesReplaceSource(t, raw, "projects/shared/regions/vws-1/images/Bad")
	if _, err := UnmarshalRecord(bad); err == nil || !strings.Contains(err.Error(), "image name") {
		t.Fatalf("bad image %v", err)
	}
}

func bytesReplaceSource(t *testing.T, raw []byte, source string) []byte {
	t.Helper()
	const from = `"source":""`
	if !bytes.Contains(raw, []byte(from)) {
		t.Fatalf("payload %s", raw)
	}
	quoted, err := json.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	return bytes.Replace(raw, []byte(from), append([]byte(`"source":`), quoted...), 1)
}

func TestReclaimDoesNotHoldThePath(t *testing.T) {
	path := ObjectPath{Bucket: mustBucket(t), Key: mustKey(t, "a.bin")}
	gen := NewGeneration()
	head := ObjectPurge{Object: ObjectRef{Path: path, Generation: gen}}
	reclaim := head
	reclaim.Reclaim = true
	if !hasRef(head.Refs(), path) || hasRef(reclaim.Refs(), path) {
		t.Fatalf("head %v reclaim %v", head.Refs(), reclaim.Refs())
	}
	op := Operation{ID: NewOperationID(), Work: reclaim, State: OpSucceeded{}, Meta: fresh(time.Unix(0, 0).UTC())}
	raw, err := MarshalRecord(op)
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnmarshalRecord(raw)
	if err != nil {
		t.Fatal(err)
	}
	loaded, ok := got.(Operation).Work.(ObjectPurge)
	if !ok || !loaded.Reclaim || hasRef(loaded.Refs(), path) {
		t.Fatalf("loaded %+v", got.(Operation).Work)
	}
}

func hasRef(refs []Ref, want Ref) bool {
	for _, ref := range refs {
		if ref.String() == want.String() {
			return true
		}
	}
	return false
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
	other.Spec.Type = VolumeTypeGP2
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
		Spec:  VolumeSpec{Size: GiB, Zone: zone, Type: VolumeTypeGP2, Source: NoSource()},
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
	img, err := ParseImageName("projects/shared/regions/vws-1/images/ubuntu-24-04")
	if err != nil {
		t.Fatal(err)
	}
	imageSrc, err := FromImage(img)
	if err != nil {
		t.Fatal(err)
	}
	imageVol := vol
	imageVol.Spec.Source = imageSrc
	out := []Record{creating, vol, imageVol, snap, bucket, obj, TokenRecord{Claim: claim, Op: NewOperationID(), Meta: fresh(now)}}
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
	if err := (CreateSnapshotRequest{Source: NewVolumeID(), Description: "bad\n"}).Validate(); err == nil {
		t.Fatal("request description")
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
	const image = "projects/shared/regions/vws-1/images/ubuntu-24-04"
	img, err := ParseImageName(image)
	if err != nil {
		t.Fatal(err)
	}
	if img.String() != image {
		t.Fatalf("image %s", img.String())
	}
	again, err := ParseImageName(img.String())
	if err != nil || again != img {
		t.Fatalf("round trip %v %v", again, err)
	}
	for _, bad := range []string{
		"images/debian",
		"projects/shared/zones/vws-1/images/ubuntu-24-04",
		"projects/Shared/regions/vws-1/images/ubuntu-24-04",
		"projects/shared/regions/vws-1/images/ubuntu-24-04/",
		"projects/shared/regions/vws-1/images/",
		"",
	} {
		if _, err := ParseImageName(bad); err == nil {
			t.Fatalf("accepted %q", bad)
		}
	}
}

func sampleWork(t *testing.T, k Kind, vol Volume, snap Snapshot, bucket Bucket, obj Object) Work {
	t.Helper()
	switch k {
	case KindCreateVolume:
		return VolumeCreation{Volume: vol.ID, Spec: vol.Spec}
	case KindDeleteVolume:
		return VolumeDeletion{Volume: vol.ID}
	case KindExpandVolume:
		return VolumeExpansion{Volume: vol.ID, Size: 2 * vol.Spec.Size}
	case KindModifyVolume:
		labels, err := ParseLabels(map[string]string{"env": "prod"})
		if err != nil {
			t.Fatal(err)
		}
		return VolumeModification{Volume: vol.ID, Labels: labels, Type: VolumeTypeStandard}
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

func TestWriteObjectFingerprintCoversBytesAndEtag(t *testing.T) {
	bucket, err := ParseBucketName("projects/p1/buckets/logs")
	if err != nil {
		t.Fatal(err)
	}
	key, err := ParseObjectKey("day.bin")
	if err != nil {
		t.Fatal(err)
	}
	req := WriteObjectRequest{Path: ObjectPath{Bucket: bucket, Key: key}, Size: 3, SHA256: Digest{1}, Expected: 2}
	base, err := HashRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	for name, mut := range map[string]func(*WriteObjectRequest){
		"sha":  func(r *WriteObjectRequest) { r.SHA256 = Digest{2} },
		"size": func(r *WriteObjectRequest) { r.Size = 4 },
		"etag": func(r *WriteObjectRequest) { r.Expected = 3 },
	} {
		other := req
		mut(&other)
		got, err := HashRequest(other)
		if err != nil {
			t.Fatal(err)
		}
		if got == base {
			t.Fatalf("%s change hashed the same", name)
		}
	}
	if (WriteObjectRequest{Path: req.Path, Size: 0}).Validate() == nil {
		t.Fatal("a write without a sha256 validated")
	}
}

func TestExpandGrowsOnlyAndStoresSizeOnCompletion(t *testing.T) {
	now := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	vol := availableVolume(t, now)
	avail, ok := vol.AsAvailable()
	if !ok {
		t.Fatal("not available")
	}
	for _, size := range []Bytes{vol.Spec.Size - GiB, vol.Spec.Size} {
		if _, err := avail.Expand(Start{Op: NewOperationID(), Now: now}, size); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("size %d: %v", size, err)
		}
	}
	want := vol.Spec.Size + GiB
	admitted, err := avail.Expand(Start{Op: NewOperationID(), Now: now}, want)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := admitted.Change.Writes()[0].(Guard); !ok {
		t.Fatal("admission did not guard the volume row")
	}
	run, ok := AsRunning[VolumeExpansion](admitted.Op)
	if !ok {
		t.Fatal("not running")
	}
	ch, err := avail.Expanded(run, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	grown := putNext(t, ch, "volume").(Volume)
	if grown.Spec.Size != want || grown.Meta.Version != vol.Meta.Version+1 {
		t.Fatalf("size %d version %d", grown.Spec.Size, grown.Meta.Version)
	}
	if _, ok := grown.AsAvailable(); !ok {
		t.Fatalf("state %v", grown.State)
	}
	if op := putNext(t, ch, "operation").(Operation); op.Phase() != PhaseSucceeded {
		t.Fatalf("phase %s", op.Phase())
	}
}

func TestExpandRequestValidateAndFingerprint(t *testing.T) {
	id := NewVolumeID()
	req := ExpandVolumeRequest{Volume: id, Size: 2 * GiB, Expected: 1}
	if err := req.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []ExpandVolumeRequest{{Size: GiB}, {Volume: id, Size: GiB / 2}, {Volume: id, Size: maxVolume + GiB}} {
		if bad.Validate() == nil {
			t.Fatalf("%+v validated", bad)
		}
	}
	base, err := HashRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	for name, mut := range map[string]func(*ExpandVolumeRequest){
		"size": func(r *ExpandVolumeRequest) { r.Size = 3 * GiB },
		"etag": func(r *ExpandVolumeRequest) { r.Expected = 2 },
	} {
		other := req
		mut(&other)
		got, err := HashRequest(other)
		if err != nil {
			t.Fatal(err)
		}
		if got == base {
			t.Fatalf("%s change hashed the same", name)
		}
	}
}

func TestFailedVolumeSurvivesCodec(t *testing.T) {
	now := time.Date(2026, 10, 9, 1, 2, 3, 4000, time.UTC)
	reason := Status{Code: CodeDeadlineExceeded, Message: "the CDI DataVolume failed 4 times"}
	for _, st := range []Lifecycle{Failed{Reason: reason}, Deleting{Op: NewOperationID(), Failed: reason}, Deleting{Op: NewOperationID()}} {
		vol := availableVolume(t, now)
		vol.State = st
		raw, err := MarshalRecord(vol)
		if err != nil {
			t.Fatal(err)
		}
		got, err := UnmarshalRecord(raw)
		if err != nil {
			t.Fatal(err)
		}
		if got.(Volume).State != st {
			t.Fatalf("state %#v want %#v\n%s", got.(Volume).State, st, raw)
		}
	}
}

// A purge finished for an absent object has no generation and must decode.
func TestPurgeWithoutGenerationSurvivesCodec(t *testing.T) {
	now := time.Date(2026, 10, 14, 1, 2, 3, 0, time.UTC)
	bucket, err := ParseBucketName("projects/a123456789012/buckets/b-1")
	if err != nil {
		t.Fatal(err)
	}
	key, err := ParseObjectKey("k/absent.txt")
	if err != nil {
		t.Fatal(err)
	}
	op := Operation{ID: NewOperationID(), Work: ObjectPurge{Object: ObjectRef{Path: ObjectPath{Bucket: bucket, Key: key}}}, State: OpSucceeded{}, Meta: fresh(now)}
	raw, err := MarshalRecord(op)
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnmarshalRecord(raw)
	if err != nil {
		t.Fatalf("decode: %v\n%s", err, raw)
	}
	if !SameRecord(op, got) {
		t.Fatalf("round trip\n%s", raw)
	}
}

func TestModifyStoresLabelsAndTypeAtAdmission(t *testing.T) {
	now := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	vol := availableVolume(t, now)
	avail, ok := vol.AsAvailable()
	if !ok {
		t.Fatal("not available")
	}
	labels, err := ParseLabels(map[string]string{"team": "a"})
	if err != nil {
		t.Fatal(err)
	}
	other := VolumeTypeStandard
	if vol.Spec.Type == VolumeTypeStandard {
		other = VolumeTypeGP2
	}
	if _, err := avail.Modify(Start{Op: NewOperationID(), Now: now}, labels, VolumeTypeUnspecified); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("unspecified type: %v", err)
	}
	admitted, err := avail.Modify(Start{Op: NewOperationID(), Now: now}, labels, other)
	if err != nil {
		t.Fatal(err)
	}
	next := putNext(t, admitted.Change, "volume").(Volume)
	if next.Labels != labels || next.Spec.Type != other || next.Meta.Version != vol.Meta.Version+1 || next.Spec.Size != vol.Spec.Size {
		t.Fatalf("volume %+v", next)
	}
	if _, ok := next.AsAvailable(); !ok {
		t.Fatalf("state %v", next.State)
	}
	if admitted.Op.Phase() != PhaseSucceeded {
		t.Fatalf("phase %s", admitted.Op.Phase())
	}
	same, err := avail.Modify(Start{Op: NewOperationID(), Now: now}, vol.Labels, vol.Spec.Type)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := same.Change.Writes()[0].(Guard); !ok || same.Op.Phase() != PhaseSucceeded {
		t.Fatal("no-op modify wrote the volume")
	}
}

func TestModifyRequestValidateAndFingerprint(t *testing.T) {
	id := NewVolumeID()
	req := ModifyVolumeRequest{Volume: id, SetLabels: true, Expected: 1}
	if err := req.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []ModifyVolumeRequest{{SetLabels: true}, {Volume: id}, {Volume: id, Type: VolumeType(9)}} {
		if bad.Validate() == nil {
			t.Fatalf("%+v validated", bad)
		}
	}
	base, err := HashRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	labels, err := ParseLabels(map[string]string{"a": "b"})
	if err != nil {
		t.Fatal(err)
	}
	for name, mut := range map[string]func(*ModifyVolumeRequest){
		"labels":     func(r *ModifyVolumeRequest) { r.Labels = labels },
		"set-labels": func(r *ModifyVolumeRequest) { r.SetLabels = false; r.Type = VolumeTypeGP2 },
		"type":       func(r *ModifyVolumeRequest) { r.Type = VolumeTypeGP2 },
		"etag":       func(r *ModifyVolumeRequest) { r.Expected = 2 },
	} {
		other := req
		mut(&other)
		got, err := HashRequest(other)
		if err != nil {
			t.Fatal(err)
		}
		if got == base {
			t.Fatalf("%s change hashed the same", name)
		}
	}
}

func TestImageNameSegments(t *testing.T) {
	img, err := ParseImageName("projects/a123/regions/vws-1/images/ubuntu-24-04")
	if err != nil {
		t.Fatal(err)
	}
	if img.Project().String() != "a123" || img.Region().String() != "vws-1" || img.ID() != "ubuntu-24-04" {
		t.Fatalf("segments %q %q %q", img.Project(), img.Region(), img.ID())
	}
	var zero ImageName
	if !zero.Project().IsZero() || !zero.Region().IsZero() || zero.ID() != "" {
		t.Fatal("zero name has segments")
	}
}
