package model

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"
)

type envelope struct {
	Record string          `json:"record"`
	Body   json.RawMessage `json:"body"`
}

type timeWire int64

func timeTo(t time.Time) timeWire {
	if t.IsZero() {
		return 0
	}
	return timeWire(t.UTC().UnixMicro())
}

func timeFrom(w timeWire) time.Time {
	if w == 0 {
		return time.Time{}
	}
	return time.UnixMicro(int64(w)).UTC()
}

type metaWire struct {
	Version Version  `json:"version"`
	Create  timeWire `json:"create"`
	Update  timeWire `json:"update"`
}

func metaTo(m Meta) metaWire {
	return metaWire{Version: m.Version, Create: timeTo(m.CreateTime), Update: timeTo(m.UpdateTime)}
}

func metaFrom(w metaWire) Meta {
	return Meta{Version: w.Version, CreateTime: timeFrom(w.Create), UpdateTime: timeFrom(w.Update)}
}

type lifeWire struct {
	Kind string `json:"kind"`
	Op   string `json:"op"`
}

func lifeTo(s Lifecycle) (lifeWire, error) {
	switch s := s.(type) {
	case Creating:
		return lifeWire{Kind: "creating", Op: s.Op.ID()}, nil
	case Available:
		return lifeWire{Kind: "available"}, nil
	case Deleting:
		return lifeWire{Kind: "deleting", Op: s.Op.ID()}, nil
	default:
		return lifeWire{}, fmt.Errorf("lifecycle %T", s)
	}
}

func lifeFrom(w lifeWire) (Lifecycle, error) {
	switch w.Kind {
	case "creating":
		id, err := ParseOperationID(w.Op)
		if err != nil {
			return nil, err
		}
		return Creating{Op: id}, nil
	case "available":
		return Available{}, nil
	case "deleting":
		id, err := ParseOperationID(w.Op)
		if err != nil {
			return nil, err
		}
		return Deleting{Op: id}, nil
	default:
		return nil, fmt.Errorf("lifecycle %q", w.Kind)
	}
}

type specWire struct {
	Size   int64  `json:"size"`
	Zone   string `json:"zone"`
	Type   string `json:"type"`
	Source string `json:"source"`
}

func specTo(s VolumeSpec) specWire {
	src := ""
	if id, ok := s.Source.Snapshot(); ok {
		src = id.ID()
	}
	return specWire{Size: int64(s.Size), Zone: s.Zone.String(), Type: s.Type.String(), Source: src}
}

func specFrom(w specWire) (VolumeSpec, error) {
	z, err := ParseZone(w.Zone)
	if err != nil {
		return VolumeSpec{}, err
	}
	t, err := ParseVolumeType(w.Type)
	if err != nil {
		return VolumeSpec{}, err
	}
	src := NoSource()
	if w.Source != "" {
		id, err := ParseSnapshotID(w.Source)
		if err != nil {
			return VolumeSpec{}, err
		}
		src, err = FromSnapshot(id)
		if err != nil {
			return VolumeSpec{}, err
		}
	}
	return VolumeSpec{Size: Bytes(w.Size), Zone: z, Type: t, Source: src}, nil
}

type claimWire struct {
	Token string `json:"token"`
	Hash  string `json:"hash"`
}

func claimTo(c TokenClaim) claimWire {
	return claimWire{Token: c.Token.String(), Hash: c.Fingerprint.String()}
}

func claimFrom(w claimWire) (TokenClaim, error) {
	if w.Token == "" {
		return TokenClaim{}, nil
	}
	tok, err := ParseClientToken(w.Token)
	if err != nil {
		return TokenClaim{}, err
	}
	fp, err := ParseDigest(w.Hash)
	if err != nil {
		return TokenClaim{}, err
	}
	return TokenClaim{Token: tok, Fingerprint: Fingerprint(fp)}, nil
}

type stateWire struct {
	Kind      string   `json:"kind"`
	Attempts  int      `json:"attempts"`
	NotBefore timeWire `json:"not_before"`
	Code      Code     `json:"code"`
	Message   string   `json:"message"`
}

func stateTo(s OpState) (stateWire, error) {
	switch s := s.(type) {
	case OpRunning:
		return stateWire{Kind: "running", Attempts: s.Attempts, NotBefore: timeTo(s.NotBefore), Code: s.LastError.Code, Message: s.LastError.Message}, nil
	case OpCompensating:
		return stateWire{Kind: "compensating", Attempts: s.Attempts, NotBefore: timeTo(s.NotBefore), Code: s.Cause.Code, Message: s.Cause.Message}, nil
	case OpSucceeded:
		return stateWire{Kind: "succeeded"}, nil
	case OpFailed:
		return stateWire{Kind: "failed", Code: s.Cause.Code, Message: s.Cause.Message}, nil
	default:
		return stateWire{}, fmt.Errorf("op state %T", s)
	}
}

func stateFrom(w stateWire) (OpState, error) {
	st := Status{Code: w.Code, Message: w.Message}
	switch w.Kind {
	case "running":
		return OpRunning{Attempts: w.Attempts, NotBefore: timeFrom(w.NotBefore), LastError: st}, nil
	case "compensating":
		return OpCompensating{Cause: st, Attempts: w.Attempts, NotBefore: timeFrom(w.NotBefore)}, nil
	case "succeeded":
		return OpSucceeded{}, nil
	case "failed":
		if st.IsZero() {
			return nil, fmt.Errorf("failed operation without cause")
		}
		return OpFailed{Cause: st}, nil
	default:
		return nil, fmt.Errorf("op state %q", w.Kind)
	}
}

type workWire struct {
	Kind        string `json:"kind"`
	Volume      string `json:"volume"`
	Snapshot    string `json:"snapshot"`
	Bucket      string `json:"bucket"`
	Key         string `json:"key"`
	Generation  string `json:"generation"`
	Size        int64  `json:"size"`
	Zone        string `json:"zone"`
	Type        string `json:"type"`
	Source      string `json:"source"`
	SHA256      string `json:"sha256"`
	ContentType string `json:"content_type"`
	Deadline    int64  `json:"deadline"`
}

func workTo(w Work) (workWire, error) {
	switch w := w.(type) {
	case VolumeCreation:
		spec := specTo(w.Spec)
		return workWire{Kind: w.Kind().String(), Volume: w.Volume.wire(), Size: spec.Size, Zone: spec.Zone, Type: spec.Type, Source: spec.Source}, nil
	case VolumeDeletion:
		return workWire{Kind: w.Kind().String(), Volume: w.Volume.wire(), Zone: w.Zone.String()}, nil
	case SnapshotCreation:
		return workWire{Kind: w.Kind().String(), Snapshot: w.Snapshot.wire(), Volume: w.Source.wire(), Zone: w.Region.String()}, nil
	case SnapshotDeletion:
		return workWire{Kind: w.Kind().String(), Snapshot: w.Snapshot.wire(), Zone: w.Region.String()}, nil
	case BucketCreation:
		return workWire{Kind: w.Kind().String(), Bucket: w.Bucket.wire()}, nil
	case BucketDeletion:
		return workWire{Kind: w.Kind().String(), Bucket: w.Bucket.wire()}, nil
	case ObjectUpload:
		return workWire{
			Kind: w.Kind().String(), Bucket: w.Object.Path.Bucket.wire(), Key: w.Object.Path.Key.String(),
			Generation: w.Object.Generation.String(), Size: int64(w.Size), SHA256: w.SHA256.String(),
			ContentType: w.ContentType, Deadline: int64(timeTo(w.Deadline)),
		}, nil
	case ObjectPurge:
		return workWire{
			Kind: w.Kind().String(), Bucket: w.Object.Path.Bucket.wire(), Key: w.Object.Path.Key.String(),
			Generation: w.Object.Generation.String(),
		}, nil
	default:
		return workWire{}, fmt.Errorf("work %T", w)
	}
}

func workFrom(w workWire) (Work, error) {
	switch w.Kind {
	case KindCreateVolume.String():
		vol, err := ParseVolumeID(w.Volume)
		if err != nil {
			return nil, err
		}
		spec, err := specFrom(specWire{Size: w.Size, Zone: w.Zone, Type: w.Type, Source: w.Source})
		if err != nil {
			return nil, err
		}
		return VolumeCreation{Volume: vol, Spec: spec}, nil
	case KindDeleteVolume.String():
		vol, err := ParseVolumeID(w.Volume)
		if err != nil {
			return nil, err
		}
		var zone Zone
		if w.Zone != "" {
			zone, err = ParseZone(w.Zone)
			if err != nil {
				return nil, err
			}
		}
		return VolumeDeletion{Volume: vol, Zone: zone}, nil
	case KindCreateSnapshot.String():
		snap, err := ParseSnapshotID(w.Snapshot)
		if err != nil {
			return nil, err
		}
		vol, err := ParseVolumeID(w.Volume)
		if err != nil {
			return nil, err
		}
		var region Region
		if w.Zone != "" {
			region, err = ParseRegion(w.Zone)
			if err != nil {
				return nil, err
			}
		}
		return SnapshotCreation{Snapshot: snap, Source: vol, Region: region}, nil
	case KindDeleteSnapshot.String():
		snap, err := ParseSnapshotID(w.Snapshot)
		if err != nil {
			return nil, err
		}
		var region Region
		if w.Zone != "" {
			region, err = ParseRegion(w.Zone)
			if err != nil {
				return nil, err
			}
		}
		return SnapshotDeletion{Snapshot: snap, Region: region}, nil
	case KindCreateBucket.String():
		b, err := ParseBucketName(w.Bucket)
		if err != nil {
			return nil, err
		}
		return BucketCreation{Bucket: b}, nil
	case KindDeleteBucket.String():
		b, err := ParseBucketName(w.Bucket)
		if err != nil {
			return nil, err
		}
		return BucketDeletion{Bucket: b}, nil
	case KindPutObject.String():
		ref, err := objectRefFrom(w)
		if err != nil {
			return nil, err
		}
		sum, err := digestOrZero(w.SHA256)
		if err != nil {
			return nil, err
		}
		return ObjectUpload{
			Object: ref, Size: Bytes(w.Size), SHA256: sum, ContentType: w.ContentType,
			Deadline: timeFrom(timeWire(w.Deadline)),
		}, nil
	case KindDeleteObject.String():
		ref, err := objectRefFrom(w)
		if err != nil {
			return nil, err
		}
		return ObjectPurge{Object: ref}, nil
	default:
		return nil, fmt.Errorf("work %q", w.Kind)
	}
}

func objectRefFrom(w workWire) (ObjectRef, error) {
	b, err := ParseBucketName(w.Bucket)
	if err != nil {
		return ObjectRef{}, err
	}
	k, err := ParseObjectKey(w.Key)
	if err != nil {
		return ObjectRef{}, err
	}
	g, err := ParseGeneration(w.Generation)
	if err != nil {
		return ObjectRef{}, err
	}
	return ObjectRef{Path: ObjectPath{Bucket: b, Key: k}, Generation: g}, nil
}

func digestOrZero(s string) (Digest, error) {
	if s == "" {
		return Digest{}, nil
	}
	return ParseDigest(s)
}

// MarshalRecord encodes r with the one codec both stores use.
func MarshalRecord(r Record) ([]byte, error) {
	body, kind, err := marshalBody(r)
	if err != nil {
		return nil, err
	}
	return json.Marshal(envelope{Record: kind, Body: body})
}

func marshalBody(r Record) ([]byte, string, error) {
	switch r := r.(type) {
	case Volume:
		life, err := lifeTo(r.State)
		if err != nil {
			return nil, "", err
		}
		b, err := json.Marshal(struct {
			ID   string   `json:"id"`
			Spec specWire `json:"spec"`
			Life lifeWire `json:"life"`
			Meta metaWire `json:"meta"`
		}{ID: r.ID.wire(), Spec: specTo(r.Spec), Life: life, Meta: metaTo(r.Meta)})
		return b, "volume", err
	case Snapshot:
		life, err := lifeTo(r.State)
		if err != nil {
			return nil, "", err
		}
		b, err := json.Marshal(struct {
			ID     string   `json:"id"`
			Source string   `json:"source"`
			Region string   `json:"region,omitempty"`
			Size   int64    `json:"size"`
			Life   lifeWire `json:"life"`
			Meta   metaWire `json:"meta"`
		}{ID: r.ID.wire(), Source: r.Source.wire(), Region: r.Region.String(), Size: int64(r.Size), Life: life, Meta: metaTo(r.Meta)})
		return b, "snapshot", err
	case Bucket:
		life, err := lifeTo(r.State)
		if err != nil {
			return nil, "", err
		}
		b, err := json.Marshal(struct {
			Name   string   `json:"name"`
			Region string   `json:"region,omitempty"`
			Life   lifeWire `json:"life"`
			Meta   metaWire `json:"meta"`
		}{Name: r.Name.wire(), Region: r.Region.String(), Life: life, Meta: metaTo(r.Meta)})
		return b, "bucket", err
	case Object:
		b, err := json.Marshal(struct {
			Bucket      string   `json:"bucket"`
			Key         string   `json:"key"`
			Generation  string   `json:"generation"`
			Size        int64    `json:"size"`
			ETag        string   `json:"etag"`
			SHA256      string   `json:"sha256"`
			ContentType string   `json:"content_type"`
			UploadedAt  timeWire `json:"uploaded_at"`
			Meta        metaWire `json:"meta"`
		}{
			Bucket: r.Path.Bucket.wire(), Key: r.Path.Key.String(), Generation: r.Generation.String(),
			Size: int64(r.Size), ETag: string(r.ETag), SHA256: r.SHA256.String(), ContentType: r.ContentType,
			UploadedAt: timeTo(r.UploadedAt), Meta: metaTo(r.Meta),
		})
		return b, "object", err
	case Operation:
		work, err := workTo(r.Work)
		if err != nil {
			return nil, "", err
		}
		st, err := stateTo(r.State)
		if err != nil {
			return nil, "", err
		}
		b, err := json.Marshal(struct {
			ID    string    `json:"id"`
			Work  workWire  `json:"work"`
			State stateWire `json:"state"`
			Claim claimWire `json:"claim"`
			Meta  metaWire  `json:"meta"`
		}{ID: r.ID.wire(), Work: work, State: st, Claim: claimTo(r.Claim), Meta: metaTo(r.Meta)})
		return b, "operation", err
	case TokenRecord:
		b, err := json.Marshal(struct {
			Project string    `json:"project,omitempty"`
			Claim   claimWire `json:"claim"`
			Op      string    `json:"op"`
			Meta    metaWire  `json:"meta"`
		}{Project: r.Project.String(), Claim: claimTo(r.Claim), Op: r.Op.wire(), Meta: metaTo(r.Meta)})
		return b, "token", err
	default:
		return nil, "", fmt.Errorf("record %T", r)
	}
}

// UnmarshalRecord decodes a record produced by MarshalRecord.
func UnmarshalRecord(data []byte) (Record, error) {
	var env envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, err
	}
	switch env.Record {
	case "volume":
		var w struct {
			ID   string   `json:"id"`
			Spec specWire `json:"spec"`
			Life lifeWire `json:"life"`
			Meta metaWire `json:"meta"`
		}
		if err := json.Unmarshal(env.Body, &w); err != nil {
			return nil, err
		}
		id, err := ParseVolumeID(w.ID)
		if err != nil {
			return nil, err
		}
		spec, err := specFrom(w.Spec)
		if err != nil {
			return nil, err
		}
		life, err := lifeFrom(w.Life)
		if err != nil {
			return nil, err
		}
		return Volume{ID: id, Spec: spec, State: life, Meta: metaFrom(w.Meta)}, nil
	case "snapshot":
		var w struct {
			ID     string   `json:"id"`
			Source string   `json:"source"`
			Region string   `json:"region"`
			Size   int64    `json:"size"`
			Life   lifeWire `json:"life"`
			Meta   metaWire `json:"meta"`
		}
		if err := json.Unmarshal(env.Body, &w); err != nil {
			return nil, err
		}
		id, err := ParseSnapshotID(w.ID)
		if err != nil {
			return nil, err
		}
		src, err := ParseVolumeID(w.Source)
		if err != nil {
			return nil, err
		}
		var region Region
		if w.Region != "" {
			region, err = ParseRegion(w.Region)
			if err != nil {
				return nil, err
			}
		}
		life, err := lifeFrom(w.Life)
		if err != nil {
			return nil, err
		}
		return Snapshot{ID: id, Source: src, Region: region, Size: Bytes(w.Size), State: life, Meta: metaFrom(w.Meta)}, nil
	case "bucket":
		var w struct {
			Name   string   `json:"name"`
			Region string   `json:"region"`
			Life   lifeWire `json:"life"`
			Meta   metaWire `json:"meta"`
		}
		if err := json.Unmarshal(env.Body, &w); err != nil {
			return nil, err
		}
		name, err := ParseBucketName(w.Name)
		if err != nil {
			return nil, err
		}
		var region Region
		if w.Region != "" {
			region, err = ParseRegion(w.Region)
			if err != nil {
				return nil, err
			}
		}
		life, err := lifeFrom(w.Life)
		if err != nil {
			return nil, err
		}
		return Bucket{Name: name, Region: region, State: life, Meta: metaFrom(w.Meta)}, nil
	case "object":
		var w struct {
			Bucket      string   `json:"bucket"`
			Key         string   `json:"key"`
			Generation  string   `json:"generation"`
			Size        int64    `json:"size"`
			ETag        string   `json:"etag"`
			SHA256      string   `json:"sha256"`
			ContentType string   `json:"content_type"`
			UploadedAt  timeWire `json:"uploaded_at"`
			Meta        metaWire `json:"meta"`
		}
		if err := json.Unmarshal(env.Body, &w); err != nil {
			return nil, err
		}
		b, err := ParseBucketName(w.Bucket)
		if err != nil {
			return nil, err
		}
		k, err := ParseObjectKey(w.Key)
		if err != nil {
			return nil, err
		}
		g, err := ParseGeneration(w.Generation)
		if err != nil {
			return nil, err
		}
		sum, err := digestOrZero(w.SHA256)
		if err != nil {
			return nil, err
		}
		return Object{
			Path: ObjectPath{Bucket: b, Key: k}, Generation: g, Size: Bytes(w.Size),
			ETag: ETag(w.ETag), SHA256: sum, ContentType: w.ContentType,
			UploadedAt: timeFrom(w.UploadedAt), Meta: metaFrom(w.Meta),
		}, nil
	case "operation":
		var w struct {
			ID    string    `json:"id"`
			Work  workWire  `json:"work"`
			State stateWire `json:"state"`
			Claim claimWire `json:"claim"`
			Meta  metaWire  `json:"meta"`
		}
		if err := json.Unmarshal(env.Body, &w); err != nil {
			return nil, err
		}
		id, err := ParseOperationID(w.ID)
		if err != nil {
			return nil, err
		}
		work, err := workFrom(w.Work)
		if err != nil {
			return nil, err
		}
		st, err := stateFrom(w.State)
		if err != nil {
			return nil, err
		}
		claim, err := claimFrom(w.Claim)
		if err != nil {
			return nil, err
		}
		return Operation{ID: id, Work: work, State: st, Claim: claim, Meta: metaFrom(w.Meta)}, nil
	case "token":
		var w struct {
			Project string    `json:"project"`
			Claim   claimWire `json:"claim"`
			Op      string    `json:"op"`
			Meta    metaWire  `json:"meta"`
		}
		if err := json.Unmarshal(env.Body, &w); err != nil {
			return nil, err
		}
		claim, err := claimFrom(w.Claim)
		if err != nil {
			return nil, err
		}
		op, err := ParseOperationID(w.Op)
		if err != nil {
			return nil, err
		}
		var project Project
		if w.Project != "" {
			project, err = ParseProject(w.Project)
			if err != nil {
				return nil, err
			}
		}
		return TokenRecord{Project: project, Claim: claim, Op: op, Meta: metaFrom(w.Meta)}, nil
	default:
		return nil, fmt.Errorf("record %q", env.Record)
	}
}

// SameRecord reports whether a and b encode to the same bytes.
func SameRecord(a, b Record) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	ab, err1 := MarshalRecord(a)
	bb, err2 := MarshalRecord(b)
	if err1 != nil || err2 != nil {
		return false
	}
	return bytes.Equal(ab, bb)
}
