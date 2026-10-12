// Package fake is the in-memory block and object backends.
package fake

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/popododo0720/vws-storage/internal/backend"
	"github.com/popododo0720/vws-storage/internal/model"
)

// Call is one applied backend call.
type Call struct {
	Method string
	ID     string
}

// Block is an in-memory Block.
type Block struct {
	mu    sync.Mutex
	vols  map[model.VolumeID]model.VolumeSpec
	snaps map[model.SnapshotID]model.VolumeID
	after func(Call)
	fail  map[string]error
	calls []Call
}

// NewBlock returns an empty block backend.
func NewBlock() *Block {
	return &Block{vols: map[model.VolumeID]model.VolumeSpec{}, snaps: map[model.SnapshotID]model.VolumeID{}, fail: map[string]error{}}
}

// AfterCall sets a hook that runs after a call is applied. nil clears it.
func (b *Block) AfterCall(fn func(Call)) {
	b.mu.Lock()
	b.after = fn
	b.mu.Unlock()
}

// FailNext makes the next call to method return err.
func (b *Block) FailNext(method string, err error) {
	b.mu.Lock()
	b.fail[method] = err
	b.mu.Unlock()
}

// Calls returns a copy of the applied calls.
func (b *Block) Calls() []Call {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]Call(nil), b.calls...)
}

// CreateVolume implements backend.Block.
func (b *Block) CreateVolume(ctx context.Context, id model.VolumeID, spec model.VolumeSpec) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("fake: %w", err)
	}
	b.mu.Lock()
	if err, ok := b.fail["CreateVolume"]; ok {
		delete(b.fail, "CreateVolume")
		b.mu.Unlock()
		return err
	}
	if cur, ok := b.vols[id]; ok {
		call := Call{Method: "CreateVolume", ID: id.String()}
		b.calls = append(b.calls, call)
		hook := b.after
		b.mu.Unlock()
		if hook != nil {
			hook(call)
		}
		grown := cur
		grown.Size = spec.Size
		if grown != spec || cur.Size < spec.Size {
			return backend.Permanent(fmt.Errorf("%w: volume", backend.ErrConflict))
		}
		return nil
	}
	if src, ok := spec.Source.Snapshot(); ok {
		if _, exists := b.snaps[src]; !exists {
			b.mu.Unlock()
			return backend.Permanent(fmt.Errorf("%w: snapshot", backend.ErrNotFound))
		}
	}
	b.vols[id] = spec
	call := Call{Method: "CreateVolume", ID: id.String()}
	b.calls = append(b.calls, call)
	hook := b.after
	b.mu.Unlock()
	if hook != nil {
		hook(call)
	}
	return nil
}

// DeleteVolume implements backend.Block.
func (b *Block) DeleteVolume(ctx context.Context, id model.VolumeID) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("fake: %w", err)
	}
	b.mu.Lock()
	if err, ok := b.fail["DeleteVolume"]; ok {
		delete(b.fail, "DeleteVolume")
		b.mu.Unlock()
		return err
	}
	delete(b.vols, id)
	call := Call{Method: "DeleteVolume", ID: id.String()}
	b.calls = append(b.calls, call)
	hook := b.after
	b.mu.Unlock()
	if hook != nil {
		hook(call)
	}
	return nil
}

// Expand implements backend.Block.
func (b *Block) Expand(ctx context.Context, id model.VolumeID, size model.Bytes) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("fake: %w", err)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err, ok := b.fail["Expand"]; ok {
		delete(b.fail, "Expand")
		return err
	}
	spec, ok := b.vols[id]
	if !ok {
		return backend.Permanent(fmt.Errorf("%w: volume", backend.ErrNotFound))
	}
	if size < spec.Size {
		return backend.Permanent(fmt.Errorf("%w: volume cannot shrink", backend.ErrInvalid))
	}
	spec.Size = size
	b.vols[id] = spec
	b.calls = append(b.calls, Call{Method: "Expand", ID: id.String()})
	return nil
}

// CreateSnapshot implements backend.Block.
func (b *Block) CreateSnapshot(ctx context.Context, id model.SnapshotID, source model.VolumeID) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("fake: %w", err)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err, ok := b.fail["CreateSnapshot"]; ok {
		delete(b.fail, "CreateSnapshot")
		return err
	}
	if _, ok := b.vols[source]; !ok {
		return backend.Permanent(fmt.Errorf("%w: volume", backend.ErrNotFound))
	}
	if cur, ok := b.snaps[id]; ok && cur != source {
		return backend.Permanent(fmt.Errorf("%w: snapshot", backend.ErrConflict))
	}
	b.snaps[id] = source
	b.calls = append(b.calls, Call{Method: "CreateSnapshot", ID: id.String()})
	return nil
}

// ClaimName is a stable made-up name. The fake has no cluster object.
func (b *Block) ClaimName(id model.VolumeID) string { return "fake-" + id.ID() }

// SnapshotObjectName is a stable made-up name. The fake has no cluster object.
func (b *Block) SnapshotObjectName(id model.SnapshotID) string { return "fake-" + id.ID() }

// DeleteSnapshot implements backend.Block.
func (b *Block) DeleteSnapshot(ctx context.Context, id model.SnapshotID) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("fake: %w", err)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err, ok := b.fail["DeleteSnapshot"]; ok {
		delete(b.fail, "DeleteSnapshot")
		return err
	}
	delete(b.snaps, id)
	b.calls = append(b.calls, Call{Method: "DeleteSnapshot", ID: id.String()})
	return nil
}

// Objects is an in-memory Object and Signer.
type Objects struct {
	mu         sync.Mutex
	base       string
	now        func() time.Time
	deleteWait <-chan struct{}
	bucket     map[model.BucketName]struct{}
	gens       map[string]stored
	expect     map[string]expect
	gets       map[string]time.Time
}

type stored struct {
	body []byte
	at   time.Time
	etag string
	sum  model.Digest
	md5  model.MD5
}

type expect struct {
	size    model.Bytes
	sha     model.Digest
	expires time.Time
}

// ObjectOptions configures the fake object backend.
type ObjectOptions struct {
	BaseURL string
	Now     func() time.Time
	// DeleteWait, when set, is received at the start of every DeleteObject.
	DeleteWait <-chan struct{}
}

// NewObjects returns an empty object backend.
func NewObjects(opts ObjectOptions) *Objects {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	base := opts.BaseURL
	if base == "" {
		base = "http://fake.invalid"
	}
	return &Objects{
		base: base, now: now, deleteWait: opts.DeleteWait,
		bucket: map[model.BucketName]struct{}{},
		gens:   map[string]stored{},
		expect: map[string]expect{},
		gets:   map[string]time.Time{},
	}
}

// CreateBucket implements backend.Object.
func (o *Objects) CreateBucket(ctx context.Context, name model.BucketName) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("fake: %w", err)
	}
	o.mu.Lock()
	o.bucket[name] = struct{}{}
	o.mu.Unlock()
	return nil
}

// DeleteBucket implements backend.Object.
func (o *Objects) DeleteBucket(ctx context.Context, name model.BucketName) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("fake: %w", err)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if _, ok := o.bucket[name]; !ok {
		return nil
	}
	prefix := name.String() + "\t"
	for k := range o.gens {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			return backend.ErrBusy
		}
	}
	delete(o.bucket, name)
	return nil
}

// Stat implements backend.Object.
func (o *Objects) Stat(ctx context.Context, ref model.ObjectRef) (model.ObservedObject, error) {
	if err := ctx.Err(); err != nil {
		return model.ObservedObject{}, fmt.Errorf("fake: %w", err)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	g, ok := o.gens[refKey(ref)]
	if !ok {
		return model.ObservedObject{}, backend.ErrNotFound
	}
	return model.ObservedObject{Size: model.Bytes(len(g.body)), ETag: model.ETag(g.etag), SHA256: g.sum, MD5: g.md5, UploadedAt: g.at}, nil
}

// DeleteObject implements backend.Object.
func (o *Objects) DeleteObject(ctx context.Context, ref model.ObjectRef) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("fake: %w", err)
	}
	if o.deleteWait != nil {
		select {
		case <-o.deleteWait:
		case <-ctx.Done():
			return fmt.Errorf("fake: %w", ctx.Err())
		}
	}
	o.mu.Lock()
	delete(o.gens, refKey(ref))
	delete(o.expect, refKey(ref))
	o.mu.Unlock()
	return nil
}

// PresignPut implements backend.Signer.
func (o *Objects) PresignPut(_ context.Context, ref model.ObjectRef, size model.Bytes, sha model.Digest, expires time.Time) (model.Locator, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if _, ok := o.gens[refKey(ref)]; ok {
		return model.Locator{}, fmt.Errorf("%w: generation", backend.ErrConflict)
	}
	exp := time.UnixMicro(expires.UTC().UnixMicro()).UTC()
	o.expect[refKey(ref)] = expect{size: size, sha: sha, expires: exp}
	return model.Locator{Method: http.MethodPut, URL: o.url(o.base, "put", ref, exp), ExpiresAt: exp}, nil
}

// PresignGet implements backend.Signer.
func (o *Objects) PresignGet(_ context.Context, ref model.ObjectRef, expires time.Time) (model.Locator, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	exp := time.UnixMicro(expires.UTC().UnixMicro()).UTC()
	o.gets[refKey(ref)] = exp
	return model.Locator{Method: http.MethodGet, URL: o.url(o.base, "get", ref, exp), ExpiresAt: exp}, nil
}

// Accept stores body at the locator from PresignPut.
func (o *Objects) Accept(loc model.Locator, body []byte) error {
	ref, exp, err := parseURL(loc.URL)
	if err != nil {
		return err
	}
	key := refKey(ref)
	o.mu.Lock()
	defer o.mu.Unlock()
	if _, ok := o.gens[key]; ok {
		return fmt.Errorf("%w: generation", backend.ErrConflict)
	}
	want, ok := o.expect[key]
	if !ok {
		return fmt.Errorf("%w: upload", backend.ErrNotFound)
	}
	if o.now().After(want.expires) || !exp.Equal(want.expires) {
		return fmt.Errorf("%w: expired", backend.ErrNotFound)
	}
	if model.Bytes(len(body)) != want.size {
		return fmt.Errorf("%w: size", backend.ErrConflict)
	}
	sum := sha256.Sum256(body)
	if !want.sha.IsZero() && sum != want.sha {
		return fmt.Errorf("%w: checksum", backend.ErrConflict)
	}
	o.gens[key] = stored{body: append([]byte(nil), body...), at: o.now(), etag: hex.EncodeToString(sum[:8]), sum: sum, md5: md5.Sum(body)}
	delete(o.expect, key)
	return nil
}

// Handler serves presigned PUT and GET URLs.
func (o *Objects) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/put", func(w http.ResponseWriter, r *http.Request) {
		loc := model.Locator{URL: "http://fake" + r.URL.RequestURI()}
		if _, _, err := parseURL(loc.URL); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, int64(model.MaxObjectBytes)+1))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := o.Accept(loc, body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		ref, exp, err := parseURL("http://x" + r.URL.RequestURI())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if !o.getAllowed(ref, exp) {
			http.NotFound(w, r)
			return
		}
		obs, err := o.Stat(r.Context(), ref)
		if err != nil {
			http.NotFound(w, r)
			return
		}
		o.mu.Lock()
		g := o.gens[refKey(ref)]
		o.mu.Unlock()
		w.Header().Set("ETag", string(obs.ETag))
		if _, err := w.Write(g.body); err != nil {
			log.Printf("fake object: write body: %v", err)
		}
	})
	return mux
}

func (o *Objects) url(base, op string, ref model.ObjectRef, exp time.Time) string {
	q := url.Values{}
	q.Set("bucket", ref.Path.Bucket.String())
	q.Set("key", ref.Path.Key.String())
	q.Set("gen", ref.Generation.String())
	q.Set("exp", strconv.FormatInt(exp.UnixMicro(), 10))
	return base + "/" + op + "?" + q.Encode()
}

func parseURL(raw string) (model.ObjectRef, time.Time, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return model.ObjectRef{}, time.Time{}, fmt.Errorf("fake: parse url: %w", err)
	}
	b, err := model.ParseBucketName(u.Query().Get("bucket"))
	if err != nil {
		return model.ObjectRef{}, time.Time{}, err
	}
	k, err := model.ParseObjectKey(u.Query().Get("key"))
	if err != nil {
		return model.ObjectRef{}, time.Time{}, err
	}
	g, err := model.ParseGeneration(u.Query().Get("gen"))
	if err != nil {
		return model.ObjectRef{}, time.Time{}, err
	}
	us, err := strconv.ParseInt(u.Query().Get("exp"), 10, 64)
	if err != nil {
		return model.ObjectRef{}, time.Time{}, fmt.Errorf("fake: parse exp: %w", err)
	}
	return model.ObjectRef{Path: model.ObjectPath{Bucket: b, Key: k}, Generation: g}, time.UnixMicro(us).UTC(), nil
}

func (o *Objects) getAllowed(ref model.ObjectRef, exp time.Time) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	until, ok := o.gets[refKey(ref)]
	return ok && exp.Equal(until) && !o.now().After(until)
}

func refKey(ref model.ObjectRef) string {
	return ref.Path.Bucket.String() + "\t" + ref.Path.Key.String() + "\t" + ref.Generation.String()
}
