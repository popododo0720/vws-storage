package storage_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/popododo0720/vws-storage/internal/backend/fake"
	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/storage"
)

var (
	envOnce    sync.Once
	env        *envtest.Environment
	errEnv     error
	kubeconfig []byte
	kc         kubernetes.Interface
	nsSeq      atomic.Int64
)

func TestMain(m *testing.M) {
	code := m.Run()
	if env != nil {
		_ = env.Stop()
	}
	os.Exit(code)
}

// kubeStore is the Kube store on a fresh namespace of one shared envtest API
// server. Without KUBEBUILDER_ASSETS the test is skipped.
func kubeStore(t *testing.T) (storage.StoreConfig, string) {
	t.Helper()
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS unset")
	}
	envOnce.Do(func() {
		env = &envtest.Environment{}
		cfg, err := env.Start()
		if err != nil {
			errEnv = err
			return
		}
		kc = kubernetes.NewForConfigOrDie(cfg)
		user, err := env.AddUser(envtest.User{Name: "vws-storage", Groups: []string{"system:masters"}}, nil)
		if err != nil {
			errEnv = err
			return
		}
		kubeconfig, errEnv = user.KubeConfig()
	})
	if errEnv != nil {
		t.Fatal(errEnv)
	}
	path := filepath.Join(t.TempDir(), "kubeconfig")
	if err := os.WriteFile(path, kubeconfig, 0o600); err != nil {
		t.Fatal(err)
	}
	ns := fmt.Sprintf("storage-%d", nsSeq.Add(1))
	obj := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}
	if _, err := kc.CoreV1().Namespaces().Create(context.Background(), obj, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	return storage.Kube(path, ns), ns
}

// kubeOpts scans every 100ms. The fakes' 5ms scan lists operations faster
// than the store's client limit of 50 calls a second.
func kubeOpts(cfg storage.StoreConfig, blk *fake.Block, obj *fake.Objects) storage.Options {
	o := opts(cfg, blk, obj)
	o.ScanInterval = 100 * time.Millisecond
	return o
}

// state is every Get and List result a client can see.
type state struct {
	volumes   []model.Volume
	snapshots []model.Snapshot
	buckets   []model.Bucket
	ops       []storage.OperationStatus
	got       []any
}

func read(t *testing.T, svc *storage.Service, project model.Project) state {
	t.Helper()
	ctx := context.Background()
	all := model.PageRequest{Size: 100}
	vols, err := svc.ListVolumes(ctx, project, model.Zone{}, all)
	if err != nil {
		t.Fatal(err)
	}
	snaps, err := svc.ListSnapshots(ctx, project, model.Region{}, all)
	if err != nil {
		t.Fatal(err)
	}
	bkts, err := svc.ListBuckets(ctx, project, all)
	if err != nil {
		t.Fatal(err)
	}
	ops, err := svc.ListOperations(ctx, storage.OperationQuery{Project: project}, all)
	if err != nil {
		t.Fatal(err)
	}
	s := state{volumes: vols.Items, snapshots: snaps.Items, buckets: bkts.Items, ops: ops.Items}
	for _, v := range vols.Items {
		got, err := svc.Volume(ctx, v.ID)
		if err != nil {
			t.Fatal(err)
		}
		s.got = append(s.got, got)
	}
	for _, sn := range snaps.Items {
		got, err := svc.Snapshot(ctx, sn.ID)
		if err != nil {
			t.Fatal(err)
		}
		s.got = append(s.got, got)
	}
	for _, b := range bkts.Items {
		got, err := svc.Bucket(ctx, b.Name)
		if err != nil {
			t.Fatal(err)
		}
		s.got = append(s.got, got)
	}
	for _, op := range ops.Items {
		got, err := svc.Operation(ctx, op.ID)
		if err != nil {
			t.Fatal(err)
		}
		s.got = append(s.got, got)
	}
	return s
}

func labeled(t *testing.T, id string, labels map[string]string) model.CreateVolumeRequest {
	t.Helper()
	req := volumeReq(t)
	vid, err := model.ParseVolumeID(id)
	if err != nil {
		t.Fatal(err)
	}
	req.ID = vid
	if req.Labels, err = model.ParseLabels(labels); err != nil {
		t.Fatal(err)
	}
	return req
}

func submitDone(t *testing.T, svc *storage.Service, tok string, req model.Request) storage.OperationStatus {
	t.Helper()
	op, err := svc.Submit(context.Background(), token(t, tok), req)
	if err != nil {
		t.Fatalf("%s: %v", tok, err)
	}
	got := waitDone(t, svc, op.ID)
	if got.Phase() != model.PhaseSucceeded {
		t.Fatalf("%s: phase %s %+v", tok, got.Phase(), got.Err())
	}
	return got
}

// A new service on the same API server sees every record the stopped one
// wrote, and finishes the operation the stopped one left once (#141 AC 2).
func TestKubeStoreSurvivesRestart(t *testing.T) {
	cfg, _ := kubeStore(t)
	blk := fake.NewBlock()
	obj := fake.NewObjects(fake.ObjectOptions{})
	svc, stop := startService(t, kubeOpts(cfg, blk, obj))
	vol := submitDone(t, svc, "vol-a", labeled(t, "data-a", map[string]string{"team": "db"}))
	submitDone(t, svc, "vol-b", labeled(t, "data-b", map[string]string{"team": "web", "tier": "hot"}))
	submitDone(t, svc, "snap", model.CreateSnapshotRequest{Source: vol.Target().(model.VolumeID), Description: "nightly"})
	bucket, err := model.ParseBucketName("logs")
	if err != nil {
		t.Fatal(err)
	}
	submitDone(t, svc, "bkt", model.CreateBucketRequest{Bucket: bucket})
	stop()

	runCtx, kill := context.WithCancel(context.Background())
	blk.AfterCall(func(c fake.Call) {
		if c.Method == "CreateVolume" {
			kill()
		}
	})
	pending, err := svc.Submit(context.Background(), token(t, "vol-c"), labeled(t, "data-c", nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Run(runCtx); err != nil {
		t.Fatal(err)
	}
	blk.AfterCall(nil)
	before := read(t, svc, model.Project{})
	if len(before.volumes) != 3 || len(before.snapshots) != 1 || len(before.buckets) != 1 || len(before.ops) != 5 {
		t.Fatalf("before restart %d volumes %d snapshots %d buckets %d operations",
			len(before.volumes), len(before.snapshots), len(before.buckets), len(before.ops))
	}
	if op, err := svc.Operation(context.Background(), pending.ID); err != nil || op.Terminal() {
		t.Fatalf("pending %+v %v", op, err)
	}
	if err := svc.Close(); err != nil {
		t.Fatal(err)
	}

	next, err := storage.Open(context.Background(), kubeOpts(cfg, blk, obj))
	if err != nil {
		t.Fatal(err)
	}
	if after := read(t, next, model.Project{}); !reflect.DeepEqual(before, after) {
		t.Fatalf("after restart\n%+v\nwant\n%+v", after, before)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = next.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
		next.Close()
	})
	if got := waitDone(t, next, pending.ID); got.Phase() != model.PhaseSucceeded {
		t.Fatalf("resumed %s %+v", got.Phase(), got.Err())
	}
	creates := 0
	for _, c := range blk.Calls() {
		if c.Method == "CreateVolume" {
			creates++
		}
	}
	if creates != 4 {
		t.Fatalf("CreateVolume calls %d, want 4: three creates and one resume", creates)
	}
	again, err := next.Submit(context.Background(), token(t, "vol-c"), labeled(t, "data-c", nil))
	if err != nil || again.ID != pending.ID || again.Phase() != model.PhaseSucceeded {
		t.Fatalf("replay %+v %v, want %s succeeded", again, err, pending.ID)
	}
}

// records counts the live ConfigMaps of one record kind.
func records(t *testing.T, ns, kind string) int {
	t.Helper()
	list, err := kc.CoreV1().ConfigMaps(ns).List(context.Background(), metav1.ListOptions{
		LabelSelector: "vws.io/record=" + kind + ",vws.io/tombstone!=true",
	})
	if err != nil {
		t.Fatal(err)
	}
	return len(list.Items)
}

// The same token and request return the same operation and leave one record;
// the same token with another request is ErrTokenReused, as on SQLite (#141 AC 4).
func TestKubeStoreTokensAreIdempotent(t *testing.T) {
	cfg, ns := kubeStore(t)
	svc, _ := startService(t, kubeOpts(cfg, fake.NewBlock(), fake.NewObjects(fake.ObjectOptions{})))
	ctx := context.Background()

	vreq := labeled(t, "data-a", map[string]string{"team": "db"})
	first := submitDone(t, svc, "vol", vreq)
	second, err := svc.Submit(ctx, token(t, "vol"), vreq)
	if err != nil || second.ID != first.ID || second.Target() != first.Target() {
		t.Fatalf("volume replay %+v %v, want %s", second, err, first.ID)
	}
	other := vreq
	other.Spec.Size = 2 * model.GiB
	if _, err := svc.Submit(ctx, token(t, "vol"), other); !errors.Is(err, model.ErrTokenReused) {
		t.Fatalf("volume reuse %v", err)
	}

	sreq := model.CreateSnapshotRequest{Source: first.Target().(model.VolumeID), Description: "nightly"}
	snap := submitDone(t, svc, "snap", sreq)
	again, err := svc.Submit(ctx, token(t, "snap"), sreq)
	if err != nil || again.ID != snap.ID || again.Target() != snap.Target() {
		t.Fatalf("snapshot replay %+v %v, want %s", again, err, snap.ID)
	}
	sreq.Description = "weekly"
	if _, err := svc.Submit(ctx, token(t, "snap"), sreq); !errors.Is(err, model.ErrTokenReused) {
		t.Fatalf("snapshot reuse %v", err)
	}

	for kind, want := range map[string]int{"volume": 1, "snapshot": 1, "operation": 2, "token": 2} {
		if got := records(t, ns, kind); got != want {
			t.Errorf("%s ConfigMaps %d, want %d", kind, got, want)
		}
	}
}
