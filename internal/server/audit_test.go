package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc/metadata"

	storagev1 "github.com/popododo0720/vws-proto/gen/go/vws/storage/v1"

	"github.com/popododo0720/vws-lib/obs"
	"github.com/popododo0720/vws-lib/obs/grpcobs"

	"github.com/popododo0720/vws-storage/internal/server"
)

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n, err := b.buf.Write(p)
	if err != nil {
		return n, fmt.Errorf("write log: %w", err)
	}
	return n, nil
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *lockedBuffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.Reset()
}

type auditRecord struct {
	Audit        bool   `json:"audit"`
	Actor        string `json:"actor"`
	Action       string `json:"action"`
	ResourceType string `json:"resource_type"`
	ResourceID   string `json:"resource_id"`
	Outcome      string `json:"outcome"`
	ErrCode      string `json:"err_code"`
	RequestID    string `json:"request_id"`
}

func lastAudit(t *testing.T, logs *lockedBuffer) auditRecord {
	t.Helper()
	var out auditRecord
	for _, line := range strings.Split(logs.String(), "\n") {
		var rec auditRecord
		if err := json.Unmarshal([]byte(line), &rec); err == nil && rec.Audit {
			out = rec
		}
	}
	if !out.Audit {
		t.Fatalf("no audit record in %q", logs.String())
	}
	return out
}

func TestMutatingRPCWritesAnAuditRecord(t *testing.T) {
	// The service logger from obs.Setup adds request_id from the context; the
	// gRPC interceptor puts the caller's x-request-id there.
	logs := &lockedBuffer{}
	old := slog.Default()
	shutdown, err := obs.Setup(context.Background(), obs.ForService(server.Service),
		obs.WithOverride(func(c *obs.Config) {
			c.HealthAddr = ""
			c.LogOutput = logs
		}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = shutdown(context.Background())
		slog.SetDefault(old)
	})

	vols, _, _, _, _ := dial(t, "")
	const incomingID = "audit-req-0001"
	ctx := metadata.AppendToOutgoingContext(context.Background(), grpcobs.RequestIDHeader, incomingID)
	req := &storagev1.CreateVolumeRequest{
		Parent:    "projects/local/zones/az-1",
		VolumeId:  "audit-1",
		RequestId: "22222222-2222-4222-8222-222222222222",
		Volume: &storagev1.Volume{Spec: &storagev1.VolumeSpec{
			SizeGib: 1,
			Type:    storagev1.VolumeType_VOLUME_TYPE_GP2,
		}},
	}
	if _, err := vols.CreateVolume(ctx, req); err != nil {
		t.Fatal(err)
	}
	got := lastAudit(t, logs)
	want := auditRecord{
		Audit: true, Actor: "local", Action: "create_volume", ResourceType: "volume",
		ResourceID: "projects/local/volumes/audit-1", Outcome: "ok", RequestID: incomingID,
	}
	if got != want {
		t.Fatalf("audit = %+v, want %+v", got, want)
	}

	logs.Reset()
	req.RequestId = "not-a-token"
	if _, err := vols.CreateVolume(ctx, req); err == nil {
		t.Fatal("bad request id accepted")
	}
	got = lastAudit(t, logs)
	if got.Outcome != "error" || got.ErrCode != "InvalidArgument" || got.Action != "create_volume" || got.RequestID != incomingID {
		t.Fatalf("failed audit = %+v", got)
	}

	logs.Reset()
	if _, err := vols.GetVolume(ctx, &storagev1.GetVolumeRequest{
		Name: "projects/local/zones/az-1/volumes/audit-1",
	}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs.String(), `"audit":true`) {
		t.Fatalf("read wrote an audit record: %q", logs.String())
	}
}
