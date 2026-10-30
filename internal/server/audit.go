package server

import (
	"context"
	"log/slog"

	"google.golang.org/grpc/status"

	"github.com/popododo0720/vws-lib/obs"

	"github.com/popododo0720/vws-storage/internal/model"
)

// Audit field names from the design's audit record. The lib owns the shared
// fields; these three are only this service's audit shape.
const (
	fieldAuditActor   = "actor"
	fieldAuditAction  = "action"
	fieldAuditOutcome = "outcome"
)

type auditEvent struct {
	action string
	kind   string
	id     string
}

// auditOf names the mutation and the resource it targets. Every mutating RPC
// reaches it through submit or submitUpload.
func auditOf(req model.Request) auditEvent {
	switch r := req.(type) {
	case model.CreateVolumeRequest:
		return auditEvent{"create_volume", "volume", r.ID.String()}
	case model.DeleteVolumeRequest:
		return auditEvent{"delete_volume", "volume", r.Volume.String()}
	case model.ExpandVolumeRequest:
		return auditEvent{"expand_volume", "volume", r.Volume.String()}
	case model.ModifyVolumeRequest:
		return auditEvent{"modify_volume", "volume", r.Volume.String()}
	case model.CreateSnapshotRequest:
		return auditEvent{"create_snapshot", "snapshot", r.ID.String()}
	case model.DeleteSnapshotRequest:
		return auditEvent{"delete_snapshot", "snapshot", r.Snapshot.String()}
	case model.CreateBucketRequest:
		return auditEvent{"create_bucket", "bucket", r.Bucket.String()}
	case model.DeleteBucketRequest:
		return auditEvent{"delete_bucket", "bucket", r.Bucket.String()}
	case model.PutObjectRequest:
		return auditEvent{"create_object", "object", r.Path.String()}
	case model.UpdateObjectRequest:
		return auditEvent{"update_object", "object", r.Path.String()}
	case model.WriteObjectRequest:
		return auditEvent{"write_object", "object", r.Path.String()}
	case model.DeleteObjectRequest:
		return auditEvent{"delete_object", "object", r.Path.String()}
	default:
		return auditEvent{"unknown", "unknown", ""}
	}
}

// audit writes one record per mutating request and counts it in
// vws.audit.events. It runs after the request is rendered, so err is the
// client-visible status. The actor is the request's project; no token, key or
// backend error text enters the record.
func (s *Server) audit(ctx context.Context, req model.Request, err error) {
	e := auditOf(req)
	outcome := "ok"
	code := ""
	if err != nil {
		outcome = "error"
		code = status.Code(err).String()
	}
	attrs := []slog.Attr{
		slog.Bool(obs.FieldAudit, true),
		slog.String(fieldAuditAction, e.action),
		slog.String(obs.FieldResourceType, e.kind),
		slog.String(obs.FieldResourceID, e.id),
		slog.String(fieldAuditOutcome, outcome),
		slog.String(obs.FieldErrCode, code),
	}
	if actor := req.Project().String(); actor != "" {
		attrs = append(attrs, slog.String(fieldAuditActor, actor))
	}
	slog.LogAttrs(ctx, slog.LevelInfo, "audit", attrs...)
	obs.M().Audit(ctx, Service, e.action, outcome)
}
