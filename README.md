# vws-storage

Control plane for VWS block volumes, snapshots, and S3-compatible buckets and objects. Object bytes go to the object backend. The service stores desired state only.

The block and object backends are interfaces. This repository ships in-memory fakes, not a Ceph client. `Open` does not start work. Call `Run` to drive operations, including work left unfinished by a previous process.

## Run

```sh
go run ./cmd/vws-storage
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `-listen` | `127.0.0.1:7400` | gRPC address |
| `-db` | empty | SQLite file. A relative path is resolved and its parent directory is created. Empty uses memory, which disappears on exit |
| `-zones` | `vws-1a` | Comma-separated zone names |
| `-projects` | empty | Comma-separated project ids. Empty accepts any valid project id |
| `-fake-object-addr` | `127.0.0.1:7401` | HTTP address of the fake object backend |
| `-object-backend` | `fake` | `fake` or `s3` |
| `-s3-profile` | `rgw` | `rgw` or `minio`. Both default to path-style addressing and region `us-east-1` |
| `-s3-endpoint` | empty | S3 endpoint URL for `s3` |
| `-s3-region` | profile default | S3 region |
| `-s3-bucket-prefix` | empty | prefix for every S3 bucket name: `<prefix>-vws-<40 hex>`. Lowercase letters, digits and `-`, starting with a letter or digit, at most 18 characters so the name fits 63. An invalid prefix fails at startup. API bucket names never carry it |
| `-block-backend` | `fake` | `fake` or `kube` |
| `-kube-config` | empty | kubeconfig path for `kube`. Empty uses in-cluster credentials |
| `-kube-namespace` | `vws-vms` | namespace for claims and snapshots. It is the VM namespace, because KubeVirt hot-plugs only a claim in the VM's namespace |
| `-kube-class-standard` | `ceph-block` | StorageClass for `standard` volumes |
| `-kube-class-ssd` | `ceph-block` | StorageClass for `ssd` volumes |
| `-kube-snapshot-class` | `ceph-block` | VolumeSnapshotClass for snapshots. Empty disables snapshots (`CreateSnapshot` is `INVALID_ARGUMENT`) |
| `-kube-volume-mode` | `Block` | `volumeMode` of new empty volumes. `Filesystem` for a provisioner that serves only Filesystem, such as NFS. It needs CDI |
| `-kube-dv-retries` | `3` | Filesystem mode: times a `Failed` CDI DataVolume is deleted and made again. `0` gives up on the first `Failed` |
| `-kube-dv-timeout` | `10m` | Filesystem mode: time from a volume's first DataVolume to `Succeeded`, across retries |

S3 keys come from `VWS_S3_ACCESS_KEY` and `VWS_S3_SECRET_KEY`, never from flags.

One process may open a given SQLite file.

## Kubernetes block backend

`internal/backend/kube` serves volumes as PersistentVolumeClaims. `cmd/vws-storage -block-backend=kube` selects it. It implements all of `backend.Block`. Object bytes stay on the fake backend.

- `kube.Config` holds the kubeconfig path (empty means in-cluster), the namespace, the class table, and the `VolumeSnapshotClass`. `kube.Open` builds the backend and does not call the API server. Defaults are namespace `vws-vms` (the VM namespace, so KubeVirt can hot-plug the claim), StorageClass `ceph-block`, and snapshot class `ceph-block`. They are the names the Rook bundle in vws-deploy creates (provisioner `rook-ceph.rbd.csi.ceph.com`). A class that does not exist leaves the claim Pending.
- `kube.Classes` maps a volume type to a StorageClass name. `DefaultClasses` maps `standard` and `ssd` to `ceph-block`. A type with no entry is `INVALID_ARGUMENT` on the operation and creates no claim.
- The claim name is derived from the full volume name, so a repeated `CreateVolume` is one claim. A repeat with another size, type, or zone is `ErrConflict`.
- `Expand` raises the claim request. A smaller size is `ErrInvalid`. The API server accepts a resize only on a bound claim whose class has `allowVolumeExpansion: true`. `Expand` is on `backend.Block`. `UpdateVolume` with mask `spec` or `spec.size_gib` submits an `ExpandVolumeRequest`. A smaller size is `INVALID_ARGUMENT` and the same size is a succeeded operation. The row keeps its old size until the backend call succeeds, then takes the new size and a new etag in the write that succeeds the operation.
- Image sources are `ErrInvalid` here. Compute images belong to CDI.
- A snapshot is one `VolumeSnapshot`. `CreateSnapshot` returns `ErrBusy` until `status.readyToUse` is true, so the engine retries and the snapshot row turns Available only when it is usable. A `status.error` is retried as `ErrBusy` until this backend has seen it for 10 minutes, then fails the operation permanently, because the snapshotter clears the error itself when a retry works. The first sighting is the `vws.popododo0720.io/error-first-seen` annotation on the VolumeSnapshot, removed when the error clears. `status.restoreSize` is read for a restore: a volume smaller than it is `ErrInvalid`. The `VolumeSnapshotClass` name comes from `kube.Config`.
- A volume cannot be cloned from another volume. `VolumeSpec.Source` is blank, a compute image, or a snapshot, so a copy goes through a snapshot. PR #13 and issue #9 say "clone" for that snapshot restore.
- A new claim has `volumeMode: Block`. KubeVirt attaches a Block claim as the disk itself; a Filesystem claim must hold a `disk.img` file, so hot-plugging an empty Filesystem claim fails. A `VolumeSnapshot` records its source claim's mode in `vws.popododo0720.io/source-volume-mode`. A restore uses that mode, and a snapshot without the annotation restores as Filesystem. `volumeMode` is immutable, so claims created before this stay Filesystem.
- A volume from a snapshot is a PVC with `dataSource` set to that `VolumeSnapshot`. A snapshot that is absent is `ErrNotFound`. One that is not ready is `ErrBusy`, and no claim is created yet.
- `VolumeStatus.instance` is the instance whose KubeVirt `VirtualMachine` (label `vws.io/managed=true`, in the claim namespace) lists the claim in `spec.template.spec.volumes`, as boot disk or hot-plugged. It is read on every Get and List (one VM list per call) and never stored, because the instance spec owns the attachment. The name is `projects/{volume project}/zones/{volume zone}/instances/{VM name}`. The service account needs `list` on `virtualmachines.kubevirt.io` in that namespace. Without it, or with any lookup error, the field is empty and the error is logged.
- `VolumeStatus.claim_name` and `SnapshotStatus.snapshot_object_name` are the PVC and VolumeSnapshot names, from `Block.ClaimName` and `Block.SnapshotObjectName`. They are a pure function of the id, so nothing is stored, they never change, and a volume created before the field existed reports it too. They are empty while the row is Creating. The fake backend returns a made-up `fake-<id>`. `vws-compute` uses the value to find the claim and does not recompute the name.
- Deleting a volume does not delete its snapshots. The engine admits the delete while a snapshot is Available, and the snapshot can still restore a volume. A snapshot being created blocks the delete. `TestVolumeDeleteKeepsItsAvailableSnapshot` and `TestDeletingSourceVolumeKeepsSnapshot` pin this. A snapshot that is a volume's source cannot be deleted.
- With `-kube-volume-mode=Filesystem` (NFS), a new empty volume is a CDI `DataVolume` with `source: blank: {}`, named after the claim. CDI writes a sparse `disk.img` owned by uid 107 and sizes the claim for the filesystem overhead. KubeVirt creates a missing `disk.img` only when a VM starts, not on hotplug, so a claim without the image cannot be hot-plugged. `CreateVolume` is `ErrBusy` until the DataVolume phase is `Succeeded`. `Failed` is final for a DataVolume (CDI has already retried its importer pod, and its webhook rejects spec changes), so the backend deletes the DataVolume and its claim and makes a new one, up to `-kube-dv-retries` times. The attempt number and the time of the first DataVolume ride on the new one as `vws.popododo0720.io/dv-attempt` and `vws.popododo0720.io/dv-first-created`. Past the retries, or `-kube-dv-timeout` after the first DataVolume in any phase, `CreateVolume` fails permanently: the operation error (`DEADLINE_EXCEEDED`) carries the reason, and the volume stays in error (see below) with the last DataVolume and claim until `DeleteVolume`. While a Failed DataVolume is still terminating, the count is held in memory, so a process restart in that gap starts the count and the clock again. The DataVolume has `cdi.kubevirt.io/storage.deleteAfterCompletion: "false"` because it owns the claim. `DeleteVolume` deletes the DataVolume and the claim. A restore from a snapshot is still a plain PVC, because the snapshot already holds `disk.img`. `Expand` grows the claim. CDI requests more than the volume size for the filesystem overhead, so `Expand` compares the new size with the volume size (the DataVolume request, then the `vws.popododo0720.io/size` annotation it writes) and keeps the overhead on top. `disk.img` grows only with KubeVirt's `ExpandDisks` feature gate. The service account needs `create`, `get`, `list` and `delete` on `datavolumes.cdi.kubevirt.io` in the namespace.
- The tests install the external-snapshotter `VolumeSnapshot` CRD from `testdata` (v8.2.0).

The tests run against envtest. Set `KUBEBUILDER_ASSETS` (`setup-envtest use -p path`). Without it the package skips.

## S3 object backend

`internal/backend/s3` implements `backend.Object` and `backend.Signer` for Ceph RGW and MinIO with one driver. `cmd/vws-storage -object-backend=s3` selects it. The contract is the existing one. Bytes move by presigned URL. The driver creates and deletes buckets, `Stat`s a generation, and deletes a generation. Listing, `request_id` replay, and the md5 the API reports stay in the engine and store.

- A VWS bucket maps to the S3 bucket `vws-` plus a hash of its full name. An object generation is the key `<generation>/<key>`.
- `PresignPut` signs `Content-Length`, `If-None-Match: *`, and `x-amz-checksum-sha256` when a digest is given. The locator carries these headers and the client must send them. The expiry is rounded down to whole seconds.
- `Stat` reads the MD5 from the ETag. The ETag is the payload MD5 only for a single-part upload without server-side encryption; otherwise MD5 is zero. `Stat` always reads the stored bytes back, pinned to the ETag with If-Match, and computes the SHA-256 itself. It neither requests nor uses a checksum the endpoint reports, because Ceph RGW keeps a wrong `x-amz-checksum-sha256` as sent and reports it back. On the same read a single-part MD5 ETag is checked, and a mismatch is a permanent error. The record keeps the computed digest, never a client's or the endpoint's claim.
- `CreateBucket`, `DeleteBucket`, and `DeleteObject` are safe to repeat. A bucket that still holds objects is `ErrBusy`.
- Tests use an in-process S3 server (gofakes3). `TestLiveEndpoint` runs against a real endpoint when `VWS_S3_TEST_ENDPOINT`, `VWS_S3_TEST_PROFILE`, and the two key variables are set, and skips otherwise.

## Health

The server answers the standard `grpc.health.v1` service on the gRPC listener.

| Service name | Answer |
| --- | --- |
| `""` (empty) | liveness: SERVING while the process runs, no dependency checked, NOT_SERVING once shutdown starts |
| `vws.storage.v1.VolumeService`, `vws.storage.v1.SnapshotService` | readiness: engine running, store answers, and with `-block-backend=kube` the cluster answers and holds the namespace, the StorageClass of each volume type and the VolumeSnapshotClass |
| `vws.storage.v1.BucketService`, `vws.storage.v1.ObjectService`, `google.longrunning.Operations` | readiness: engine running and store answers |
| `vws.storage.v1.Ready` | SERVING only when every service above is. For a probe that names one service |

A background loop evaluates the checks (`-health-period`, default 10s, sooner while something is down; `-health-timeout`, default 3s). `Check` and `Watch` answer from memory. S3 is not checked: an S3 outage must not take the volume services out of rotation. The kube checks read the Namespace, StorageClass and VolumeSnapshotClass, which are cluster-scoped, so the pod's service account needs `get` on them.

### Storage preflight

vws-storage does not install or operate storage. The StorageClasses, the CSI driver behind them, the VolumeSnapshotClass (if one is set) and the storage itself (a Ceph cluster, or an NFS export reachable from every node) must exist before it starts. The kube readiness check is the preflight, both at startup and on every period:

- Each StorageClass must exist. Its provisioner is logged once at startup (`storage class` with `class` and `provisioner`).
- A provisioner that serves only Filesystem (`nfs.csi.k8s.io`, `smb.csi.k8s.io`) with `-kube-volume-mode=Block` is not ready.
- A VolumeSnapshotClass that is set must exist, and its `driver` must be the provisioner of one of the StorageClasses.
- In Filesystem mode, the CDI DataVolume API must answer in the namespace.

Until all of these hold, the volume and snapshot services are NOT_SERVING.

### External storage (generic CSI and NFS)

Any CSI driver that provisions dynamically works through the StorageClass flags. NFS uses [csi-driver-nfs](https://github.com/kubernetes-csi/csi-driver-nfs) with a StorageClass such as:

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata: {name: nfs-csi}
provisioner: nfs.csi.k8s.io
parameters: {server: nas.example, share: /export/vws}
allowVolumeExpansion: true
```

Then run with `-kube-class-standard=nfs-csi -kube-class-ssd=nfs-csi -kube-volume-mode=Filesystem`. `-kube-snapshot-class` is the csi-driver-nfs VolumeSnapshotClass, or empty for no snapshots.

iSCSI has no option of its own. The kubernetes-csi `csi-driver-iscsi` only attaches LUNs that already exist and cannot provision them. An iSCSI SAN whose vendor CSI driver provisions dynamically is a generic CSI StorageClass, in Block mode.

### HTTP probes

When `VWS_HEALTH_ADDR` is set, the process also serves HTTP probes on that address (vws-lib `obs.Setup`):

| Path | Answer |
| --- | --- |
| `/healthz` | 200 while the process runs; checks nothing |
| `/readyz` | 200 when `vws.storage.v1.Ready` is SERVING (the same cached verdict as above), 503 otherwise and once shutdown or SIGTERM starts. Telemetry backends (OTLP collector, Sentry) are never checked |

### Environment

| Variable | Effect |
| --- | --- |
| `VWS_HEALTH_ADDR` | HTTP probe address, e.g. `0.0.0.0:8086`. Empty serves no HTTP probes |
| `VWS_LOG_LEVEL`, `VWS_LOG_FORMAT` | slog level (default `info`) and format (`json` or `text`, default `json`) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP endpoint for traces, metrics and logs. Empty exports nothing |
| `OTEL_SERVICE_NAME`, `OTEL_SDK_DISABLED`, `OTEL_EXPORTER_OTLP_PROTOCOL`, `OTEL_TRACES_SAMPLER(_ARG)` | standard OpenTelemetry settings |
| `SENTRY_DSN`, `SENTRY_ENVIRONMENT`, `SENTRY_RELEASE`, `SENTRY_SAMPLE_RATE` | error reports to Sentry when the DSN is set |
| `VWS_S3_ACCESS_KEY`, `VWS_S3_SECRET_KEY` | S3 keys for `-object-backend=s3` |

## Operations

Mutations return a long-running operation. Poll `GetOperation` until `done` is set. `request_id` is a UUID:

- The same id and the same request return the same operation.
- The same id and a different request returns `ABORTED` with `ErrorInfo.reason` `IDEMPOTENCY_CONFLICT`.

`Run` is the only driver. After a restart it scans the same runnable operations and continues them. A failed operation records the error. The volume, snapshot, or bucket does not gain a failed state. An object row exists only after its bytes are committed.

## Object payload contract (WriteObject, ReadObject)

For `vws-console-api` and other callers. The authority is `third_party/vws-proto/vws/storage/v1/object.proto`; this section states what this service does with it. `name` is `projects/{project}/buckets/{bucket}/objects/{object}`, with the key percent-encoded.

### Limits

- A volume whose create fails permanently in the backend (a retry limit or timeout, a conflict, an invalid argument) is kept in error instead of being undone. GetVolume and ListVolumes show it with `status.observed.state = RECONCILIATION_STATE_ERROR` and the reason in `status.observed.message`, the same text as the create operation's error. It has no `claim_name`, so it cannot be attached. Snapshot, expand and a create replay of the same id are `FAILED_PRECONDITION`. DeleteVolume removes it and whatever the backend left; a failed delete returns it to error with the same reason. The state and reason are in the store and survive a restart. The SQLite store is at schema version 2 for this; a version 1 file is upgraded at Open, and an older binary refuses a version 2 file.
- CreateObject is a strict create, so S3 `If-None-Match: *` maps to it. A live object at the path, or another create of the path still in flight, is `ALREADY_EXISTS`, even with identical bytes. Only the same `request_id` with the same request replays the first operation. UpdateObject with `allow_missing` is the upsert.
- Payload maximum is **4194304 bytes** (4 MiB, decoded) on CreateObject, UpdateObject, and WriteObject. 4194304 is accepted. 4194305 is `INVALID_ARGUMENT` / `INVALID_SPEC` and stores nothing.
- The server accepts messages up to 8 MiB (`server.MaxRecvMsgSize`). A larger message is rejected by gRPC itself as `RESOURCE_EXHAUSTED` before this service sees it.
- gRPC clients default to a 4 MiB receive limit. A client that calls ReadObject on a payload at the limit must raise it (Go: `grpc.MaxCallRecvMsgSize(8<<20)`), or the read fails with `RESOURCE_EXHAUSTED` on the client.

### WriteObject

`WriteObjectRequest` returns a long-running operation. Poll `GetOperation` until `done`.

| Field | Meaning |
| --- | --- |
| `name` | Required. The object must exist. WriteObject never creates. |
| `contents` | Required, `optional bytes`. Unset is `INVALID_SPEC`. Set and empty stores a zero-length object. Full replacement, no range. |
| `sha256` | Optional lowercase hex SHA-256 of `contents`. Empty means the server computes it. A value that does not match is `INVALID_SPEC` and stores nothing. |
| `etag` | Optional. Must equal the stored `Object.etag`. Otherwise `ABORTED` / `ETAG_MISMATCH`. |
| `request_id` | Optional UUID. See idempotency below. |
| `validate_only` | Checks the request and stores nothing. Returns a done operation. |

The write replaces the payload only. Content type and labels stay as stored. A changed payload raises `generation`, changes `Object.etag`, and sets `spec.sha256`, `status.md5`, and `status.size_bytes` to the new bytes. Writing bytes identical to the stored bytes succeeds without a new generation, and `etag`, `md5`, and `sha256` stay as stored.

Errors, in the order they are checked:

| Condition | gRPC code | `ErrorInfo.reason` |
| --- | --- | --- |
| Same `request_id`, different request | `ABORTED` | `IDEMPOTENCY_CONFLICT` |
| Bad name, unset or oversize `contents`, bad or mismatched `sha256`, bad `etag` text | `INVALID_ARGUMENT` | `INVALID_SPEC` |
| Object or bucket missing | `NOT_FOUND` | `RESOURCE_NOT_FOUND` |
| Object has an operation in flight, or is being deleted | `FAILED_PRECONDITION` | `DEPENDENCY_NOT_READY` |
| `etag` differs from stored | `ABORTED` | `ETAG_MISMATCH` |

### ReadObject

`ReadObjectRequest{name, etag}` returns `ReadObjectResponse{contents, object}` with no side effects. `contents` is the whole stored payload (empty for a zero-length object). `object` is the record the bytes belong to, including `etag`, `spec.sha256`, and `status.md5`. When `etag` is set and differs from the record that would be returned, the call is `ABORTED` / `ETAG_MISMATCH` with no body. A missing object is `NOT_FOUND` / `RESOURCE_NOT_FOUND`. `GetObject` and `ListObjects` never return the payload.

A read that races a committed write returns either the old record or the new one, never a mix. The server re-reads the record up to three times when the head moves during the byte fetch, then returns `UNAVAILABLE`.

### md5 and etag

- `status.md5` is the lowercase hex MD5 of the stored payload, 32 characters, no quotes. It is a content digest. Do not use it as a concurrency token.
- `spec.sha256` is the lowercase hex SHA-256 of the stored payload.
- `Object.etag` is the concurrency token. It is not an MD5 and not the SHA-256. It changes when the payload, content type, or labels change. Send it back as `etag` on WriteObject, ReadObject, UpdateObject, and DeleteObject to guard against a concurrent change.
- The md5 comes from the object backend's observation of the committed bytes and is stored with the object row. It is set when the upload commits, which `Run` does after the operation is created. Poll the operation to `done` before relying on `status.md5`, `etag`, or `ReadObject` of the new bytes. An object row written before md5 was recorded has an empty `status.md5`.

### request_id idempotency

- `request_id` is a UUID, scoped to the project. The key is the id plus a fingerprint of the request.
- The fingerprint of WriteObject is the object name, payload size and SHA-256 (so the payload), and `etag`. `validate_only` is never recorded.
- Same id and same fingerprint returns the operation that was created first, even after the object has since changed and even if the `etag` is now stale. Nothing is written again. Replay is checked before existence and the `etag` compare, so a lost response retries onto the committed operation.
- Same id and a different fingerprint is `ABORTED` / `IDEMPOTENCY_CONFLICT` and writes nothing.
- A rejected request records no id.

## Proto

Storage RPCs come from `github.com/popododo0720/vws-proto`. The module is private, so `go.mod` replaces it with `./third_party/vws-proto` (snapshot of main `2d8a7e9`).

vws-proto가 공개되면 replace를 제거하고 태그 버전을 require한다.

Project comes from each request name. `-projects` restricts which projects this process serves; empty accepts any valid id. The same volume id under two projects is two rows. Bucket and snapshot regions come from the request and are stored. Volume zones come from `-zones`.

## Test

```sh
go test -race -count=1 ./...
```
