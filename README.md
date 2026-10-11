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

One process may open a given SQLite file.

## Operations

Mutations return a long-running operation. Poll `GetOperation` until `done` is set. `request_id` is a UUID:

- The same id and the same request return the same operation.
- The same id and a different request returns `ABORTED` with `ErrorInfo.reason` `IDEMPOTENCY_CONFLICT`.

`Run` is the only driver. After a restart it scans the same runnable operations and continues them. A failed operation records the error. The volume, snapshot, or bucket does not gain a failed state. An object row exists only after its bytes are committed.

## Object payload contract (WriteObject, ReadObject)

For `vws-console-api` and other callers. The authority is `third_party/vws-proto/vws/storage/v1/object.proto`; this section states what this service does with it. `name` is `projects/{project}/buckets/{bucket}/objects/{object}`, with the key percent-encoded.

### Limits

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
