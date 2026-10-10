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
| `-fake-object-addr` | `127.0.0.1:7401` | HTTP address of the fake object backend |

One process may open a given SQLite file.

## Operations

Mutations return a long-running operation. Poll `GetOperation` until `done` is set. `request_id` is a UUID:

- The same id and the same request return the same operation.
- The same id and a different request returns `ABORTED` with `ErrorInfo.reason` `IDEMPOTENCY_CONFLICT`.

`Run` is the only driver. After a restart it scans the same runnable operations and continues them. A failed operation records the error. The volume, snapshot, or bucket does not gain a failed state. An object row exists only after its bytes are committed.

## Proto

Storage RPCs come from `github.com/popododo0720/vws-proto`. The module is private, so `go.mod` replaces it with `./third_party/vws-proto` (snapshot of main `a1c6084`).

vws-proto가 공개되면 replace를 제거하고 태그 버전을 require한다.

The process serves project `local` and region `local`. Volume zones come from `-zones`.

## Test

```sh
go test -race -count=1 ./...
```
