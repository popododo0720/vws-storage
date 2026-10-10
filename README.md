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
| `-db` | empty | SQLite file. Empty uses memory, which disappears on exit |
| `-zones` | `az-1` | Comma-separated zone names |
| `-fake-object-addr` | `127.0.0.1:7401` | HTTP address of the fake object backend |

One process may open a given SQLite file.

## Operations

Mutations return an operation. Poll `GetOperation` until `done` is set. A client token names the request:

- The same token and the same request return the same operation.
- The same token and a different request returns `ALREADY_EXISTS` with `ErrorInfo.reason` `CLIENT_TOKEN_REUSED`.

`Run` is the only driver. After a restart it scans the same runnable operations and continues them. A failed operation records the error. The volume, snapshot, or bucket does not gain a failed state. An object row exists only after its bytes are committed.

## Proto

`proto/vws/storage/v1/storage.proto` lives in this repository for now.

TODO(vws): move to the shared VWS proto repository; keep package `vws.storage.v1` stable so the move only changes `go_package`.

Generated Go is committed under `internal/gen`.

## Test

```sh
go test -race -count=1 ./...
```
