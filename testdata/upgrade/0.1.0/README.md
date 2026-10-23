# Upgrade fixtures: 0.1.0

`storage.db` was written by the 0.1.0 build: source commit `0c874bc`
(the build of the `ghcr.io/popododo0720/vws/storage:0.1.0` image that ran on
vws-test before `dac2dd2`; the repo has no git tag for it). SQLite schema
`user_version` 2.

It holds three volumes (`standard` 8 GiB with a label, `ssd` 16 GiB, and
`standard` 2000 GiB, over the later 1024 GiB limit, for #126), one snapshot
of the first volume, the four create operations, and their four create tokens
(request_id `11111111-...` to `44444444-...`).

Command (run from the repository root of this branch, Go 1.27, fake backends):

```sh
git worktree add --detach /tmp/vws-storage-0.1.0 0c874bc
(cd /tmp/vws-storage-0.1.0 && go build -o /tmp/vws-storage-0.1.0.bin ./cmd/vws-storage)
dir=$(mktemp -d)
/tmp/vws-storage-0.1.0.bin -db="$dir/storage.db" -listen=127.0.0.1:17400 -fake-object-addr=127.0.0.1:17401 &
pid=$!
sleep 2
(cd /tmp/vws-storage-0.1.0 && VWS_FIXTURE_ADDR=127.0.0.1:17400 go test -count=1 "$OLDPWD/testdata/upgrade/0.1.0/gen/gen_test.go")
kill -TERM "$pid"; wait "$pid"
cp "$dir/storage.db" testdata/upgrade/0.1.0/storage.db
```

`gen/gen_test.go` is only a gRPC client; every stored byte comes from the 0.1.0
server. Regenerate these files only at a release bump.

Tests that read them: `TestUpgradeFrom010RecordsRoundTrip`
(`internal/storage/internal/store/sqlite`), `TestUpgradeFrom010DecodesStoredRecords`
and `TestUpgradeFrom010ReplaysStoredTokens` (`internal/server`).

sha256 of `storage.db`: `2cda1621962d4d475c5c097fb38c0a2d50fb9e258792ce42d40f8c8c7768b452`
