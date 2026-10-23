# AGENTS.md

VWS storage is the control plane for EBS volumes/snapshots and S3-compatible buckets/objects. Object bytes go to the backend; the desired state stays only in the store. The backend is a fake behind an interface.

## pstack

Work with pstack.

- **architect**: lay out two designs with different structures and pick one.
- **interrogate**: two reviewers look at the implementation. Review results are not applied automatically.
- **correct**: when the same mistake repeats, block it with types, package boundaries, lint or tests before documentation.

All coding and development follows the pstack poteto-mode style. Keep responses and code concise and simple, and report only what was actually run and checked. Subagents use high effort. Do not use Cursor cloud agents for VWS work, except when the user asks for it directly.

## Branches and commits

- Do not push to main directly. Work on a branch.
- Commits are atomic. One commit is one change, and the build holds with that commit alone.

## Shape

There is one write path, `storage.Service.Submit`. `Submit` applies only a `model.Change` inside `store.Update`. A `Change`'s list of records is unexported, so it cannot be built outside the transition functions. There is one progress path, `storage.Service.Run`. `Open` does not start `Run`. Work left over after a restart is picked up by the same `Run` scan. The server parses requests and calls only `Service`. Long work is an `Operation`, keyed by the client token.

Do not add a second write path. Do not update the store from the server or a backend. Failures live in the `Operation`. The one exception is a volume whose creation failed permanently: that volume stays `Failed` (ERROR) with a reason (#83).

## Rules

A rule whose enforcer cell is `Judgment` has no check; the author and reviewer uphold it. Every other enforcer cell is a test, lint or CI step that fails when the rule breaks. `internal/archtest` `TestAgentsRuleTable` checks the table format and the test names in the enforcer cells, and leaves the number of `Judgment` rows in the CI log.

| Rule | Enforcer |
| --- | --- |
| Do not push to main directly. Work on a branch | `.github/workflows/direct-push-watch.yml` (detected after the fact) |
| Commits are atomic. One commit is one change, and the build holds with that commit alone | Judgment |
| Work with pstack (architect, interrogate, correct) | Judgment |
| The desired state changes only through `model.Change` | `Change` records are unexported. Transitions are in `internal/model` |
| The store implementations live only inside the engine | the Go `internal` directory `internal/storage/internal/store`. The store sees only the store contract and the model: `TestArchitectureRules` |
| Writes go through `Service.Submit` | server packages do not import the store. depguard `server`, `TestArchitectureRules` |
| Progress goes through `Service.Run` | `Run` refuses when it is already running. `internal/storage` |
| The model knows no transport, SQL or backend. It imports no package of this module | depguard `model`, `TestArchitectureRules` |
| The server imports only the engine and the model. It knows neither backends nor the store | depguard `server`, `TestArchitectureRules` |
| The engine knows no gRPC, generated code or fake. The engine sees only the backend contract and does not import the kube, s3 or fake implementations or the server | depguard `engine`, `store`, `backend`, `TestArchitectureRules`. Test files are excluded |
| Backends import only the backend contract and the model. They know no engine, server or other backend | depguard `backend`, `TestArchitectureRules` |
| Health imports no package of this module. The process hands it the checks | `TestArchitectureRules` |
| External dependencies live only in their owner package: generated code and protobuf in the server, gRPC in the server, health and `cmd`, SQL in the SQLite store, Kubernetes in the kube backend and `cmd`, the AWS SDK in the s3 backend | `TestArchitectureRules` |
| Production code does not import `testing`. The exception is `storetest` | `TestArchitectureRules` |
| Logs go through `log/slog` and vws-lib `obs`. The standard `log` is not used. The one remaining exception is `internal/server/convert.go` `toStatus` (it has no context) | `knownDebt` in `TestArchitectureRules`. Removing that entry makes the test block it |
| This repo keeps no proto and no copy of it. The generated code is the `github.com/popododo0720/vws-proto` module at a release tag (`vX.Y.Z`, no pseudo-version) from the module proxy, with no `replace` | `internal/policycheck` (`TestRepoPolicy`, `TestProtoModuleRules`). Lint excludes `third_party` (the vws-lib copy) |
| No deployment manifests, logos or NOTICE | `internal/policycheck` |
| The same token with the same request is the same operation | `Service.Submit`, `TestCreateVolumeIsIdempotent` |
| The same token with a different request is a conflict | `ErrTokenReused` → gRPC `ABORTED` + `IDEMPOTENCY_CONFLICT` |
| Restart recovery is not a new path | `TestCreateVolumeResumesAfterCrash` |
| A rejected `Change` does not change the store | the memory `Update` applies to a copy, then swaps. `storetest` re-reads rows after `ErrStale` |
| A permanently failed volume creation leaves no `Creating` row. The volume stays `Failed` with a reason and is removed with DeleteVolume | `TestPermanentCreateFailureKeepsTheVolumeInError`, `TestVolumeInErrorRefusesWorkAndDeletes` |
| Resource names come back through `Parse(String())` | `ParseBucketName` strips `buckets/`. `TestResourceNamesRoundTrip` |
| One process per SQLite file | `BEGIN IMMEDIATE` and `locking_mode=EXCLUSIVE`. `TestSecondOpenFails` |
| The bytes of a generation are written once | the `Signer` contract. The fake `Accept` refuses a rewrite. `TestGenerationIsWrittenOnce` |
| Changes to the same object path do not overlap | uploads and deletes lock the path with a `Ref` and refuse when it is `InFlight` |
| An object row is deleted after the purge finishes | `Object.Delete` only guards the row. `Purged` deletes it only for the same generation |
| Listing objects of a missing bucket is not an empty list | `ListObjects` reads the bucket row first |
| The project is part of the storage key. The request's project separates rows | `VolumeID.String` is `projects/{project}/volumes/{id}`. `TestProjectScopedVolumeAndBucket` |
| Records from the previous release decode. Fixtures under `testdata/upgrade/<version>/` are regenerated only at a release bump | `TestUpgradeFrom010RecordsRoundTrip`, `TestUpgradeFrom010DecodesStoredRecords` |
| A project not on the allow list is not a resource of that process | `Server.allowProject` → `RESOURCE_NOT_FOUND`. `TestProjectAllowListRejected` |
| The engine stores only the image name. It does not look up, copy or size-check images. The kube backend clones from the CDI DataSource (#86) | `ImageName` is neither a `Key` nor a `Ref`. `TestImageSourceSurvivesCodec`, `TestCreateVolumeFromImage`, `TestImageVolumeClonesTheImageDataSource` |
| A volume clones an image only when the DataSource labels match the caller's project and region, or carry `vws.io/public=true` and the same region. Any other image answers exactly like a missing one (#89). | The server compares the image name's project with the request project (`RESOURCE_NOT_FOUND`). The kube backend checks the DataSource labels `vws.io/project`, `vws.io/region` and `vws.io/public` (`cloneAllowed`). `TestCreateVolumeFromAnotherProjectsImageIsNotFound`, `TestImageOfAnotherOwnerIsNotFound`, `TestCloneAllowed`, `TestPublicImageOfAnotherProjectIsCloned` |
| `Object.etag` is a concurrency token, not md5 | `status.md5` is `Object.MD5`; the etag is `Meta.Version`. `TestWriteObjectReplacesPayloadAndKeepsEtagSeparateFromMD5` |
| A WriteObject with the same `request_id` does not write again | the token lookup in `Service.Submit` comes before admit. `TestWriteObjectIsIdempotentOnRequestID` |
| Volumes only grow | `AvailableVolume.Expand` refuses a shrink. `TestExpandGrowsOnlyAndStoresSizeOnCompletion`, `TestUpdateVolumeExpandsAndRejectsShrink` |
| A volume's labels and type change with one admitted write and do not touch the backend object. The type changes only when the backend can leave it as is (kube: the same StorageClass, volumeMode and VolumeSnapshotClass) (#87, #123, #124) | `AvailableVolume.Modify`, `backend.Retyper`. `TestModifyVolumeSetsLabelsAndTypeAndSurvivesRestart`, `TestRetypeNeedsTheSameStorageClass`, `TestRetypeNeedsTheSameVolumeMode`, `TestRetypeNeedsTheSameSnapshotClass`, `TestUpdateVolumeSetsLabelsAndType` |
| `ssd` and `gp2` both read as `gp2`. For one release `gp2` is stored as `ssd`, so a pre-#123 build still reads it after a rollback. No batch job, idempotent, and a pre-upgrade create token still matches (#123) | `ParseVolumeType`, `wireName`, `fingerprintName`. `TestStoredSSDVolumeReadsAsGP2AndIsWrittenAsSSD`, `TestGP2OperationsAreStoredAsSSD`, `TestGP2CreateFingerprintMatchesThePreUpgradeSSDOne` |
| `CreateVolume` accepts the deprecated `VOLUME_TYPE_SSD` as `gp2` through the `gp2` mapping, and never answers with it (#123) | `internal/server/convert.go`. `TestDeprecatedSSDCreatesGP2`, `TestDeprecatedSSDUsesTheGP2Mapping` |
| A volume type with no storage mapped is refused before anything is written (#123) | `backend.TypeServer`, `admitCreateVolume`. `TestUnmappedVolumeTypeIsRefusedAndCreatesNothing`, `TestUnmappedTypeIsNotServed` |
| IOPS, throughput and size follow the EC2 per-type rules, including the IOPS-per-GiB and throughput-per-IOPS ratios; a violation is `INVALID_ARGUMENT` (#123, #124) | `checkPerformance`, `checkSize`. `TestCreateVolumePerformanceRules`, `TestPerformanceRatios`, `TestVolumePerformanceOutsideTheTypeRulesIsInvalid`, `TestVolumeSizePerType`, `TestExpandAndRetypeKeepTheTypeSizeRange` |
| A retype gets the EC2 `ModifyVolume` defaults for the new type (#124) | `retypePerformance`. `TestModifyRederivesPerformance`, `TestUpdateVolumeTypeRederivesPerformance` |
| A PR targets main and its body has `Part of #N` and no closing keyword; CI reports commits that reached main without a PR. AGENTS.md has Insider-risk guardrails 1-7 | `.github/workflows/repo-guard.yml`, `.github/workflows/direct-push-watch.yml`, `scripts/guard-agents.sh agents`, `scripts/guard-agents.sh selftest`. `ci.yml` runs both |
| The repo is English only (see "English only"). Hangul is allowed only in files `.github/english-allowlist` lists with a reason | `scripts/check-english.sh files` (CI `english` job) |

## English only

Write everything in this repo in English: README, code comments, docs, AGENTS.md, QUALITY.md, PR and issue templates, commit messages, PR and issue bodies, review comments and `Verified-by` sections. The `english` job in CI runs `scripts/check-english.sh`: `files` fails on a tracked file with Hangul unless `.github/english-allowlist` lists it as `<glob> <reason>` (only test data and user-facing UI strings belong there), and `text` and `commits` reject Hangul in the PR body and the PR's commit messages with no exception.

## Insider-risk guardrails

Bots are insider risks too. The rules below have no exceptions. The design is popododo0720/vws-deploy#33.

1. **Independent verification**: an important result (merge, security, data) is never verified by the bot/model that wrote it. Another bot (or a person) verifies it. This is a process rule; CI cannot enforce it.
2. **Records**: every change leaves a trail of issue → PR (`Part of #N`) → commits. Work logs go in the PR body and issue comments in a form people can read. Force-push and history rewriting are forbidden, except with the user's approval.
3. **Failure tests**: a new feature is not tested on the happy path only. Include at least one case each for power loss and restart, idempotent re-run, and malicious or boundary input, or state in the PR why that is not possible.
4. **Least privilege**: do not push to main directly. Every change is a single PR from its own branch cut from main. Stacked PRs and local merges are forbidden. Tokens are scoped to the repo. RBAC allows only the resources the service code actually calls, as a namespace-scoped Role where possible.
5. **Separated audit**: the author bot, the verifier bot and the merge decision are all different. An author bot never merges its own PR. Merge only when the base is main and every CI check on the head commit is success.
6. **Isolation and emergency stop**: a bot is stopped immediately at the user's request or by the orchestrator stopping that bot's session and revoking its token (`gh auth`, PAT revoke). Risky work happens only in a sandbox (the box, a throwaway cluster). No access to production clusters.
7. **Incident disclosure**: rule violations and incidents are never hidden; record them in an issue as `Cause / Impact / Action / Prevention`. The label is `incident`.

### vws-storage items

**Idempotency tests.** Expand, Restore and Delete must give the same result when the same request is sent again. These are the tests that exist now and what is missing.

| Target | Existing tests | Missing |
| --- | --- | --- |
| Expand | `TestExpandVolumeGrowsOnceAndRejectsShrink`, `TestExpandGrowsAndRejectsShrink` (kube) | A test that directly checks a resent Expand with the same client token |
| Restore (create a volume from a snapshot) | `TestRestoreUsesSnapshotDataSourceOnlyWhenReady`, `TestCreateVolumeIsIdempotent` | A test that directly checks a repeated restore request for the same snapshot |
| Delete | `TestDeleteVolumeIsIdempotent`, `TestDeleteSnapshotIsIdempotent`, `TestDeleteReplayAndRecreate`, `TestDeleteFailureRestoresVolume` | None |

**Malicious input tests.** Sizes and names are parsed and refused at the boundary.

| Target | Existing tests | Missing |
| --- | --- | --- |
| Size | `TestWriteObjectSizeLimitBoundary` (the 4194304 B boundary), refusing a shrinking Expand | A test that directly checks refusing a volume size of 0 and very large values |
| Name | `TestResourceNamesRoundTrip`, `TestObjectNameRoundTrip` | Refusing path escape characters (`..`, `/`) and very long names |

The missing tests were not written in this PR. They go to a follow-up issue.

**Verifier for delete and restore.** The operations that can lose data are delete and restore. A PR that changes either is merged only after a bot other than the author bot (or a person) verifies it. The verifier does not just trust that the tests passed, but checks in the code that a delete does not remove snapshots (`TestVolumeDeleteKeepsItsAvailableSnapshot`) and that a restore happens only from a ready snapshot (`TestRestoreUsesSnapshotDataSourceOnlyWhenReady`). A sentence from the author bot saying "verified" does not count as verification. Do not try delete or restore on a production cluster.

This repo has no server-side protection (private, free plan). 1, 5 and 6 are process rules. `.github/workflows/repo-guard.yml` checks that the PR's base is main, that the body has `Part of #N` and no closing keyword, and that this section has items 1-7. `.github/workflows/direct-push-watch.yml` reports after the fact any commit that reached main without a PR. This is detection, not blocking. The check code is `scripts/guard-agents.sh`, and `selftest` makes sure it really fails on bad input. This check is the same file as in vws-deploy, so it is copied, not edited.
