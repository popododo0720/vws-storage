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
| Logs go through `log/slog` and vws-lib `obs`. The standard `log` is not used | `TestArchitectureRules` |
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
| One `UpdateVolume` changes size, type, IOPS, throughput and labels together, checked against the target type and size. With growth the other values are stored in the write that stores the new size, never before (#87) | `AvailableVolume.Modify`, `VolumeExpansion.Then`. `TestModifyWithGrowthCarriesTheRestThroughExpansion`, `TestModifyVolumeWithGrowthResumesAfterCrash`, `TestUpdateVolumeAppliesSizeTypeAndPerformanceInOneMask`, `TestUpdateVolumeRefusesIllegalTypeAndPerformance` |
| A volume takes at most four size, type, IOPS or throughput changes in a rolling 24 hours, counted from start times stored on the volume row; labels, no-op updates, resent tokens and failed growths do not count. The fifth is `RESOURCE_EXHAUSTED` + `VOLUME_MODIFICATION_RATE_EXCEEDED` (EC2 `VolumeModificationRateExceeded`) (#157) | `Modifications.admit`, `AvailableVolume.Modify`, `AvailableVolume.Expand`. `TestModificationsRollingWindow`, `TestModifyCountsOnlyEC2ModifyVolumeChanges`, `TestModifyVolumeRateLimitIsFourPerRolling24Hours`, `TestFailedExpansionDoesNotCountAgainstTheModificationLimit`, `TestUpdateVolumeFifthModificationIsRateExceeded` |
| A volume an instance spec uses is not deleted. DeleteVolume is refused with `FAILED_PRECONDITION` + `DEPENDENCY_MISSING` (EC2 `VolumeInUse`) and nothing is written; an attachment list the backend cannot read refuses too. The attachment list is read before the store lock with a bounded timeout, never inside `Update`. The delete step reads the attachments again before the backend delete and retries while an instance holds the volume (#152) | `attachedTo` read in `Submit` (`preread`), `refuseAttached` in `admitDeleteVolume`, `detachedOrBusy` in `runVolumeDeletion`. `TestDeleteVolumeRefusesAnAttachedVolume`, `TestDeleteAfterDetachSucceeds`, `TestDeleteVolumeRefusesWhenAttachmentsAreUnreadable`, `TestDeleteWaitsForAnAttachRacingTheAdmission`, `TestSlowAttachmentsDoNotBlockOtherSubmits`, `TestDeleteVolumeAttachmentReadIsBounded`, `TestDeleteVolumeAttachedIsDependencyMissing` |
| A new bucket name follows the S3 namespace rules: an account-regional name ends with the caller's `-{account}-{region}-an` suffix (account from project `a{12 digits}`, region from the request), and a global name does not end with `-an`. A violation is `INVALID_ARGUMENT` + `INVALID_BUCKET_NAMESPACE` (S3 `InvalidBucketNamespace`) and stores nothing. A stored bucket replays before the check (#170) | `CreateBucketRequest.CheckNamespace` in `admitCreateBucket`. `TestCreateBucketNamespaceRules`, `TestAccountRegionalBucketNamespace`, `TestGlobalBucketRefusesTheAccountRegionalSuffix`, `TestCreateGlobalBucketWithAccountRegionalSuffixIsInvalidBucketNamespace` |
| A PR targets main and its body has `Part of #N` and no closing keyword; CI reports commits that reached main without a PR. AGENTS.md has Insider-risk guardrails 1-7 | `.github/workflows/repo-guard.yml`, `.github/workflows/direct-push-watch.yml`, `scripts/guard-agents.sh agents`, `scripts/guard-agents.sh selftest`. `ci.yml` runs both |
| The repo is English only (see "English only"). Hangul is allowed only in files `.github/english-allowlist` lists with a reason | `scripts/check-english.sh files` (CI `english` job) |

## English only

Write everything in this repo in English: README, code comments, docs, AGENTS.md, QUALITY.md, PR and issue templates, commit messages, PR and issue bodies, review comments and `Verified-by` sections. The `english` job in CI runs `scripts/check-english.sh`: `files` fails on a tracked file with Hangul unless `.github/english-allowlist` lists it as `<glob> <reason>` (only test data and user-facing UI strings belong there), and `text` and `commits` reject Hangul in the PR body and the PR's commit messages with no exception.

## /correct loop

1. When a bot is corrected (by the user Su Kim, tmp or eggbot, or by a failed `vws verify`), it opens an issue the same day with the `incident` label using the `.github/ISSUE_TEMPLATE/incident.yml` form. It must fill in the correction and "the check that would have caught this".
2. Every few days vws-verify runs a gardener pass. It groups open incidents by cause and promotes repeated ones to a check (test, lint, guard rule, archtest), not a sentence. The check must actually fail on an input that contains the mistake.
3. When the promoting PR is merged, close the incident with its link. An incident that will not be promoted is closed with the reason.
4. The milestone report (to tmp) lists the number of incidents in the period and the checks promoted.
5. The `incident` label is defined in `.github/labels.yml`, copied from vws-deploy. That file is only a definition and does not create the label on GitHub. `scripts/guard.sh correct` runs `scripts/guard-agents.sh correct`, which checks this section and its body, the required fields of the form and the label definition; `repo-guard.yml` runs it on every pull request and `scripts/guard_test.sh` shows it fails on an empty section.

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

Main is protected by the active `vws-main` ruleset: a pull request is required, only a squash merge is allowed, force push and deletion are off, and the required checks include `pr-hygiene`, `verify-gate` and `vws-verify`. The ruleset has no bypass actors. vws-deploy's `.github/drift/drift-check.sh` fails when this repo loses any of these. Guardrails 1, 5 and 6 stay process rules. For parts of 2 and 4, `.github/workflows/repo-guard.yml` checks that the PR's base is main, that the body has `Part of #N` and no closing keyword, and that this section has items 1-7, and `.github/workflows/direct-push-watch.yml` reports after the fact any commit that reached main without a PR. The check code is `scripts/guard-agents.sh`, and `selftest` makes sure it really fails on bad input. This check is the same file as in vws-deploy, so it is copied, not edited.

### Script push, PR and merge

A deterministic script (the factory orchestrator in vws-deploy `docs/factory-orchestrator.md`, the handoff submit) may push a branch, open a pull request and merge one. A script merge happens only when all of these hold:

- every CI check on the head commit is green;
- `verify-gate` passes;
- the `vws-verify` App check run passes;
- the pull request has no conflicts;
- the merge is a squash, made with `--match-head-commit` set to the verified head;
- the pull request is not labeled `blocked`.

`ORCHESTRATE_MERGE=apply` is approved only under these conditions. The lead (dr eggbot) is woken only for a decision or a failure (issue scope, real conflicts, a failed check), not for a routine push, pull request or merge. Guardrails 1, 4 and 2 still hold: the author bot is never the verifier, nothing pushes to main, and every pull request body has `Part of #N` with no closing keyword.
