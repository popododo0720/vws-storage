# AGENTS.md

VWS storage는 EBS 볼륨/스냅샷과 S3 호환 버킷/오브젝트의 제어 평면이다. 오브젝트 바이트는 백엔드로 가고, 원하는 상태는 store에만 남는다. 백엔드는 인터페이스 뒤의 fake다.

## pstack

작업을 pstack으로 한다.

- **architect**: 구조가 다른 설계를 둘 두고 고른다.
- **interrogate**: 구현을 리뷰어 둘이 본다. 리뷰 결과를 자동으로 적용하지 않는다.
- **correct**: 같은 실수가 반복되면 문서보다 먼저 타입, 패키지 경계, lint, 테스트로 막는다.

모든 코딩과 개발은 pstack poteto-mode 스타일로 한다. 응답과 코드는 간결하고 단순하게 쓰고, 실제로 돌려 확인한 것만 보고한다. 서브에이전트를 쓰면 effort는 high다. VWS 작업에 Cursor 클라우드 에이전트를 쓰지 않는다. 사용자가 직접 시킬 때만 예외다.

## 브랜치와 커밋

- main에 직접 푸시하지 않는다. 작업은 브랜치에서 한다.
- 커밋은 원자적이다. 한 커밋은 한 변경이고, 그 커밋만으로 빌드가 성립한다.

## 형태

쓰기는 `storage.Service.Submit` 하나다. `Submit`은 `store.Update` 안에서 `model.Change`만 적용한다. `Change`의 기록 목록은 비공개라 전이 함수 밖에서 만들 수 없다. 진행은 `storage.Service.Run` 하나다. `Open`은 `Run`을 시작하지 않는다. 재시작 후에 남은 작업도 같은 `Run` 스캔이 맡는다. 서버는 요청을 파싱해서 `Service`만 호출한다. 긴 작업은 `Operation`이고, 클라이언트 토큰이 같다.

두 번째 쓰기 경로를 만들지 않는다. store를 서버나 백엔드에서 갱신하지 않는다. 실패는 `Operation`에 있다. 예외는 생성이 영구 실패한 볼륨 하나다. 그 볼륨은 이유와 함께 `Failed`(ERROR) 상태로 남는다(#83).

## 규칙

강제 칸이 `Judgment`인 규칙은 검사가 없다. 작성자와 리뷰어가 지킨다. 그 밖의 강제 칸은 규칙이 깨지면 실패하는 테스트, lint, CI다. 표의 형식과 강제 칸의 테스트 이름은 `internal/archtest` `TestAgentsRuleTable`이 보고, CI 로그에 `Judgment` 행 수를 남긴다.

| 규칙 | 강제 |
| --- | --- |
| main에 직접 푸시하지 않는다. 작업은 브랜치에서 한다 | `.github/workflows/direct-push-watch.yml`(사후 탐지) |
| 커밋은 원자적이다. 한 커밋은 한 변경이고, 그 커밋만으로 빌드가 성립한다 | Judgment |
| pstack으로 작업한다(architect, interrogate, correct) | Judgment |
| 원하는 상태는 `model.Change`로만 바뀐다 | `Change`의 기록은 비공개. 전이는 `internal/model` |
| 저장소 구현은 엔진 안에만 있다 | Go `internal` 디렉터리 `internal/storage/internal/store`. store는 store 계약과 모델만 본다: `TestArchitectureRules` |
| 쓰기는 `Service.Submit` | 서버 패키지는 store를 import하지 않는다. depguard `server`, `TestArchitectureRules` |
| 진행은 `Service.Run` | `Run`이 이미 돌면 거절한다. `internal/storage` |
| 모델은 전송, SQL, 백엔드를 모른다. 이 모듈의 어떤 패키지도 import하지 않는다 | depguard `model`, `TestArchitectureRules` |
| 서버는 엔진과 모델만 import한다. 백엔드도 store도 모른다 | depguard `server`, `TestArchitectureRules` |
| 엔진은 gRPC, 생성 코드, fake를 모른다. 엔진은 백엔드 계약만 보고 kube, s3, fake 구현과 서버를 import하지 않는다 | depguard `engine`, `store`, `backend`, `TestArchitectureRules`. 테스트 파일은 제외 |
| 백엔드는 백엔드 계약과 모델만 import한다. 엔진, 서버, 다른 백엔드를 모른다 | depguard `backend`, `TestArchitectureRules` |
| 헬스는 이 모듈의 패키지를 import하지 않는다. 검사는 프로세스가 넘긴다 | `TestArchitectureRules` |
| 외부 의존은 주인 패키지에만 있다: 생성 코드와 protobuf는 서버, gRPC는 서버·헬스·`cmd`, SQL은 SQLite store, Kubernetes는 kube 백엔드와 `cmd`, AWS SDK는 s3 백엔드 | `TestArchitectureRules` |
| 프로덕션 코드는 `testing`을 import하지 않는다. 예외는 `storetest` | `TestArchitectureRules` |
| 로그는 `log/slog`와 vws-lib `obs`로 남긴다. 표준 `log`는 쓰지 않는다. 남은 예외는 `internal/server/convert.go` `toStatus` 하나다(컨텍스트가 없다) | `TestArchitectureRules`의 `knownDebt`. 그 항목을 지우면 테스트가 막는다 |
| 이 레포는 proto를 두지 않는다. 생성 코드는 `github.com/popododo0720/vws-proto`이고, 공개 전에는 `replace`가 `./third_party/vws-proto`를 가리킨다 | `internal/policycheck`. lint는 `third_party`를 제외 |
| 배포 매니페스트, 로고, NOTICE를 두지 않는다 | `internal/policycheck` |
| 같은 토큰과 같은 요청은 같은 operation | `Service.Submit`, `TestCreateVolumeIsIdempotent` |
| 같은 토큰과 다른 요청은 충돌 | `ErrTokenReused` → gRPC `ABORTED` + `IDEMPOTENCY_CONFLICT` |
| 재시작 복구는 새 경로가 아니다 | `TestCreateVolumeResumesAfterCrash` |
| 거절된 `Change`는 저장소를 바꾸지 않는다 | 메모리 `Update`는 사본에 적용한 뒤 바꾼다. `storetest`가 `ErrStale` 이후 행을 다시 읽는다 |
| 영구 실패한 볼륨 생성은 `Creating` 행을 남기지 않는다. 볼륨은 이유와 함께 `Failed`로 남고 DeleteVolume으로 지운다 | `TestPermanentCreateFailureKeepsTheVolumeInError`, `TestVolumeInErrorRefusesWorkAndDeletes` |
| 리소스 이름은 `Parse(String())`로 돌아온다 | `ParseBucketName`이 `buckets/`를 벗긴다. `TestResourceNamesRoundTrip` |
| SQLite 파일은 프로세스 하나 | `BEGIN IMMEDIATE`와 `locking_mode=EXCLUSIVE`. `TestSecondOpenFails` |
| 한 세대의 바이트는 한 번만 쓴다 | `Signer` 계약. fake `Accept`가 다시 쓰기를 거절한다. `TestGenerationIsWrittenOnce` |
| 같은 오브젝트 경로의 변경은 겹치지 않는다 | 업로드와 삭제는 경로를 `Ref`로 걸고, `InFlight`면 거절한다 |
| 오브젝트 행은 퍼지가 끝난 뒤에 지운다 | `Object.Delete`는 행을 가드만 한다. `Purged`가 같은 세대일 때만 지운다 |
| 없는 버킷의 오브젝트 목록은 빈 목록이 아니다 | `ListObjects`가 버킷 행을 먼저 읽는다 |
| 프로젝트는 저장 키다. 요청의 project가 행을 가른다 | `VolumeID.String`이 `projects/{project}/volumes/{id}`. `TestProjectScopedVolumeAndBucket` |
| 허용 목록에 없는 프로젝트는 그 프로세스의 리소스가 아니다 | `Server.allowProject` → `RESOURCE_NOT_FOUND`. `TestProjectAllowListRejected` |
| 엔진은 이미지를 이름만 저장한다. 조회, 복사, 크기 검사를 하지 않는다. 복제는 kube 백엔드가 CDI DataSource에서 한다(#86) | `ImageName`은 `Key`도 `Ref`도 아니다. `TestImageSourceSurvivesCodec`, `TestCreateVolumeFromImage`, `TestImageVolumeClonesTheImageDataSource` |
| A volume clones an image only when the DataSource labels match the caller's project and region, or carry `vws.io/public=true` and the same region. Any other image answers exactly like a missing one (#89). | The server compares the image name's project with the request project (`RESOURCE_NOT_FOUND`). The kube backend checks the DataSource labels `vws.io/project`, `vws.io/region` and `vws.io/public` (`cloneAllowed`). `TestCreateVolumeFromAnotherProjectsImageIsNotFound`, `TestImageOfAnotherOwnerIsNotFound`, `TestCloneAllowed`, `TestPublicImageOfAnotherProjectIsCloned` |
| `Object.etag`은 동시성 토큰이고 md5가 아니다 | `status.md5`는 `Object.MD5`, etag는 `Meta.Version`. `TestWriteObjectReplacesPayloadAndKeepsEtagSeparateFromMD5` |
| 같은 `request_id`의 WriteObject는 다시 쓰지 않는다 | `Service.Submit`의 토큰 조회가 admit보다 먼저다. `TestWriteObjectIsIdempotentOnRequestID` |
| 볼륨은 늘리기만 한다 | `AvailableVolume.Expand`가 축소를 거절한다. `TestExpandGrowsOnlyAndStoresSizeOnCompletion`, `TestUpdateVolumeExpandsAndRejectsShrink` |
| 볼륨 라벨과 타입은 승인 쓰기 하나로 바뀌고 백엔드 객체를 건드리지 않는다. 타입은 백엔드가 그대로 둘 수 있을 때만 바뀐다(kube: 같은 StorageClass)(#87) | `AvailableVolume.Modify`, `backend.Retyper`. `TestModifyVolumeSetsLabelsAndTypeAndSurvivesRestart`, `TestRetypeNeedsTheSameStorageClass`, `TestUpdateVolumeSetsLabelsAndType` |
| PR은 main을 대상으로 하고 본문에 `Closes #N`이 있으며, main에 PR 없이 들어온 커밋은 CI가 알린다. AGENTS.md는 Insider-risk guardrails 1~7을 갖는다 | `.github/workflows/repo-guard.yml`, `.github/workflows/direct-push-watch.yml`, `scripts/guard-agents.sh agents`, `scripts/guard-agents.sh selftest`. 둘 다 `ci.yml`이 부른다 |

## Insider-risk guardrails

봇도 내부자 위험이다. 아래는 예외 없는 규칙이다. 설계는 popododo0720/vws-deploy#33이다.

1. **검증 독립**: 중요 결과(머지, 보안, 데이터)는 작성한 봇/모델이 스스로 검증하지 않는다. 검증은 작성 봇이 아닌 다른 봇(또는 사람)이 한다. 프로세스 규칙이고 CI로 강제할 수 없다.
2. **기록**: 모든 변경은 이슈 → PR(`Closes #N`) → 커밋으로 남긴다. 작업 로그는 PR 본문과 이슈 코멘트에 사람이 읽을 수 있게 쓴다. force-push와 히스토리 재작성은 금지다. 사용자가 승인하면 예외다.
3. **실패 테스트**: 새 기능은 정상 경로만 테스트하지 않는다. 전원 차단과 재시작, 멱등 재실행, 악성·경계 입력을 최소 한 개씩 포함하거나 불가 사유를 PR에 적는다.
4. **최소 권한**: main에 직접 푸시하지 않는다. 모든 변경은 main에서 딴 독립 브랜치의 단일 PR이다. 스택 PR과 로컬 머지는 금지다. 토큰은 해당 레포 범위만 쓴다. RBAC은 서비스 코드가 실제로 호출하는 리소스만 허용하고, 가능하면 네임스페이스 범위의 Role을 쓴다.
5. **감사 분리**: 작성 봇, 검증 봇, 머지 판단은 서로 다르다. 작성 봇은 자기 PR을 머지하지 않는다. 머지는 base가 main이고 head 커밋의 CI가 전부 success일 때만 한다.
6. **격리·비상정지**: 봇 즉시 중단은 사용자 요청이나 오케스트레이터가 해당 봇 세션을 중단하고 토큰을 폐기하는 것이다(`gh auth`, PAT revoke). 위험 작업은 샌드박스(박스, 임시 클러스터)에서만 한다. 운영 클러스터에 접근하지 않는다.
7. **사고 공개**: 규칙 위반과 사고는 숨기지 않고 이슈에 `원인 / 영향 / 조치 / 재발방지`로 기록한다. 라벨은 `incident`다.

### vws-storage 항목

**멱등성 테스트.** Expand, Restore, Delete는 같은 요청을 다시 보내도 같은 결과여야 한다. 아래는 지금 있는 테스트와 없는 것이다.

| 대상 | 있는 테스트 | 없는 것 |
| --- | --- | --- |
| Expand | `TestExpandVolumeGrowsOnceAndRejectsShrink`, `TestExpandGrowsAndRejectsShrink`(kube) | 같은 클라이언트 토큰의 Expand 재전송을 직접 보는 테스트 |
| Restore(스냅샷에서 볼륨 생성) | `TestRestoreUsesSnapshotDataSourceOnlyWhenReady`, `TestCreateVolumeIsIdempotent` | 같은 스냅샷 복원 요청의 재호출을 직접 보는 테스트 |
| Delete | `TestDeleteVolumeIsIdempotent`, `TestDeleteSnapshotIsIdempotent`, `TestDeleteReplayAndRecreate`, `TestDeleteFailureRestoresVolume` | 없음 |

**악성 입력 테스트.** 크기와 이름은 경계에서 파싱하고 거절한다.

| 대상 | 있는 테스트 | 없는 것 |
| --- | --- | --- |
| 크기 | `TestWriteObjectSizeLimitBoundary`(4194304 B 경계), 축소 Expand 거절 | 볼륨 크기 0과 매우 큰 값의 거절을 직접 보는 테스트 |
| 이름 | `TestResourceNamesRoundTrip`, `TestObjectNameRoundTrip` | 경로 탈출 문자(`..`, `/`)와 매우 긴 이름의 거절 |

없는 테스트는 이 PR로 쓰지 않았다. 후속 이슈로 올린다.

**삭제·복원의 검증자.** 데이터를 잃을 수 있는 작업은 삭제와 복원이다. 이 둘을 바꾸는 PR은 작성 봇과 다른 봇(또는 사람)이 검증한 뒤에만 머지한다. 검증자는 테스트가 통과했다는 말만 믿지 않고, 삭제가 스냅샷을 지우지 않는 것(`TestVolumeDeleteKeepsItsAvailableSnapshot`)과 복원이 준비된 스냅샷에서만 일어나는 것(`TestRestoreUsesSnapshotDataSourceOnlyWhenReady`)을 코드에서 직접 확인한다. 작성 봇이 쓴 "검증했다"는 문장은 검증으로 치지 않는다. 운영 클러스터에서 삭제나 복원을 시험하지 않는다.

이 레포의 서버측 보호는 없다(private, 무료 플랜). 1, 5, 6은 프로세스 규칙이다. `.github/workflows/repo-guard.yml`이 PR의 base가 main인지, 본문에 `Closes #N`이 있는지, 이 섹션의 1~7 항목이 있는지 검사한다. `.github/workflows/direct-push-watch.yml`이 main에 PR 없이 들어온 커밋을 사후에 알린다. 차단이 아니라 탐지다. 검사 코드는 `scripts/guard-agents.sh`이고 `selftest`가 나쁜 입력에서 실제로 실패하는지 본다. 이 검사는 vws-deploy와 같은 파일이므로 고치지 않고 복사한다.
