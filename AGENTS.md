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

두 번째 쓰기 경로를 만들지 않는다. store를 서버나 백엔드에서 갱신하지 않는다. 리소스 상태에 실패를 넣지 않는다. 실패는 `Operation`에만 있다.

## 규칙

| 규칙 | 강제 |
| --- | --- |
| 원하는 상태는 `model.Change`로만 바뀐다 | `Change`의 기록은 비공개. 전이는 `internal/model` |
| 저장소 구현은 엔진 안에만 있다 | `internal/storage/internal/store` |
| 쓰기는 `Service.Submit` | 서버 패키지는 store를 import하지 않는다. depguard `server` |
| 진행은 `Service.Run` | `Run`이 이미 돌면 거절한다. `internal/storage` |
| 모델은 전송, SQL, 백엔드를 모른다 | depguard `model` |
| 서버는 백엔드를 모른다 | depguard `server` |
| 엔진은 gRPC, 생성 코드, fake를 모른다 | depguard `engine`, `store`, `backend`. 테스트 파일은 제외 |
| 이 레포는 proto를 두지 않는다. 생성 코드는 `github.com/popododo0720/vws-proto`이고, 공개 전에는 `replace`가 `./third_party/vws-proto`를 가리킨다 | `internal/policycheck`. lint는 `third_party`를 제외 |
| 배포 매니페스트, 로고, NOTICE를 두지 않는다 | `internal/policycheck` |
| 같은 토큰과 같은 요청은 같은 operation | `Service.Submit`, `TestCreateVolumeIsIdempotent` |
| 같은 토큰과 다른 요청은 충돌 | `ErrTokenReused` → gRPC `ABORTED` + `IDEMPOTENCY_CONFLICT` |
| 재시작 복구는 새 경로가 아니다 | `TestCreateVolumeResumesAfterCrash` |
| 거절된 `Change`는 저장소를 바꾸지 않는다 | 메모리 `Update`는 사본에 적용한 뒤 바꾼다. `storetest`가 `ErrStale` 이후 행을 다시 읽는다 |
| 실패한 생성은 `Creating` 행을 남기지 않는다 | 영구적인 정리는 행을 지운다. `TestPermanentUndoDropsVolume` |
| 리소스 이름은 `Parse(String())`로 돌아온다 | `ParseBucketName`이 `buckets/`를 벗긴다. `TestResourceNamesRoundTrip` |
| SQLite 파일은 프로세스 하나 | `BEGIN IMMEDIATE`와 `locking_mode=EXCLUSIVE`. `TestSecondOpenFails` |
| 한 세대의 바이트는 한 번만 쓴다 | `Signer` 계약. fake `Accept`가 다시 쓰기를 거절한다. `TestGenerationIsWrittenOnce` |
| 같은 오브젝트 경로의 변경은 겹치지 않는다 | 업로드와 삭제는 경로를 `Ref`로 걸고, `InFlight`면 거절한다 |
| 오브젝트 행은 퍼지가 끝난 뒤에 지운다 | `Object.Delete`는 행을 가드만 한다. `Purged`가 같은 세대일 때만 지운다 |
| 없는 버킷의 오브젝트 목록은 빈 목록이 아니다 | `ListObjects`가 버킷 행을 먼저 읽는다 |
| 프로젝트는 저장 키다. 요청의 project가 행을 가른다 | `VolumeID.String`이 `projects/{project}/volumes/{id}`. `TestProjectScopedVolumeAndBucket` |
| 허용 목록에 없는 프로젝트는 그 프로세스의 리소스가 아니다 | `Server.allowProject` → `RESOURCE_NOT_FOUND`. `TestProjectAllowListRejected` |
| 이미지는 이름만 저장한다. 조회, 복사, 크기 검사를 하지 않는다 | `ImageName`은 `Key`도 `Ref`도 아니다. `TestImageSourceSurvivesCodec`, `TestCreateVolumeFromImage` |
| `Object.etag`은 동시성 토큰이고 md5가 아니다 | `status.md5`는 `Object.MD5`, etag는 `Meta.Version`. `TestWriteObjectReplacesPayloadAndKeepsEtagSeparateFromMD5` |
| 같은 `request_id`의 WriteObject는 다시 쓰지 않는다 | `Service.Submit`의 토큰 조회가 admit보다 먼저다. `TestWriteObjectIsIdempotentOnRequestID` |
| 볼륨은 늘리기만 한다 | `AvailableVolume.Expand`가 축소를 거절한다. `TestExpandGrowsOnlyAndStoresSizeOnCompletion`, `TestUpdateVolumeExpandsAndRejectsShrink` |
