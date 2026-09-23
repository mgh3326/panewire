# 노드 갱신 절차 — 태그 → Release → 카나리아 1대 → 나머지

노드(`panewire daemon`) 바이너리를 hub 의 `update.available` 로 교체하는 운영자 절차다.
교체 장치는 기존 R19 경로 하나(`hub_r19_node.go` `applyHubUpdate`)뿐이고, 이 문서는 그것을
**언제·어느 노드에** 쓰는지를 정한다. 판단이 필요한 곳·예상과 다른 출력은 전부 멈추고 ESC 한다.

## 🔴 먼저 읽을 것 — 보호 장치가 효력을 갖는 시점

- **허브 배포 전: `--machines` 에 ncp 금지 — 허브가 막지 않는다.**
  ncp 거부(AC6)와 `update.overdue`(AC5)는 **hub 코드**다. hub 배포는 BLOCK-3(CF Access 입력) 뒤라,
  이 코드가 머지돼도 hub 가 교체되기 전에는 효력이 없다. 그 사이 ncp 제외는 **이 절차로만** 지켜진다.
  새 `panewire` CLI 는 `--machines` 에 ncp(대소문자·공백 변형 포함)가 있으면 요청을 보내기 전에
  거부하지만, 옛 CLI·직접 HTTP 요청은 막지 못한다. ncp 를 넣어 "거부되는지" 시험하는 것도 금지다 —
  옛 hub 는 그대로 발행한다.
- **첫 회는 카나리아 1대 + 수동 확인(보호 장치는 두 번째 갱신부터 유효).**
  URL 레포 고정(AC2)·교체 전 시험 기동(AC3)·되돌리기(AC4)는 **노드 코드**다. 첫 롤아웃에서
  새 바이너리를 내려받는 것은 보호 장치가 없는 **옛 노드 코드**이므로, 첫 회에는 레포 고정도
  시험 기동도 되돌리기도 없다. 첫 회는 반드시 카나리아 1대로 하고 §4 를 사람이 확인한 뒤에만 넓힌다.
- **첫 회는 `update publish` 로 되지 않는다 — 손 설치(§0)다.** 옛 노드 코드는 리다이렉트를
  `objects.githubusercontent.com` 으로만 따라가는데, GitHub release download 는 현재
  다른 release-asset 호스트로 302 한다(2026-09-23 확인). 그래서 옛 노드는 자산을 받지 못하고
  `update unavailable` 로 끝나며 재시작하지 않는다(실행 파일 불변). 이 코드를 가진 바이너리가
  한 번 손으로 깔린 노드부터 §2 의 `update publish` 가 동작한다.

## 대상과 경계

| 노드 | 대상 여부 | 이유 |
|---|---|---|
| ncp (root) | **영구 제외** | `panewired.service` 와 `panewire-hub.service` 가 같은 실행 파일을 쓴다. 노드 갱신 = 모르는 사이의 hub 배포(BLOCK-3). hub·CLI·노드(자기 machine id 가 ncp 면 교체 거부) 3곳에서 거부. |
| ncp-director (비root) | 현재 대상 아님 | 같은 실행 파일을 쓰는데 쓰기 권한이 없어 `update unavailable` 로 조용히 실패한다. 사용자 홈의 전용 바이너리로 옮기는 것은 별건 태스크. |
| 그 외 노드 | 대상 | launchd `KeepAlive` 또는 systemd `Restart=always` 로 떠 있어야 한다 — 교체 뒤 재시작과 되돌리기 모두 그 재시작에 기댄다. |

- 노드는 `https://github.com/<레포>/releases/download/<태그>/panewire_<버전>_<os>_<arch>` 에서
  **시작하는** 다운로드만 받는다. 리다이렉트는 GitHub 의 release-asset 저장소 호스트
  (`hub_r19_node.go` 의 `hubUpdateAssetHosts` 두 곳)로만 따라간다.
  `<레포>` 기본값은 `mgh3326/panewire`, `PANEWIRE_UPDATE_REPO=<owner>/<repo>` 로 바꿀 수 있다(hub·노드 각각).
  값이 형식에 안 맞으면 그 프로세스는 갱신 전체를 거부한다.
- `<버전>` 은 `pw-<커밋 앞 7자리>`(hub-status 의 기존 형식, 예 `pw-05667f4`) 한 문자열이다.
  바이너리 스탬프(`-X main.version`)·자산 이름·`SHA256SUMS`·`update publish --version`·노드의 시험 기동 비교가
  모두 이 값이다. hub 는 자산 이름의 버전이 `--version` 과 다르면 400 으로 거부한다.
- 신뢰 경계: 이 레포에 태그를 밀 수 있는 사람은 전 노드에 코드를 넣을 수 있다. 운영자 토큰만으로는
  이 레포 Release 밖의 코드를 넣을 수 없다 — 단 이 레포의 **옛** 태그로 되돌리는 것은 막지 않는다.

## 0. 첫 회 — 손 설치(노드당 1회, 운영자)

§1 로 만든 첫 Release(이 코드가 들어간 것)를 각 노드에 한 번 손으로 깐다. 카나리아 1대 → §3·§4 확인 → 나머지 순서는 같다.

```sh
TAG=v<YYYYMMDD>.<n>; VERSION=pw-<sha7>; ASSET=panewire_${VERSION}_<os>_<arch>
curl -fsSLO "https://github.com/mgh3326/panewire/releases/download/$TAG/$ASSET"
curl -fsSLO "https://github.com/mgh3326/panewire/releases/download/$TAG/SHA256SUMS"
grep " $ASSET\$" SHA256SUMS | shasum -a 256 -c -      # "$ASSET: OK" 가 아니면 멈춘다
chmod 755 "$ASSET" && test "$(./"$ASSET" version)" = "$VERSION"
# 실행 파일 옆에 옛 바이너리를 <실행 파일>.bak-<UTC> 로 남기고, 같은 폴더의 임시 이름으로 복사한 뒤 mv 로 교체,
# 그다음 launchd/systemd 로 재시작한다. ncp·ncp-director 는 하지 않는다.
```

기대: §3 의 hub-status 에서 그 노드 `version=pw-<sha7>`. 손 설치 중 `update publish` 를 그 노드에 보내지 않는다.

## 1. 태그 → Release

```sh
git fetch origin && git switch --detach origin/main
git tag -a v<YYYYMMDD>.<n> -m "panewire node release"
git push origin v<YYYYMMDD>.<n>
```

`.github/workflows/release.yml` 이 `go vet`·`go test` 뒤 `scripts/build-release.sh` 로 4개
(darwin/amd64 · darwin/arm64 · linux/amd64 · linux/arm64)를 빌드하고, 스탬프·체크섬을 확인한 뒤
Release 를 만든다.

기대: Release 에 `panewire_pw-<sha7>_{darwin,linux}_{amd64,arm64}` 4개 + `SHA256SUMS`.
`<sha7>` 은 `git rev-parse origin/main | cut -c1-7` 과 같다. 다르면 멈춘다.

로컬에서 같은 산출물을 만들어 비교하려면(선택):

```sh
sh scripts/build-release.sh /tmp/panewire-release   # 마지막 줄에 pw-<sha7>
(cd /tmp/panewire-release && shasum -a 256 -c SHA256SUMS)
```

## 2. 카나리아 1대에 발행

```sh
TAG=v<YYYYMMDD>.<n>
VERSION=pw-<sha7>
ASSET=panewire_${VERSION}_<os>_<arch>          # 카나리아 노드의 os/arch
SHA=$(curl -fsSL "https://github.com/mgh3326/panewire/releases/download/$TAG/SHA256SUMS" | awk -v a="$ASSET" '$2==a{print $1}')
test ${#SHA} -eq 64                              # 아니면 멈춘다
panewire update publish \
  --hub-url <hub HTTPS URL> --hub-token-env <operator token env> [--hub-cf-env <cf env>] \
  --version "$VERSION" --sha256 "$SHA" \
  --url "https://github.com/mgh3326/panewire/releases/download/$TAG/$ASSET" \
  --machines <카나리아 1대>
```

- 🔴 `--machines` 는 **정확히 1대**, ncp 금지(위 §먼저 읽을 것).
- 기대: 종료코드 0, 출력 `{"published":["<카나리아>"]}`. `update rejected` 면 멈추고 입력을 확인한다
  (자산 이름의 버전 ≠ `--version`, 레포 밖 URL, 중복·제외 노드 모두 400).

## 3. hub-status 로 버전 확인

```sh
panewire hub-status --hub-url <hub HTTPS URL> --hub-token-env <token env> [--hub-cf-env <cf env>]
```

기대(수 분 안): 카나리아 행 `REMOTE_META` 의 `version=pw-<sha7>`, `STATE=connected`.

| 관측 | 뜻 | 조치 |
|---|---|---|
| `LAST_NOTE` = `update rejected(smoke) pw-<sha7>` | 새 바이너리가 `version` 시험 기동(5초, exit 0, 버전 일치)에 실패. 실행 파일·`.bak` 불변. | 멈춘다. 자산 os/arch·스탬프를 확인하고 ESC. |
| `LAST_NOTE` = `update overdue pw-<sha7>` | 마감(기본 10분)까지 그 버전으로 돌아오지 않음. hub 가 sink 레인에 `update.overdue` 1건. | 멈춘다. 노드 상태를 사람이 확인. |
| 옛 버전으로 `connected` | 되돌리기가 동작했을 수 있다(아래 §되돌리기). | 멈추고 ESC. |

## 4. 수동 확인(첫 회 필수, 이후 권장)

카나리아에서(운영자가 직접, 접속 권한이 있는 사람만):

- `panewire version` 이 `pw-<sha7>`.
- 실행 파일 옆 `panewire.bak-<UTC>` 가 1개 늘었고(최대 2개 보존), `panewire.update-state` 가 **없다**
  (첫 hello 성공 시 지워진다).
- darwin/arm64 노드: 새 바이너리가 서명 문제 없이 떠 있다(`codesign -dv <실행 파일>` 에 ad-hoc 서명).
  첫 darwin/arm64 카나리아에서 1회 확인하고 결과를 기록한다.

## 5. 나머지 노드

§2 를 `--machines <나머지, 쉼표 구분>` 으로 반복한다(ncp 제외 · ncp-director 제외). §3 의 표로 확인한다.

## 되돌리기(AC4) — 노드가 스스로 하는 것

- `applyHubUpdate` 는 교체 직전 실행 파일 옆에 `panewire.update-state`(기대 버전·`.bak` 이름·기동 횟수)를 쓴다.
- `panewire daemon` 은 **기동 최초**(플래그 파싱·스키마 가드보다 앞)에 이를 판정한다:
  자기 버전 ≠ 기록 버전이면 기록을 지운다. 같으면 기동 횟수를 1 올린다.
  hub 에 hello 를 하면 기록을 지운다(카운터 리셋).
- 기동 뒤 **60초** 를 살아 있으면(hello 여부와 무관) 그 기동은 "기동 직후 죽음" 이 아니므로 기동 횟수를
  0 으로 되돌린다 — hub 가 한동안 불통이어도 오래 정상 동작한 뒤의 계획 재시작은 되돌리기로 이어지지 않는다.
- hello 없이 60초 안에 죽은 기동이 **3회 연속**이면 4번째 기동이 기록된 `.bak-<UTC>` 를 원자적으로 되돌려 놓고
  비0 종료한다 — launchd/systemd 가 옛 바이너리를 띄운다. `.bak` 은 지우지 않는다.
- `.bak` 이 없으면 `rollback_unavailable` 로 표시하고 새 바이너리로 계속 돈다(오늘과 같은 동작) → 사람이 복구.

## 설정

| 무엇 | 어디 | 기본 |
|---|---|---|
| 고정 레포 | `PANEWIRE_UPDATE_REPO` (hub·노드 프로세스 환경) | `mgh3326/panewire` |
| overdue sink 레인 | hub `--update-overdue-lane <레인>`, 그 레인은 `lanes.json` 에서 `"sink": true` | 플래그가 없으면 `lanes.json` 의 sink 레인이 **정확히 1개**일 때 그 레인 |
| 확인 마감 | hub 내부 `UpdateConfirmationTimeout` | 10분 |

`update.overdue` 는 Telegram 알림(연결 끊김·stale 등)을 쓰지 않는다. sink 가 아닌 레인을 지정하면
어느 pane 에도 주입되지 않는다. 행이 아직 기록되지 않은 통지(handoffkeep 실패, sink 레인 없음·여럿)는
hub 가 sweep 마다 다시 시도하고, 행이 생기면 멈춘다 — (노드, 버전)당 행은 1개다(hub 재시작 뒤에도).
대기 중인 통지는 hub 메모리에만 있으므로 hub 재시작 전에 기록되지 못한 통지는 사라진다(LAST_NOTE 도 마찬가지).
