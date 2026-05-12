# S2 최신화 자료원 딥다이브 Survey

작성일: 2026-05-12

## 결론

현재 S2 최신화는 아래 3개 축을 자동으로 받는다.

1. `[5031] 정산정보(지급)관리`
2. `정산정보 누락 건` guard
3. `정산정보(청구)관리` guard

질문에 나온 `https://kiss.kld.kr/stm/stm/pymt-missing-list` 화면 자체를 브라우저 조작으로 다운로드하지는 않는다. 대신 그 화면의 데이터로 보이는 API `GET /stm/stm`을 `scripts/refresh_s2_reference_guards.py`에서 호출해 `data/s2_payment_missing_lookup.csv`로 저장한다.

반대로 `판매채널 콘텐츠 조회` 자료는 정식 S2 최신화 체인에는 아직 들어 있지 않다. 관련 API 조사/감사 스크립트는 있지만, 앱의 일반 매핑 기준 또는 daily refresh 산출물로 자동 반영되지는 않는다.

## 현재 받는 자료

| 자료 | 현재 자동 최신화 | 구현 | 산출물 | 최신 로컬 행 수 |
| --- | --- | --- | --- | ---: |
| 정산정보(지급)관리 | 예 | `GET /mst/setl/pymt-setl` | `data/kiss_payment_settlement_s2_lookup.csv` | 124,755 |
| 정산정보 누락 건 | 예 | `GET /stm/stm` | `data/s2_payment_missing_lookup.csv` | 2,327 |
| 정산정보(청구)관리 | 예, 보조 guard | `GET /mst/setl/req-setl` | `data/s2_billing_settlement_lookup.csv` | 869 |
| 판매채널 콘텐츠 조회 | 아니오 | 감사 스크립트에만 `GET /sale/ext/ext-salm/schn-ctns` 존재 | 정식 앱 기준 없음 | 해당 없음 |

근거:

- 앱 기준 파일 경로: `app.py`의 `S2_SOURCE_LOOKUP`, `S2_MISSING_LOOKUP`, `S2_BILLING_LOOKUP`
- daily refresh: `scripts/run_daily_s2_refresh.ps1`
- 지급정산 API: `scripts/refresh_kiss_payment_settlement.py`
- 누락/청구 guard API: `scripts/refresh_s2_reference_guards.py`
- 판매채널콘텐츠 감사 API: `scripts/audit_sales_channel_settlement_gap.py`

## 자료별 상세

### 1. 정산정보(지급)관리

정식 S2 기준의 본체다.

- 앱 실행: `app.py`의 `run_s2_refresh()`
- 일일 실행: `scripts/run_daily_s2_refresh.ps1`
- 스크립트: `scripts/refresh_kiss_payment_settlement.py`
- API: `GET /mst/setl/pymt-setl`
- 기본 조건: `1900-01-01 ~ 오늘`, `pageSize=1000000`, `ctnsStleCd=102`
- 앱 수동 안내 URL: `https://kiss.kld.kr/mst/stmi/pymt-setl`

2026-05-12 요약:

- source rows: 141,732
- compact S2 lookup rows: 124,755
- 콘텐츠형태: 소설 141,732
- 판매채널콘텐츠ID unique: 124,755

운영 의미:

- 매핑의 실제 후보는 이 지급정산 lookup이다.
- `판매채널콘텐츠ID`, `콘텐츠ID`, `콘텐츠명`, `판매채널명`이 여기서 온다.
- 이 자료에 없는 같은 채널 작품은 기본적으로 `no_match`가 된다.

### 2. 정산정보 누락 건

질문에 나온 `pymt-missing-list` 계열 자료는 현재 이미 받고 있다.

- 스크립트: `scripts/refresh_s2_reference_guards.py`
- API: `GET /stm/stm`
- 기본 조건: `ctnsStleCd=102`, `plfmCd=""`, `pageSize=1000000`
- 산출물: `data/s2_payment_missing_lookup.csv`
- 2026-05-12 행 수: 2,327

현재 동작:

- S2 기준 로딩 직후 `판매채널콘텐츠ID` 기준으로 누락 건을 제외한다.
- no_match 행에는 `판매채널명 + 정제_콘텐츠명` 기준으로 누락 후보 라벨을 붙인다.
- 전송자료 생성에서는 `S2_분리사유`가 있으면 차단된다.

주의:

- 코드에는 `pymt-missing-list` 화면 URL 문자열이 없다.
- 따라서 “브라우저에서 유통 탭 선택, 소설 선택, 조회, 엑셀 다운로드”를 자동화하는 구조가 아니라 API 직접 조회 구조다.
- 현재 `plfmCd`는 빈 값이다. 빈 값이 “전체 플랫폼”인지, 화면 기본 탭인지, 또는 유통과 동등한지는 API 동작 검증이 필요하다.

### 3. 정산정보(청구)관리

지급정산이 아닌 청구정산 후보를 보조 사유로 붙인다.

- 스크립트: `scripts/refresh_s2_reference_guards.py`
- API: `GET /mst/setl/req-setl`
- 산출물: `data/s2_billing_settlement_lookup.csv`
- 2026-05-12 행 수: 869

운영 의미:

- 청구정산 목록 응답에는 지급정산용 `판매채널콘텐츠ID`가 없다.
- 그래서 매칭 확정 기준이 아니라 no_match 행의 `청구정산 후보 있음` 정보로만 쓴다.

### 4. 판매채널 콘텐츠 조회

정식 최신화에는 없다.

존재하는 것은 별도 감사 스크립트다.

- 스크립트: `scripts/audit_sales_channel_settlement_gap.py`
- API: `GET /sale/ext/ext-salm/schn-ctns`
- 목적: 판매채널콘텐츠는 있는데 지급정산이 없는 케이스를 감사/분류
- 현재 앱 일반 매핑 기준에는 미연결

운영 의미:

- 지금 앱은 “지급정산관리 기준에 있는 것”을 매핑 후보로 삼는다.
- 판매채널콘텐츠만 있고 지급정산이 없는 작품은 정식 매칭 후보가 아니라, 별도 근거 자료가 있을 때 `정산정보 없음/보강 필요` 계열로 해석하는 것이 맞다.

## 개선 필요성

### P0. 지금 당장 큰 누락은 아님

`pymt-missing-list` 계열 자료를 아예 안 받고 있는 상태는 아니다. 이미 `/stm/stm`으로 받아서 누락 guard로 쓰고 있다.

### P1. `유통` 탭 필터 검증 필요

사용자가 알려준 화면 플로우는 `유통` 탭 선택 후 `소설` 조회다.

현재 코드는:

- `ctnsStleCd=102`는 명시한다.
- `plfmCd`는 빈 값으로 보낸다.
- CLI 옵션 `--platform-code`는 있지만 daily/app refresh에서는 넘기지 않는다.

개선안:

1. 실제 `유통` 탭의 `plfmCd` 값을 확인한다.
2. `plfmCd=""` 조회와 `plfmCd=<유통>` 조회의 row/id 차이를 비교하는 감사 스크립트를 만든다.
3. 차이가 있으면 daily refresh에서 `--platform-code <유통>`을 명시하거나, 전체+유통 비교 결과를 summary에 남긴다.

리스크:

- 빈 값이 이미 전체라면 명시 필터가 오히려 누락을 만들 수 있다.
- 유통 외 탭에도 소설 정산 관련 누락 건이 섞여 있으면 단일 탭 고정은 위험하다.

### P1. 판매채널콘텐츠 조회를 정보 전용 lookup으로 승격 검토

현재 no_match 사유 고도화는 지급정산 lookup, 누락 guard, 청구 guard, 콘텐츠마스터를 근거로 한다.

판매채널콘텐츠 조회를 추가하면 다음 구분이 더 선명해진다.

- 같은 채널 판매채널콘텐츠 있음 / 지급정산 없음
- 판매채널콘텐츠 자체 없음
- 콘텐츠마스터는 있으나 판매채널콘텐츠 없음

권장 방향:

- 자동 매칭 후보로 쓰지 않는다.
- `S2_미매핑상세사유`, `S2_미매핑근거`, `S2_권장조치`에만 쓰는 정보 전용 lookup으로 둔다.

리스크:

- 판매채널콘텐츠는 지급정산 가능 상태를 뜻하지 않는다.
- 같은 제목/정제키 중복이 많으면 자동 매칭에 쓰는 순간 오매칭 위험이 커진다.
- 채널 allowlist, 거래처 코드, 콘텐츠형태 필터를 정확히 고정해야 한다.

### P2. 누락 guard summary 강화

현재 summary는 전체 행 수 중심이다.

추가하면 좋은 항목:

- `plfmCd` 요청값
- 판매채널별 row count
- 콘텐츠형태별 row count
- `setlBgnYn` 분포
- 이전 run 대비 added/deleted/modified
- 지급정산 lookup과 누락 lookup의 `판매채널콘텐츠ID` overlap 수

이렇게 하면 “이번 누락 자료가 유통/소설 기준으로 제대로 들어왔는지”를 UI/API 변화 없이도 감리할 수 있다.

## 권고

1. 지금 질문의 `pymt-missing-list` 자료는 “이미 받고 있음”으로 본다.
2. 다만 `유통` 탭 조건은 코드상 명시되어 있지 않으므로 API 파라미터 검증을 해야 한다.
3. 판매채널 콘텐츠 조회는 정식 최신화 후보이지만, 매칭 후보가 아니라 사유/근거 보강용으로만 붙이는 것이 안전하다.
4. 다음 구현을 한다면 순서는 `유통 plfmCd 검증 -> 누락 guard summary 강화 -> 판매채널콘텐츠 정보 lookup 추가`가 좋다.

## 구현 반영

2026-05-12 추가 반영:

- `/stm/stm` guard summary에 `plfmCd`, 판매채널별 건수, 콘텐츠형태별 건수, 정산시작여부 분포, 지급정산 lookup과의 `판매채널콘텐츠ID` overlap을 기록한다.
- `scripts/refresh_s2_reference_guards.py`에 `--compare-platform-code`를 추가했다. 유통 탭의 실제 `plfmCd`가 확인되면 `plfmCd=""` 조회와 비교해 ID 차이를 summary에 남길 수 있다.
- `scripts/refresh_s2_sales_channel_contents.py`를 추가했다. `판매채널콘텐츠 조회` API를 소설 조건(`ctnsStleCd=102`)으로 받아 `data/s2_sales_channel_content_lookup.csv`를 만든다.
- 앱과 daily refresh에서 판매채널콘텐츠 lookup을 읽도록 연결했다.
- 단, 판매채널콘텐츠 lookup은 매칭 후보로 섞지 않는다. `no_match` 행의 `S2_미매핑상세사유`, `S2_미매핑근거`, `S2_권장조치`에만 쓰는 정보 전용 근거다.
