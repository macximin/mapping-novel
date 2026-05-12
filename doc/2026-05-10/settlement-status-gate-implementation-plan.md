# 정산정보 없음 게이트 시스템 구현 계획

작성일: 2026-05-10

## 결론

정산정보가 없는 판매채널콘텐츠ID 문제는 제목 클리닝이나 `[정산정보없음]` suffix rename으로 풀지 않는다.

시스템에는 `판매채널콘텐츠ID` 단위 정산상태 게이트를 추가한다. 이 게이트는 정산서 제목 매칭 전에 S2 후보군을 검사하고, 지급정산관리 기준으로 정산 전송에 사용할 수 없는 ID를 후보에서 제외한다.

이 기능은 고정 차단목록이 아니다. 최신 S2 지급정산관리와 판매채널콘텐츠를 다시 수집해 매번 재계산하는 동적 정산상태 게이트다. 지급정산정보가 사후 생성된 판매채널콘텐츠ID는 다음 갱신 때 자동으로 차단 해제되어야 한다.

핵심 원칙은 다음과 같다.

- 정상 매핑 후보의 기준은 `지급정산관리`에 존재하는 판매채널콘텐츠ID다.
- `판매채널콘텐츠` 목록은 서비스 가능 채널 증거이지, 정산 전송 가능 증거가 아니다.
- 같은 콘텐츠ID 안에 정상 채널과 정산정보 없는 채널이 섞인 경우, 콘텐츠명 suffix는 정상 채널까지 오염시킬 수 있으므로 금지한다.
- 애매한 케이스는 콘텐츠명 rename이 아니라 판매채널콘텐츠ID 단위 체크리스트/차단표로 관리한다.
- 이미 `[정산정보없음]` 등으로 rename 완료된 건은 되돌리지 않는다. 해당 작업은 당시 확실한 대상만 처리한 것으로 보고, 이번 개선은 추가 rename을 막고 동적 게이트를 붙이는 범위로 제한한다.
- 제목 클리닝 고도화는 정산 가능 후보만 남긴 뒤 적용한다.

## 현재 확인 숫자

숫자는 두 층으로 분리한다.

1. 전체 최신 S2 대조 숫자

- 판매채널콘텐츠 행: 98,113개
- 판매채널콘텐츠에는 있으나 지급정산관리에는 없는 전체 판매채널콘텐츠ID: 4,710개
- 이 중 `[사용안함]`/`[사용금지]` 등으로 선차단되는 C급: 3,337개
- 사용안함/사용금지 제외 후 A/B 체크리스트: 1,373개
- 동일 콘텐츠ID 안에 정상 정산 채널은 있지만 특정 판매채널콘텐츠ID만 정산정보가 없는 혼합 위험: 351개

2. 확정 소설 담당부서 범위

확정 담당부서:

- `소설1팀`
- `소설2팀`
- `소설편집팀`
- `소설유통팀`
- `소설사업부`

확정 범위 최신 숫자:

- 판매채널콘텐츠 행: 83,233개
- A/B 체크리스트 대상: 381개
- 위 381개가 속한 콘텐츠ID: 203개
- 혼합 위험 판매채널콘텐츠ID: 261개
- 위 261개가 속한 콘텐츠ID: 102개

부서별 A/B 체크리스트:

- `소설편집팀`: 323개
- `소설유통팀`: 46개
- `소설2팀`: 12개
- `소설1팀`/`소설사업부`: 0개

부서별 혼합 위험:

- `소설편집팀`: 203개
- `소설유통팀`: 46개
- `소설2팀`: 12개
- `소설1팀`/`소설사업부`: 0개

주의: 기존 문의 문구의 `정산정보가 없는 판매채널콘텐츠ID 후보 2,556개`는 이전 산출 기준 숫자로 남아 있을 수 있다. 시스템 구현 기준 숫자는 매번 최신 S2 재수집 산출물에서 재생성한다. 하드코딩하지 않는다.

## 추가 Survey

조사 기준 파일:

- `app.py`
- `mapping_core.py`
- `s2_transfer.py`
- `kiss_payment_settlement.py`
- `cleaning_rules.py`
- `scripts/audit_sales_channel_settlement_gap.py`
- `doc/2026-05-09/title-cleaning-and-settlement-mapping-plan.md`
- `판매채널-지급정산 불일치/05_불일치_판정/20260509/판매채널콘텐츠_vs_지급정산관리_전체판정.csv`
- `판매채널-지급정산 불일치/05_불일치_판정/20260509/novel_core_dept_scope_ab_checklist.csv`
- `판매채널-지급정산 불일치/05_불일치_판정/20260509/novel_core_dept_scope_mixed.csv`

현재 흐름:

1. `app.py`에서 정산서를 업로드한다.
2. `load_selected_s2_basis()`가 로컬 S2 지급정산관리 lookup 또는 수동 업로드 S2 기준을 읽는다.
3. `process_settlement_batch_item()`이 파일별 S2 판매채널을 감지하고 `filter_s2_by_sales_channel()`로 S2 후보를 채널 단위로 좁힌다.
4. `mapping_core.build_mapping()`이 S2 후보와 정산서 제목을 정제키로 매칭한다.
5. `s2_transfer.build_s2_transfer()`가 matched가 아닌 행, 판매채널콘텐츠ID 공란, 금액 공란 등을 차단한다.

이미 있는 보호 장치:

- `cleaning_rules.drop_disabled_rows()`는 `[사용안함]`, `[사용금지]`, `[정산정보없음]` 류 표식 행을 제거할 수 있다.
- `kiss_payment_settlement.to_s2_lookup()`은 지급정산관리 데이터를 `판매채널콘텐츠ID` 기준으로 dedupe해 S2 lookup을 만든다.
- `scripts/audit_sales_channel_settlement_gap.py`는 판매채널콘텐츠와 지급정산관리를 대조해 `OK`, `A`, `B`, `C` 판정 파일을 만든다.
- `s2_transfer.build_s2_transfer()`는 최종 전송 직전에 `S2_매칭상태 != matched`를 차단한다.

현재 빈틈:

- `audit_sales_channel_settlement_gap.py`의 판정 결과는 운영 CSV로만 존재하고, 앱의 런타임 매핑 후보 생성에는 아직 직접 연결되어 있지 않다.
- 현재 로컬 지급정산관리 lookup만 쓰면 정산정보 없는 ID가 후보에 안 올라올 가능성이 높지만, 수동 S2 파일이나 향후 판매채널콘텐츠 기반 S2 소스가 들어오면 문제 ID가 다시 후보가 될 수 있다.
- `[정산정보없음]`을 제목에 붙이는 방식은 콘텐츠명 단위 처리가 되므로 혼합 위험 351개/소설 확정 범위 261개를 안전하게 처리하지 못한다.
- `s2_transfer`는 매칭상태와 금액은 보지만, 정산상태 게이트 컬럼이 누락된 결과물을 별도로 검증하지 않는다.

## 목표 아키텍처

추가할 핵심 모듈:

- `settlement_status_gate.py`

역할:

1. 최신 대조 판정 CSV를 읽는다.
2. 판매채널콘텐츠ID별 정산상태를 만든다.
3. S2 후보 DataFrame에 정산상태를 merge한다.
4. 매핑에 사용할 수 있는 후보와 보류/차단 후보를 분리한다.
5. 앱과 export에 사람이 볼 수 있는 요약과 차단 사유를 제공한다.

권장 상태값:

- `OK_PAYMENT_SETTLEMENT_EXISTS`
  - 지급정산관리에 존재한다.
  - 자동 매핑 후보로 사용 가능하다.
- `BLOCK_DISABLED_MARKER`
  - `[사용안함]`, `[사용금지]` 등 선차단 표식이 있다.
  - 매핑 후보에서 제외한다.
- `HOLD_NO_PAYMENT_SETTLEMENT`
  - 판매채널콘텐츠에는 있으나 지급정산관리에 없다.
  - 자동 매핑 후보에서 제외한다.
- `HOLD_MIXED_CONTENT_RISK`
  - 같은 콘텐츠ID 안에 정상 지급정산 채널은 있지만 이 판매채널콘텐츠ID만 정산정보가 없다.
  - 콘텐츠명 suffix 처리 금지, 판매채널콘텐츠ID 단위로 개발팀 답변 대기.
- `OK_PAYMENT_LOOKUP_SOURCE`
  - 현재 S2 후보 소스가 지급정산관리 lookup이고, 해당 ID가 lookup에 존재한다.
  - 판정 CSV에 없어도 정산 가능 후보로 본다.
- `REVIEW_UNKNOWN_STATUS`
  - 지급정산관리 lookup도 아니고 최신 판정표에서도 상태를 확인할 수 없다.
  - 기본은 자동 매핑 제외다. 단, 운영자가 명시적으로 우회 모드를 켠 경우에만 후보로 남긴다.

중요한 설계 선택:

- A/B 구분은 UI 필터와 감리 보조 정보로만 쓴다.
- 정산 가능 여부 판정은 `지급정산관리_존재`, `사용안함_사용금지_표식`, `콘텐츠ID 내 정상 채널 혼재 여부`로 재계산한다.
- 정산서 샘플 등장 여부는 우선순위 정보일 뿐 게이트 조건이 아니다.
- 숫자는 CSV에서 계산하고 문서나 코드에 박지 않는다.

## 런타임 처리 순서

새 처리 순서:

1. 정산서 업로드
2. S2 기준 로드
3. 파일명 또는 수동 선택으로 S2 판매채널 결정
4. S2 후보를 판매채널로 필터링
5. `settlement_status_gate` 적용
6. `OK_*` 후보만 `mapping_core.build_mapping()`에 전달
7. 제목 정제키 매칭
8. duplicate/ambiguous/no_match 감리
9. `s2_transfer.build_s2_transfer()`에서 정산상태 컬럼 재검증
10. 결과 workbook, 차단표, 점검표 다운로드

게이트 적용 후 산출물:

- `s2_candidates_allowed`
  - 매핑에 실제 투입되는 S2 후보
- `s2_candidates_blocked`
  - 판매채널콘텐츠ID 단위 차단/보류 후보
- `settlement_status_summary`
  - 상태별 건수, 플랫폼별 건수, 부서별 건수
- `blocked_title_hits`
  - 이번 정산서 제목 정제키와 보류 후보 제목 정제키가 만난 건
- `only_blocked_candidate_alerts`
  - 이번 정산서 제목 정제키 기준으로 정상 후보는 0개이고 보류/차단 후보만 존재하는 건

`blocked_title_hits`가 필요한 이유:

- 단순히 후보에서 제거하면 운영자는 왜 no_match가 생겼는지 모른다.
- 같은 제목이 보류 ID에 있었는지 보여줘야 “매칭 실패”가 아니라 “정산정보 없음으로 보류”임을 이해할 수 있다.

`only_blocked_candidate_alerts`가 필요한 이유:

- 어떤 정산서 행은 정제키 기준으로 S2 후보가 존재하지만, 그 후보가 전부 정산정보 없음/HOLD일 수 있다.
- 이 경우 결과를 단순 `no_match`로 보여주면 사용자는 제목 클리닝 실패로 오해한다.
- 따라서 `정상 후보 0개 + 차단 후보 1개 이상`이면 별도 경보를 띄운다.
- 경보 문구는 “정산서 제목과 일치하는 S2 후보가 있으나 정산정보 없음 게이트로 전부 보류됨”으로 둔다.

## 앱 UI 계획

기존 메인 처리 화면에는 최소 변경만 넣는다.

1. S2 기준 영역

- 최신 정산상태 판정표 로드 여부 표시
- 판정표 기준일 표시
- 상태표 행 수 표시
- 게이트 모드 표시

게이트 모드:

- `strict`
  - 기본값.
  - 확인 불가/정산정보 없음/혼합 위험 후보는 자동 매핑 제외.
- `payment_lookup_safe`
  - 지급정산관리 lookup 소스일 때 기본값처럼 동작.
  - lookup에 있는 ID는 OK로 본다.
- `audit_only`
  - 후보는 유지하되 상태 컬럼과 차단 경고만 붙인다.
  - 개발/검증용. 운영 기본값으로 쓰지 않는다.

2. 처리 결과 화면

기존 탭:

- `검토필요`
- `중복후보`
- `입력검증`

추가 탭:

- `정산상태게이트`

표시 항목:

- 상태별 후보 수
- 보류/차단 후보 상세
- 이번 정산서 제목과 만난 보류 후보
- 정상 후보 없이 보류/차단 후보만 존재하는 경보
- CSV 다운로드

3. 별도 점검 탭

앱 상단 또는 사이드바에 `정산정보 없음 점검` 화면을 만든다.

필터:

- 기준일
- 플랫폼
- 판매채널명
- 담당부서
- 판정등급
- 혼합 위험 여부
- 콘텐츠ID
- 판매채널콘텐츠ID

다운로드:

- 전체 A/B 체크리스트
- 혼합 위험만
- 확정 소설 담당부서 범위
- Jira 문의용 요약 텍스트

## 데이터 산출물 계획

입력:

- `data/kiss_payment_settlement_s2_lookup.csv`
- `판매채널-지급정산 불일치/05_불일치_판정/{YYYYMMDD}/판매채널콘텐츠_vs_지급정산관리_전체판정.csv`
- `data/kidari_contents.xlsx`

생성/사용:

- `판매채널-지급정산 불일치/05_불일치_판정/{YYYYMMDD}/novel_core_dept_scope_ab_checklist.csv`
- `판매채널-지급정산 불일치/05_불일치_판정/{YYYYMMDD}/novel_core_dept_scope_mixed.csv`
- `판매채널-지급정산 불일치/05_불일치_판정/{YYYYMMDD}/novel_core_dept_scope_summary.json`
- 새 runtime cache 후보: `data/settlement_status_gate_latest.csv`

`data/settlement_status_gate_latest.csv` 권장 컬럼:

- `판매채널콘텐츠ID`
- `콘텐츠ID`
- `콘텐츠명`
- `플랫폼`
- `S2판매채널명`
- `담당부서`
- `지급정산관리_존재`
- `사용안함_사용금지_표식`
- `혼합위험`
- `정산상태`
- `정산상태사유`
- `판정기준일`

## 구현 단계

### 1단계: 상태표 생성 스크립트

새 스크립트:

- `scripts/build_settlement_status_gate.py`

기능:

- 최신 판정 CSV 자동 탐색 또는 `--judgement-csv` 입력
- IPS 담당부서 join
- 확정 소설 담당부서 필터 옵션
- 상태값 재계산
- `data/settlement_status_gate_latest.csv` 출력
- 요약 JSON 출력

완료 기준:

- 전체 최신 숫자 4,710/1,373/351 재현
- 확정 소설 담당부서 숫자 381/261 재현
- `소설1팀`, `소설사업부` 0건도 누락이 아니라 0건으로 명시

### 2단계: 게이트 모듈

새 모듈:

- `settlement_status_gate.py`

권장 함수:

- `load_settlement_status_table(path) -> pd.DataFrame`
- `build_status_table(judgement, ips=None, departments=None) -> pd.DataFrame`
- `apply_settlement_status_gate(s2_df, status_table, source_kind, mode="strict") -> SettlementGateResult`
- `find_blocked_title_hits(settlement_df, blocked_s2_df) -> pd.DataFrame`
- `find_only_blocked_candidate_alerts(settlement_df, allowed_s2_df, blocked_s2_df) -> pd.DataFrame`

`SettlementGateResult` 필드:

- `allowed`
- `blocked`
- `summary`
- `warnings`
- `mode`
- `source_kind`

완료 기준:

- 지급정산관리 lookup 소스는 기존 정상 매핑률을 깨지 않는다.
- 판매채널콘텐츠 소스 또는 수동 S2 파일에 정산정보 없는 ID가 들어오면 후보에서 제외된다.
- 혼합 위험 ID는 제목이 같아도 자동 매핑되지 않는다.

### 3단계: 매핑 엔진 연결

변경 위치:

- `app.py`의 `process_settlement_batch_item()`
- 필요 시 `mapping_core.build_mapping()` 인자 확장

최소 변경안:

- `process_settlement_batch_item()`에서 `filter_s2_by_sales_channel()` 직후 게이트 적용
- `mapping_core.build_mapping()`에는 `allowed`만 전달
- 게이트 summary와 blocked rows는 `mapping.input_validation` 또는 result dict에 붙여 UI에서 보여준다.

보강안:

- `mapping_core.build_mapping()`에 `s2_status_columns`를 보존하는 옵션 추가
- 최종 `mapping.rows`에 `S2_정산상태`, `S2_정산상태사유`를 포함
- `s2_transfer`가 이 컬럼을 다시 검증

권장: 보강안까지 간다. 전송자료 단계에서 한 번 더 막아야 한다.

### 4단계: S2 전송자료 이중 잠금

변경 위치:

- `s2_transfer.py`

추가 검증:

- `S2_정산상태` 컬럼이 있으면 `OK_*` 외 상태 차단
- 게이트가 활성화된 처리에서 `S2_정산상태` 컬럼이 없으면 차단
- `S2_판매채널콘텐츠ID`가 status allowlist에 없으면 차단

완료 기준:

- 누군가 매핑 결과를 중간에 수정해도 정산정보 없는 ID가 S2 전송자료로 나가지 않는다.

### 5단계: 점검 UI

변경 위치:

- `app.py`

추가:

- `정산정보 없음 점검` 섹션
- 상태별 metric
- 부서/플랫폼 필터
- 혼합 위험 다운로드
- Jira 요약문 생성

완료 기준:

- 사용자가 앱에서 381개/261개 산출물을 확인하고 다운로드할 수 있다.
- 정산서 처리 결과에서 no_match와 정산정보 없음 보류를 구분할 수 있다.

### 6단계: 제목 클리닝 고도화로 이동

정산상태 게이트가 먼저다.

그 다음에 제목 클리닝을 고도화한다.

클리닝 고도화 원칙:

- 정산 불가능 후보를 제거한 뒤 정상 후보끼리만 정제키를 비교한다.
- `외전`, `개정판`, `완전판`, `19세`, `세트`, `단행본`, `연재` 같은 보호 토큰은 무리하게 삭제하지 않는다.
- clean key 충돌은 자동 확정하지 않는다.
- 작품별 alias는 별도 테이블로 관리한다.

## 테스트 계획

신규 테스트:

- `tests/test_settlement_status_gate.py`

테스트 케이스:

1. 지급정산관리 존재 ID는 `OK_PAYMENT_SETTLEMENT_EXISTS`
2. 사용안함/사용금지 표식은 `BLOCK_DISABLED_MARKER`
3. 지급정산관리 없음은 `HOLD_NO_PAYMENT_SETTLEMENT`
4. 같은 콘텐츠ID 안에 정상 채널과 누락 채널이 섞이면 `HOLD_MIXED_CONTENT_RISK`
5. 확정 소설 담당부서 필터가 `소설1팀`, `소설2팀`, `소설편집팀`, `소설유통팀`, `소설사업부`만 남긴다.
6. 게이트 적용 후 HOLD ID는 `build_mapping()` 후보로 들어가지 않는다.
7. `s2_transfer`는 `S2_정산상태` 누락 또는 HOLD 상태를 차단한다.
8. 정제키 기준 정상 후보가 0개이고 HOLD/BLOCK 후보만 있으면 `only_blocked_candidate_alerts`에 잡힌다.

회귀 테스트:

- `tests/test_mapping_core.py`
- `tests/test_s2_transfer.py`
- `tests/test_kiss_payment_settlement.py`
- `tests/test_cleaning_rules.py`

실데이터 검증:

- 최신 판정 CSV로 4,710/1,373/351 재현
- 확정 소설 담당부서 CSV로 381/261 재현
- 원스토어 9,999행은 실제 개수로 취급하고 서버 상한 경고를 되살리지 않는다.

## 적대적 감리 1회차: 과차단 위험

공격 질문:

> 지급정산관리 lookup만 쓰면 이미 정상 ID만 있는데, 게이트를 또 넣으면 정상 매핑을 괜히 망치는 것 아닌가?

판정:

- 맞는 우려다.
- 그래서 `source_kind`가 필요하다.
- `payment_lookup` 소스에서는 lookup에 존재하는 ID를 `OK_PAYMENT_LOOKUP_SOURCE`로 인정한다.
- 판정 CSV에 없다는 이유만으로 지급정산관리 lookup 행을 막으면 안 된다.

반영:

- 상태값에 `OK_PAYMENT_LOOKUP_SOURCE` 추가.
- `REVIEW_UNKNOWN_STATUS`는 지급정산관리 lookup이 아닌 소스에서만 기본 차단.
- 게이트 모드에 `payment_lookup_safe` 추가.

## 적대적 감리 2회차: 숫자 혼선 위험

공격 질문:

> 문서에는 2,556, 4,710, 1,373, 351, 381, 261이 섞여 있다. 운영자가 어느 숫자를 믿어야 하나?

판정:

- 숫자가 섞이면 Jira와 시스템 판단이 서로 다른 말을 하게 된다.
- 숫자는 용도별로 분리해야 한다.

반영:

- 전체 최신 S2 대조 숫자와 확정 소설 담당부서 숫자를 분리했다.
- 기존 2,556은 이전 문의 문구 숫자로만 취급하고, 구현 기준으로 쓰지 않는다.
- 구현 완료 기준은 최신 산출물 재현성으로 둔다.

## 적대적 감리 3회차: 후보 제거 후 설명력 상실 위험

공격 질문:

> HOLD 후보를 매핑 전에 빼버리면, 사용자는 왜 no_match가 생겼는지 모른다. 그냥 매칭 실패로 보이면 운영이 더 어려워지는 것 아닌가?

판정:

- 맞다.
- 후보에서 제거하는 것만으로는 부족하다.
- 정산서 제목과 HOLD 후보의 정제키가 만나는 경우를 별도 표로 보여줘야 한다.

반영:

- `blocked_title_hits` 산출물을 추가했다.
- `only_blocked_candidate_alerts` 산출물을 추가했다.
- UI에 `정산상태게이트` 탭을 추가한다.
- 결과 workbook 또는 ZIP에 보류/차단 후보 상세를 포함한다.
- `s2_transfer`에서 한 번 더 정산상태를 검사한다.

## 최종 실행 순서

1. `scripts/build_settlement_status_gate.py` 구현
2. `settlement_status_gate.py` 구현
3. 단위 테스트 추가
4. `app.py`에 게이트 연결
5. `s2_transfer.py`에 이중 잠금 추가
6. `정산정보 없음 점검` UI 추가
7. 실데이터로 381/261 재현 확인
8. 제목 클리닝 고도화 착수

## 완료 기준

- 정산정보 없는 판매채널콘텐츠ID는 제목 매칭 후보에 들어가지 않는다.
- 혼합 위험 케이스는 콘텐츠명 suffix 없이 판매채널콘텐츠ID 단위로 보류된다.
- 지급정산관리 lookup 기반 기존 정상 매핑률은 유지된다.
- 보류 때문에 발생한 no_match는 `정산상태게이트` 탭에서 설명된다.
- 정상 후보 없이 보류 후보만 있는 행은 일반 no_match가 아니라 경보로 표시된다.
- S2 전송자료 export 단계에서 정산정보 없음 ID가 다시 차단된다.
- 확정 소설 담당부서 기준 381개/261개 산출물을 앱 또는 스크립트에서 재현할 수 있다.
