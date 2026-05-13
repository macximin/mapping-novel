# 매핑 속도 개선 Execution Plan

작성일: 2026-05-13

## 1. 목표

38개 이상의 정산서 파일을 한 번에 처리할 때 체감 실행 시간을 줄인다.

성능 목표:

- 1차 목표: Phase 1 적용 후 5-20% 단축
- 2차 목표: Phase 2 적용 후 누적 25-55% 단축
- 장기 목표: Phase 3 일부 적용 후 누적 45-70% 단축 가능성 검증

품질 목표:

- 매핑 결과가 기존과 달라지지 않아야 한다.
- Cloud 기본 worker 수는 2로 유지한다.
- 사용자는 기존처럼 버튼 한 번으로 처리 상황을 볼 수 있어야 한다.

## 2. 비목표

이번 개선의 첫 단계에서는 다음을 하지 않는다.

- Cloud 기본 worker 수 3 이상 증가
- ProcessPool 도입
- 매핑 규칙의 의미 변경
- S2 기준 데이터 구조 변경
- ClickUp 알림 기능 변경
- 다운로드 파일명 변경

## 3. Phase 0: 계측 추가

### 목적

실제 병목을 숫자로 분리한다.

### 구현 범위

`process_settlement_batch_item()` 내부에 stage timer를 추가한다.

측정 대상:

- `normalize_seconds`
- `s2_filter_seconds`
- `build_mapping_seconds`
- `guard_annotation_seconds`
- `transfer_build_seconds`
- `mapping_export_seconds`
- `transfer_export_seconds`
- `file_total_seconds`

`build_mapping_session_state()` 주변에는 batch-level timer를 추가한다.

측정 대상:

- `summary_frame_seconds`
- `work_order_report_seconds`
- `combined_report_seconds`
- `csv_encode_seconds`
- `zip_build_seconds`
- `postprocess_total_seconds`

### 표시 방식

기본 화면에는 과도하게 노출하지 않고, 성공 summary 또는 debug expander에 stage timing table을 둔다.

### 검증

- 기존 테스트 통과
- 1개 파일 처리 성공
- 38개 파일 처리 시 timing table 생성
- timing 추가 전후 결과 파일 row count 동일

### 예상 효과

직접적인 속도 개선은 거의 없다.

하지만 이후 작업의 우선순위를 결정하는 기준이 된다.

## 4. Phase 1: Low-risk Quick Wins

### 4.1 combined report 재사용

현재 흐름:

1. `build_pd_work_order_report_frame(results)` 내부에서 combined report 생성
2. `build_combined_mapping_report_frame(results)`를 다시 호출

변경 방향:

- combined frame을 한 번만 만든다.
- PD work order report는 이미 만들어진 combined frame을 입력으로 받는다.

예상 효과:

- 대량 row batch에서 후처리 시간 감소
- memory copy 감소

### 4.2 CSV bytes 재사용

현재 흐름:

- 다운로드용 CSV bytes를 만든다.
- ZIP 생성 시 CSV를 다시 `.to_csv()` 한다.

변경 방향:

- work order CSV bytes와 combined CSV bytes를 한 번만 만든다.
- ZIP 생성 함수에 이미 만들어진 bytes를 전달한다.

예상 효과:

- CSV encoding 중복 제거
- ZIP 생성 단계 단축

### 4.3 XLSX ZIP 재압축 회피

현재 흐름:

- ZIP 전체가 `ZIP_DEFLATED`로 생성된다.
- 이미 압축 포맷인 `.xlsx`도 다시 압축한다.

변경 방향:

- CSV/TXT는 deflate 유지
- XLSX entry는 `ZIP_STORED` 사용

예상 효과:

- 38개 파일처럼 XLSX가 많은 batch에서 ZIP build 시간 감소

### 4.4 normalization summary copy 축소

현재 흐름:

- `summarize_normalization()`이 `len(result.default_feed_rows)`를 호출한다.
- 이 속성이 DataFrame copy를 만들 수 있다.

변경 방향:

- 가능한 mask/count 기반으로 default feed row 수를 계산한다.

예상 효과:

- 소폭 개선
- 큰 정산서에서 불필요한 DataFrame copy 감소

### 4.5 후처리 progress 추가

현재 문제:

- 파일 처리 완료 후 report/ZIP 생성 중에는 사용자가 멈춘 것으로 느낄 수 있다.

변경 방향:

- "보고서 생성 중"
- "ZIP 생성 중"
- "다운로드 준비 중"

같은 post-processing 상태를 표시한다.

### Phase 1 검증

- `git diff --check`
- 기존 pytest
- 1개 파일 수동 처리
- 38개 파일 처리 전후 timing 비교
- ZIP 내부 파일명 비교

### Phase 1 예상 효과

전체 기준 5-20% 단축.

파일이 작고 후처리 row가 적으면 5%에 가까울 수 있다.

파일이 많고 XLSX/CSV/ZIP이 큰 경우 15-20%까지 기대할 수 있다.

## 5. Phase 2: S2/Guard Batch Context

### 목적

파일마다 반복하는 S2/guard 전처리를 run 단위 1회로 줄인다.

### 설계

`S2RuntimeContext` 또는 유사한 context를 둔다.

보관 후보:

- S2 기준 frame shape/hash
- clean title column
- normalized sales channel column
- parsed registration/payment date column
- channel별 filtered frame 또는 candidate index
- missing title index
- billing/service lookup index
- payment key index
- master content index

### 적용 위치

- `filter_s2_by_sales_channel()`
- `build_mapping()`
- `annotate_mapping_result()`

단, 기존 public 함수는 유지하고 context optional parameter로 시작하는 것이 안전하다.

### 정합성 테스트

필수:

- context 미사용 결과와 context 사용 결과가 동일해야 한다.
- duplicate clean key에서 선택된 S2 row가 동일해야 한다.
- blank/invalid date가 섞인 후보군 fallback order가 동일해야 한다.
- guard annotation 컬럼이 동일해야 한다.
- 여러 파일을 한 context로 처리해도 파일 간 오염이 없어야 한다.

### Phase 2 예상 효과

전체 기준 누적 25-55% 단축.

계산 근거:

- 합성 측정상 `build_mapping + annotate_mapping`이 파일당 약 15.5초였다.
- 38개 파일이면 serial 기준 약 589초, worker 2 기준 단순 추산 약 294초 규모다.
- 이 구간을 40-70% 줄이면 약 2-3분 단위 절감 가능성이 있다.

## 6. Phase 3: Excel I/O 및 Artifact 구조 개선

### 6.1 export width sampling

현재는 전체 worksheet cell을 훑어 열 너비를 잡는다.

변경 방향:

- header + 상위 N행 + 일부 긴 텍스트 후보만 사용한다.
- 필요하면 `MAPPING_EXPORT_WIDTH_SAMPLE_ROWS` env로 조정한다.

예상 효과:

- 대형 결과 파일 export에서 10-30% 구간 절감 가능

### 6.2 workbook streaming

변경 방향:

- merged cell 확장이 필요 없는 플랫폼부터 `read_only=True` 경로를 연다.
- merged cell 의존 플랫폼은 기존 경로를 유지한다.

필수 조건:

- 플랫폼별 fixture row count 동일
- sheet audit 동일
- 헤더 인식 동일

예상 효과:

- 대형 원본 Excel load에서 큰 개선 가능
- 단, 위험이 있어 마지막 단계 권장

### 6.3 lazy artifact generation

변경 방향:

- 매핑 계산과 XLSX/ZIP 생성 시점을 분리한다.
- 사용자가 다운로드를 누를 때 artifact를 만든다.

장점:

- 최초 매핑 완료까지의 체감 시간이 크게 줄 수 있다.
- session state peak memory를 줄일 여지가 있다.

위험:

- 다운로드 시점 오류 처리 필요
- Streamlit rerun state 관리 필요
- 사용자가 기존 UX와 다르게 느낄 수 있음

## 7. Rollback 전략

각 변경은 feature flag 또는 optional path로 되돌릴 수 있게 한다.

권장 flag:

- `MAPPING_FAST_REPORTS=1`
- `MAPPING_FAST_ZIP=1`
- `MAPPING_S2_CONTEXT=1`
- `MAPPING_EXPORT_WIDTH_SAMPLE_ROWS=500`
- `MAPPING_WORKBOOK_STREAMING=0`

처음에는 Phase 1만 기본 활성화한다.

Phase 2는 테스트 통과 후 기본 활성화한다.

Phase 3은 계측 결과가 필요성을 보여줄 때 선택 활성화한다.

## 8. Definition of Done

Phase별 완료 기준은 다음과 같다.

Phase 0 완료:

- stage timing이 파일별로 기록된다.
- 38개 파일 batch에서 postprocess timing까지 확인된다.

Phase 1 완료:

- 기존 매핑 결과와 다운로드 결과가 동일하다.
- ZIP entry 이름과 개수가 동일하다.
- 동일 입력에서 전체 시간이 최소 5% 이상 줄거나, 후처리 시간이 유의미하게 줄어든다.

Phase 2 완료:

- context on/off 결과가 fixture와 실제 sample에서 동일하다.
- 동일 입력에서 전체 시간이 최소 25% 이상 줄어드는지 확인한다.

Phase 3 완료:

- 플랫폼별 fixture가 통과한다.
- export 또는 workbook load stage가 유의미하게 줄어든다.
- Cloud에서 memory/timeout 문제가 증가하지 않는다.

## 9. 실행 순서

권장 commit 순서:

1. timing instrumentation
2. report/CSV/ZIP quick wins
3. 38개 파일 baseline vs quick-win 측정 결과 문서화
4. S2/guard context 구현
5. context on/off golden comparison
6. export width sampling
7. workbook streaming 후보 플랫폼 실험
8. lazy artifact 검토

## 10. 보고 방식

각 실행 후 다음 형식으로 기록한다.

| 항목 | Before | After | 개선 |
| --- | ---: | ---: | ---: |
| 전체 시간 |  |  |  |
| normalize |  |  |  |
| build mapping |  |  |  |
| guard annotation |  |  |  |
| export |  |  |  |
| report/ZIP |  |  |  |

숫자가 나오기 전의 현재 판단은 다음과 같다.

- 낮은 위험으로 바로 할 만한 개선: Phase 1
- 가장 큰 성능 잠재력: Phase 2
- 가장 큰 체감 변화 가능성: lazy artifact
- 가장 큰 정합성 위험: workbook streaming

