# Socratic S2 Refresh Audit Report

Date: 2026-05-08  
Target: `main` at `99b21d8`  
Scope: S2 최신화 전체 교체, 변경 이력, Streamlit human-facing flow  
Constraint: 코드 수정 없이 보고서만 작성

## Executive Summary

3회 소크라테스 테스트 결과, 현재 구현은 S2 최신화를 "전체 교체"로 강제하고, 기존 누적 병합 경로는 실행 경로에서 차단되어 있다. 실제 S2 무기간 조회도 성공했으며, 로컬 S2 기준 127,565건과 변경 이력 신규 127,565건이 기록됐다.

다만 운영자가 오해할 수 있는 지점은 남아 있다. 첫 전체 교체 실행은 모든 행이 신규로 보이므로 정상적인 대량 신규인지, 위험 신호인지 README와 UI에서 더 명확히 설명하면 좋다. 또한 `판매채널콘텐츠ID`가 빈 186개 원천 행은 S2 기준/변경 감리에서 제외되므로, 이 수치를 운영 체크리스트에 별도 확인 항목으로 올리는 것이 안전하다.

## Test 1. 전체 교체 정책은 정말 우회 불가능한가?

### Socratic Questions

- "전체 교체"라고 말하지만, 기존 로컬 S2 기준과 새 결과가 어디선가 병합될 가능성은 없는가?
- CLI에서 기간 조회나 과거 `rolling-3m` 경로가 살아 있어 부분 교체처럼 동작할 수 있는가?
- 사람이 실수로 `merge_existing=True`를 넣으면 조용히 병합되는가, 아니면 실패하는가?

### Evidence

- `import_payment_settlement_frame()`은 `merge_existing=True`가 들어오면 `ValueError`로 실패한다.
- `refresh_kiss_payment_settlement.py`의 CLI 선택지는 `full-replace`, `initial`, `custom`만 허용한다.
- `resolve_query_window()`는 `rolling-3m`을 명시적으로 거부한다.
- `custom`은 `1900-01-01 ~ today` 보완 조회만 허용한다.
- 테스트 `test_s2_refresh_audit.py`가 stale row 삭제, 신규/삭제/변경 기록, merge 차단을 검증한다.
- 테스트 `test_s2_refresh_window.py`가 부분 범위와 `rolling-3m` 거부를 검증한다.

### Result

Pass. 현재 실행 경로 기준으로 누적 병합/부분 조회 우회는 차단되어 있다.

### Residual Risk

- 함수 시그니처에는 여전히 `merge_existing` 인자가 남아 있다. 호환성 측면에서는 괜찮지만, 새 개발자가 의도를 잘못 읽을 여지는 있다.
- 과거 이력의 `rolling-3m` 표시는 UI에서 `과거 기록(부분 조회)`로 바뀌었지만, 해당 과거 기록이 왜 지금은 금지됐는지 설명은 없다.

### Recommendation

- 다음 리팩터 때 `merge_existing` 인자를 완전히 제거하거나 `deprecated` 주석을 추가한다.
- README에 "과거 부분 조회 이력은 참고용이며 현재 실행 정책은 전체 교체만 허용" 문장을 추가한다.

## Test 2. 변경 이력은 운영자가 믿고 판단할 만큼 충분한가?

### Socratic Questions

- `판매채널콘텐츠ID`가 같은데 콘텐츠명, S2 마스터ID, 콘텐츠ID, 작가정보가 바뀌면 실제로 잡히는가?
- 삭제/신규는 기존 로컬 기준과 새 조회 결과의 차이로 구분되는가?
- 중복 `판매채널콘텐츠ID`가 있는 경우 어떤 1건을 대표로 비교하는지 운영자가 이해할 수 있는가?
- `판매채널콘텐츠ID`가 비어 있는 행은 어떻게 되는가?

### Evidence

- 실제 S2 전체 교체 실행:
  - source rows: 145,059
  - local S2 basis rows: 127,565
  - added: 127,565
  - deleted: 0
  - modified: 0
  - first run이므로 모두 신규 처리된 것은 정상이다.
- 실행 요약:
  - `sales_channel_content_id_nonblank`: 144,873
  - `sales_channel_content_id_unique`: 127,565
  - `sales_channel_content_id_duplicate_keys`: 12,258
  - `sales_channel_content_id_multiple_titles`: 1
  - `sales_channel_content_id_multiple_master_ids`: 12,258
  - `sales_channel_content_id_multiple_content_ids`: 1
- 변경 감리는 `판매채널콘텐츠ID`가 빈 행을 제외하고, 등록일 기준 최신 행을 대표로 삼아 비교한다.

### Result

Mostly pass. 주요 변경은 잡히며, 신규/삭제/변경 분류도 동작한다. 다만 운영 해석에는 주의점이 있다.

### Findings

1. 첫 실행은 모든 S2 기준이 신규로 기록된다.
   - 이번 실행의 `added=127,565`는 정상이다.
   - 하지만 UI에서 이 숫자만 보면 운영자가 대량 신규를 장애로 오해할 수 있다.

2. `판매채널콘텐츠ID`가 빈 원천 행 186개는 S2 기준/변경 감리에서 제외된다.
   - source rows 145,059 중 nonblank ID 144,873이다.
   - 매핑 기준은 ID가 있어야 하므로 제외 자체는 타당하다.
   - 다만 운영자가 "왜 원천 행 수와 S2 기준 행 수가 다른가"를 이해하려면 설명이 필요하다.

3. 중복 ID가 12,258개로 많다.
   - 현재 S2 기준은 등록일 기준 최신 행을 대표로 남긴다.
   - 같은 ID의 복수 마스터ID 자체는 summary에 잡히지만, 변경 이력은 대표 행끼리 비교한다.
   - 운영자가 중복 ID의 전체 후보군을 보려면 별도 상세 리포트가 필요하다.

### Recommendation

- UI 최신화 기록에 `S2 ID 빈 행`, `중복 판매채널콘텐츠ID`, `복수 S2 마스터ID 후보`를 보조 지표로 보여준다.
- 첫 실행 또는 `cache_rows_before=0`인 경우 "초기 생성이라 모든 기준이 신규로 기록됨" 안내를 추가한다.
- 중복 ID 후보 상세 CSV/엑셀 export를 다음 단계 후보로 둔다.

## Test 3. 화면과 문서는 사람 기준으로 충분히 명확한가?

### Socratic Questions

- 업무 사용자가 "지금 뭘 넣고, 어떤 결과를 받아야 하는지" 한 화면에서 이해하는가?
- 자동감지가 파일명 기반이라는 사실을 사전에 알 수 있는가?
- 어댑터 실패가 조용히 지나가지 않고 경고/오류로 드러나는가?
- 내부 용어가 사용자 판단을 흐리지 않는가?

### Evidence

- 앱 상단에 `정산서 업로드 -> 엑셀 파일명 기반 플랫폼 자동감지 -> 어댑터 정규화 -> S2 매핑 -> 결과 엑셀 다운로드` 흐름이 표시된다.
- 파일 업로드 전/후에 자동감지가 엑셀 파일명 기반임을 안내한다.
- 어댑터 실패 시 `입력 파일을 처리하지 못했습니다`, `오류 상세`, 시트별 감사가 표시된다.
- README에 S2 최신화 전/후 체크리스트와 정산서 업로드 실패 시 확인 항목이 추가됐다.
- human-facing 검색상 `Ctrl+C`, `S2 캐시`, `기본 feed` 같은 오해 문구는 발견되지 않았다.

### Result

Pass with minor clarity risks. 핵심 업무 흐름은 human-facing하게 정리되어 있다.

### Residual Risk

- "최근 S2 변경 이력 상세"는 최대 500건만 보여준다. 대량 변경 시 전체 상세가 아니라 일부 샘플처럼 보일 수 있다.
- 최초 실행의 127,565 신규 같은 숫자는 설명 없이는 위협적으로 보인다.
- `S2 마스터ID`라는 표현은 내부 구조를 어느 정도 드러내지만, 현재 요구 범위상 필요한 감리 항목이다.

### Recommendation

- 변경 상세 expander 제목에 `최대 500건 표시`를 명시한다.
- 대량 변경 발생 시 전체 상세 export 버튼을 추가한다.
- README 운영 체크리스트에 "최초 실행은 전체 신규로 표시될 수 있음"을 추가한다.

## Overall Judgment

현재 상태는 main에 유지해도 되는 수준이다. 전체 교체 강제, 변경 이력 기록, UI 노출, README 체크리스트가 모두 구현되어 있으며 실제 S2 full replace도 성공했다.

다음으로 할 일은 기능 수정이 아니라 운영 해석 보강이다. 특히 최초 실행/대량 변경/빈 ID/중복 ID를 운영자가 빠르게 판단할 수 있도록 UI 보조 지표와 상세 export를 추가하는 것이 가장 실용적인 다음 단계다.

## Suggested Next Prompt

```text
mapping-novel main에서 새 브랜치 따고,
S2 최신화 감리 UI를 운영 해석 중심으로 보강해줘.

목표:
1. 최신화 기록에 S2 ID 빈 행 수, 중복 판매채널콘텐츠ID 수, 복수 S2 마스터ID 후보 수를 보조 지표로 노출.
2. cache_rows_before=0인 최초 실행은 "초기 생성이라 전체가 신규로 기록됨"이라고 UI에 안내.
3. 최근 S2 변경 이력 상세 expander에 "최대 500건 표시"를 명시.
4. 변경 상세 전체를 CSV 또는 엑셀로 다운로드할 수 있게 추가.
5. README 운영 체크리스트에 최초 실행/대량 변경/빈 ID/중복 ID 해석법 추가.
6. 테스트 추가 후 전체 테스트 실행.
```
