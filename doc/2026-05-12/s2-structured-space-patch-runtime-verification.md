# S2 구조형 제목/공백 회차 패치 실제 배치 검증

작성일: 2026-05-12

## 처리한 묶음

재감리에서 `로직 후보`로 분리된 50그룹 / 698행을 처리했다.

패치 대상:

1. 공백 있는 회차 표기
   - 예: `평화로운 먼치킨 영지 5 화`
   - 기존 정제키: `평화로운먼치킨영지화`
   - 신규 정제키: `평화로운먼치킨영지`
2. S2 구조형 제목
   - 예: `0_야한규칙으로 다 따먹음_파워레인젖_일반`
   - 신규 정제키: `야한규칙으로다따먹음`

수동확인 후보인 `이야기/의이야기` 제거는 처리하지 않았다.

## 검증 방식

2026-02 SSOT 38개 파일을 앱 핵심 처리 경로로 다시 처리했다.

처리 경로:

1. `normalize_settlement`
2. S2 판매채널 자동 감지
3. `filter_s2_by_sales_channel`
4. `build_mapping`
5. `annotate_mapping_result`
6. 종합 CSV 생성

## 결과

1차 좁은 패치 이후 결과와 비교했다.

| 항목 | 1차 패치 후 | 이번 패치 후 | 증감 |
| --- | ---: | ---: | ---: |
| 전체 행 | 82,957 | 82,957 | 0 |
| S2 matched | 76,003 | 76,701 | +698 |
| S2 no_match | 6,954 | 6,256 | -698 |
| 검토필요 행 | 6,954 | 6,256 | -698 |

상태가 바뀐 행은 698행이며, 전부 `no_match -> matched` 방향이다.

## 영향 채널

| 판매채널 | 개선 행 |
| --- | ---: |
| 원스토어(소설) | 203 |
| 한아름(P000003716) | 157 |
| 리디북스(소설) | 154 |
| 판무림 | 88 |
| Yes24(서점) | 17 |
| 알라딘(소설)(1068659313) | 16 |
| 에피루스 이북클럽(B2C) | 12 |
| 북큐브(소설) | 11 |
| 교보문고(소설) | 10 |
| 기타 채널 | 30 |

## 누적 효과

초기 고도화 엑셀 기준:

| 항목 | 초기 | 현재 | 증감 |
| --- | ---: | ---: | ---: |
| S2 matched | 75,095 | 76,701 | +1,606 |
| 검토필요 행 | 7,862 | 6,256 | -1,606 |

## 남은 대상

이번 묶음 처리 후 남은 검토필요는:

- 행: 6,256
- `판매채널 + 정제키` 그룹: 496

이전 재감리 기준으로 보면 남은 주요 영역은 다음이다.

- S2 지급정산 없음 유력
- 해당채널 지급정산 없음 유력
- 수동확인 후보
- 다른채널 근접후보만 있음

## 산출물

실제 배치 결과:

- `igignore/2026-02_patch_verify_after_structured_cleaning/batch_summary_after_structured_patch_20260512_114230.csv`
- `igignore/2026-02_patch_verify_after_structured_cleaning/전체_행별매핑_종합_after_structured_patch_20260512_114230.csv`
- `igignore/2026-02_patch_verify_after_structured_cleaning/PD_작업지시_종합리포트_after_structured_patch_20260512_114230.csv`
- `igignore/2026-02_patch_verify_after_structured_cleaning/mapping_reports_after_structured_patch_20260512_114230.zip`

영향 분석:

- `doc/2026-05-12/s2_structured_space_patch_changed_rows.csv`
- `doc/2026-05-12/s2_structured_space_patch_delta_summary.csv`
- `doc/2026-05-12/s2_structured_space_patch_channel_delta.csv`

## 테스트

- `python -m pytest`
- 결과: 91 passed

## 판정

이번 묶음은 운영 경로 기준에서도 의도한 범위로 처리됐다.

- 처리 실패 파일 없음
- 기존 matched 하락 없음
- 개선 행 698행 전부 `no_match -> matched`
