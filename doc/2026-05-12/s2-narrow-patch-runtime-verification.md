# S2 좁은 클리닝 패치 실제 배치 검증

작성일: 2026-05-12

## 검증 목적

함수 단위 영향 분석에 이어, 앱에서 사용하는 핵심 처리 경로로 2026-02 SSOT 38개 파일을 실제 재처리했다.

처리 경로:

1. `normalize_settlement`
2. S2 판매채널 자동 감지
3. `filter_s2_by_sales_channel`
4. `build_mapping`
5. `annotate_mapping_result`
6. 종합 CSV 생성

## 입력

- 입력 폴더: `igignore/2026-02_app_upload_확정_2`
- 대상 파일: `2026-02__*__SSOT.xlsx` 38개
- S2 기준: `data/kiss_payment_settlement_s2_lookup.csv`
- 누락/청구 보조 기준:
  - `data/s2_payment_missing_lookup.csv`
  - `data/s2_billing_settlement_lookup.csv`

## 결과

전체 38개 파일 모두 성공 처리됐다.

| 항목 | 기존 고도화 엑셀 | 패치 후 실제 배치 | 증감 |
| --- | ---: | ---: | ---: |
| 전체 행 | 82,957 | 82,957 | 0 |
| S2 matched | 75,095 | 76,003 | +908 |
| S2 no_match | 7,861 | 6,954 | -907 |
| S2 blank_key | 1 | 0 | -1 |
| 검토필요 행 | 7,862 | 6,954 | -908 |

## 영향 채널

| 판매채널 | matched 증감 | 검토필요 증감 |
| --- | ---: | ---: |
| 리디북스(소설) | +900 | -900 |
| 구글(소설) | +7 | -7 |
| 문피아 | +1 | -1 |

그 외 채널은 매칭/검토필요 수 변화가 없다.

## 산출물

실제 배치 검증 결과:

- `igignore/2026-02_patch_verify_after_narrow_cleaning/batch_summary_after_patch_20260512_112205.csv`
- `igignore/2026-02_patch_verify_after_narrow_cleaning/전체_행별매핑_종합_after_patch_20260512_112205.csv`
- `igignore/2026-02_patch_verify_after_narrow_cleaning/PD_작업지시_종합리포트_after_patch_20260512_112205.csv`
- `igignore/2026-02_patch_verify_after_narrow_cleaning/mapping_reports_after_patch_20260512_112205.zip`
- `igignore/2026-02_patch_verify_after_narrow_cleaning/channel_delta_vs_text_cleaning.xlsx`

## 판정

운영 경로 기준에서도 패치 효과는 의도한 범위로 제한됐다.

- 새로 매칭된 행: 908
- 기존 matched 하락: 없음
- 처리 실패 파일: 없음
- 영향 채널: 리디북스(소설), 구글(소설), 문피아

## 다음 스텝

1. `cleaning_rules.py`, `tests/test_cleaning_rules.py`만 커밋 대상으로 잡는다.
2. 문서/검증 산출물은 필요 시 별도 커밋 또는 제외한다.
3. 커밋 후 Cloud 반영을 위해 `main`에 push한다.
4. Cloud에서 같은 입력 파일 일부를 샘플로 돌려 검토필요 감소가 반영되는지 확인한다.
