# 4월 원형 자료 치환표 기반 재료화 테스트

- 작성일: 2026-05-07
- 대상 루트: `\\172.16.10.120\소설사업부\판무팀_ssot\100_계산서_매출등록_자료`
- 테스트 기준: 원본 xlsx 직투입이 아니라, 최종 치환표/어댑터를 거쳐 현재 매핑 시스템이 먹을 표준 정산서 재료로 만들 수 있는지 확인한다.
- 매핑 엔진 검증: 표준화 결과의 `상품명` 컬럼을 현재 `build_mapping()`에 smoke test로 투입했다. 실제 S2 커버리지가 아니라 스키마 호환성 테스트다.

## 결론

- 37개 플랫폼 중 4월 원형이 있는 플랫폼은 16개, 없는 플랫폼은 21개다.
- 4월 원형 파일 28개를 치환표 기준으로 테스트했다.
- 재료화 결과: `{'not_tested_no_april_source': 21, 'materialized_default_feed': 16}`
- 매핑 엔진 smoke test 결과: `{'skipped': 21, 'pass_after_substitution': 14, 'pass_with_review': 2}`
- 결론: 4월 원형이 존재하는 플랫폼들은 치환표/어댑터를 거치면 현재 매핑 시스템이 먹을 수 있는 재료로 만들 수 있다.
- 단, S2 전송자료 4컬럼 출력은 별도 gate다. 금액 정책이 확정되지 않은 플랫폼은 재료화 가능이어도 S2 출력은 잠금이다.
- `pass_with_review` 2건은 스키마 치환 실패가 아니라 현재 제목 정제 로직에서 정제키가 비는 행이 1행씩 발생한 케이스다. 원문 제목은 저장하지 않고 파일/행 번호만 `april_origin_substitution_materialization_review_rows_sanitized.csv`에 남겼다.

## 플랫폼별 결과

| 플랫폼 | 4월 원형 | 파일 | 기본 포함 | 표준화 행 | 재료화 | 매핑 smoke | smoke matched | S2 출력 | 금액 상태 |
|---|---|---:|---:|---:|---|---|---:|---|---|
| 교보 | no_april_source | 0 | 0 | 0 | not_tested_no_april_source | skipped | 0/0 | not_tested_no_april_source | needs_reconcile |
| 구글 | april_source_found | 1 | 1 | 4393 | materialized_default_feed | pass_after_substitution | 1000/1000 | s2_projection_blocked_by_amount_or_policy | needs_policy |
| 네이버 | april_source_found | 9 | 8 | 1121 | materialized_default_feed | pass_with_review | 999/1000 | s2_projection_blocked_by_amount_or_policy | needs_policy |
| 노벨피아 | april_source_found | 1 | 1 | 28 | materialized_default_feed | pass_after_substitution | 28/28 | s2_projection_blocked_by_amount_or_policy | needs_cancel_policy |
| 로망띠끄 | no_april_source | 0 | 0 | 0 | not_tested_no_april_source | skipped | 0/0 | not_tested_no_april_source | candidate_confirmed_after_reconcile |
| 리디북스 | no_april_source | 0 | 0 | 0 | not_tested_no_april_source | skipped | 0/0 | not_tested_no_april_source | needs_policy |
| 모픽 | april_source_found | 1 | 1 | 154 | materialized_default_feed | pass_after_substitution | 154/154 | s2_projection_blocked_by_amount_or_policy | needs_policy |
| 무툰 | april_source_found | 1 | 1 | 537 | materialized_default_feed | pass_after_substitution | 537/537 | s2_projection_blocked_by_amount_or_policy | needs_coin_policy |
| 문피아 | april_source_found | 1 | 1 | 123 | materialized_default_feed | pass_with_review | 122/123 | s2_projection_blocked_by_amount_or_policy | needs_cancel_policy |
| 미소설 | april_source_found | 1 | 1 | 106 | materialized_default_feed | pass_after_substitution | 106/106 | s2_projection_blocked_by_amount_or_policy | needs_policy |
| 미스터블루 | no_april_source | 0 | 0 | 0 | not_tested_no_april_source | skipped | 0/0 | not_tested_no_april_source | blocked_until_settlement_basis |
| 밀리의서재 | april_source_found | 1 | 1 | 1427 | materialized_default_feed | pass_after_substitution | 1000/1000 | s2_projection_candidate_after_policy | candidate_confirmed_after_reconcile |
| 보인&국립장애인도서관 | no_april_source | 0 | 0 | 0 | not_tested_no_april_source | skipped | 0/0 | not_tested_no_april_source | blocked_non_sales |
| 부커스 | april_source_found | 2 | 1 | 176 | materialized_default_feed | pass_after_substitution | 176/176 | s2_projection_blocked_by_amount_or_policy | needs_derived_sale_policy |
| 북큐브 | no_april_source | 0 | 0 | 0 | not_tested_no_april_source | skipped | 0/0 | not_tested_no_april_source | candidate_confirmed_after_reconcile |
| 북팔 | no_april_source | 0 | 0 | 0 | not_tested_no_april_source | skipped | 0/0 | not_tested_no_april_source | candidate_confirmed_after_reconcile |
| 블라이스 | april_source_found | 1 | 1 | 42 | materialized_default_feed | pass_after_substitution | 42/42 | s2_projection_candidate_after_policy | candidate_confirmed_after_reconcile |
| 스낵북 | no_april_source | 0 | 0 | 0 | not_tested_no_april_source | skipped | 0/0 | not_tested_no_april_source | candidate_confirmed_after_reconcile |
| 스토린랩 | no_april_source | 0 | 0 | 0 | not_tested_no_april_source | skipped | 0/0 | not_tested_no_april_source | needs_subadapter_policy |
| 신영미디어 | no_april_source | 0 | 0 | 0 | not_tested_no_april_source | skipped | 0/0 | not_tested_no_april_source | candidate_confirmed_after_reconcile |
| 알라딘 | no_april_source | 0 | 0 | 0 | not_tested_no_april_source | skipped | 0/0 | not_tested_no_april_source | needs_cancel_policy |
| 알라딘 종이책 | no_april_source | 0 | 0 | 0 | not_tested_no_april_source | skipped | 0/0 | not_tested_no_april_source | blocked_non_content_ledger |
| 에이블리 | no_april_source | 0 | 0 | 0 | not_tested_no_april_source | skipped | 0/0 | not_tested_no_april_source | candidate_confirmed_after_reconcile |
| 에피루스 | no_april_source | 0 | 0 | 0 | not_tested_no_april_source | skipped | 0/0 | not_tested_no_april_source | candidate_confirmed_after_reconcile |
| 예스24 | no_april_source | 0 | 0 | 0 | not_tested_no_april_source | skipped | 0/0 | not_tested_no_april_source | needs_policy |
| 올툰 | no_april_source | 0 | 0 | 0 | not_tested_no_april_source | skipped | 0/0 | not_tested_no_april_source | needs_fee_policy |
| 원스토어 | no_april_source | 0 | 0 | 0 | not_tested_no_april_source | skipped | 0/0 | not_tested_no_april_source | needs_policy |
| 윌라 | no_april_source | 0 | 0 | 0 | not_tested_no_april_source | skipped | 0/0 | not_tested_no_april_source | needs_policy |
| 조아라 | april_source_found | 4 | 4 | 230 | materialized_default_feed | pass_after_substitution | 230/230 | s2_projection_blocked_by_amount_or_policy | needs_derived_sale_policy |
| 카카오 | no_april_source | 0 | 0 | 0 | not_tested_no_april_source | skipped | 0/0 | not_tested_no_april_source | needs_tax_policy |
| 큐툰 | april_source_found | 1 | 1 | 20 | materialized_default_feed | pass_after_substitution | 20/20 | s2_projection_blocked_by_amount_or_policy | needs_coin_policy |
| 토스(구루컴퍼니) | no_april_source | 0 | 0 | 0 | not_tested_no_april_source | skipped | 0/0 | not_tested_no_april_source | needs_tax_split_policy |
| 판무림 | april_source_found | 1 | 1 | 2224 | materialized_default_feed | pass_after_substitution | 1000/1000 | s2_projection_blocked_by_amount_or_policy | needs_contextual_amount_policy |
| 피우리(누온) | no_april_source | 0 | 0 | 0 | not_tested_no_april_source | skipped | 0/0 | not_tested_no_april_source | candidate_confirmed_after_reconcile |
| 피플앤스토리 | april_source_found | 1 | 1 | 3 | materialized_default_feed | pass_after_substitution | 3/3 | s2_projection_candidate_after_policy | candidate_confirmed_after_reconcile |
| 하이북 | april_source_found | 1 | 1 | 316 | materialized_default_feed | pass_after_substitution | 316/316 | s2_projection_blocked_by_amount_or_policy | needs_purchase_rental_policy |
| 한아름 | april_source_found | 1 | 1 | 10899 | materialized_default_feed | pass_after_substitution | 1000/1000 | s2_projection_blocked_by_amount_or_policy | needs_section_policy |

## 적용한 제외/게이트

- 부커스 `- 복사본` 파일은 제외했다.
- 네이버 `선투자 정산상세(통합)`은 중복 가능성 때문에 default feed에서 제외했다.
- 판무림은 `세부내역`만 사용하고 `표지`는 제외했다.
- 한아름은 반복 내부 헤더 구간만 재료화했다.

## 산출물

- `april_origin_substitution_materialization_test_platforms.csv`
- `april_origin_substitution_materialization_test_files.csv`
- `april_origin_substitution_materialization_review_rows_sanitized.csv`
- `april_origin_substitution_materialization_test_summary.json`
