# 최신 원형 자료 치환표 기반 재료화 테스트

- 작성일: 2026-05-07
- 대상 루트: `\\172.16.10.120\소설사업부\판무팀_ssot\100_계산서_매출등록_자료`
- 범위: 전 플랫폼의 파일명 `정산상세` 포함 원형 중, 파일명/폴더 기준 최신 정산월. 현재일 기준 미래월 오탐은 2025년 자료로 보정했다.
- 테스트 기준: 원본 xlsx 직투입이 아니라, 최종 치환표/어댑터를 거쳐 현재 매핑 시스템이 먹을 표준 정산서 재료로 만들 수 있는지 확인한다.

## 결론

- 37개 플랫폼 모두 최신 `정산상세` 후보를 찾았다.
- 최신 파일 53개를 테스트했고, 파일 파싱은 {'parsed': 53}로 정리됐다.
- 재료화 결과: `{'materialized_default_feed': 35, 'materialized_but_blocked_from_default_feed': 2}`
- 매핑 엔진 smoke test: `{'pass_after_substitution': 31, 'pass_with_review': 4, 'skipped_blocked': 2}`
- 35개 플랫폼은 default feed 재료로 만들 수 있다.
- 2개 플랫폼은 표 형태 파싱은 가능하지만 `non_s2_source_blocked`라 default feed에서 제외했다: 보인&국립장애인도서관, 알라딘 종이책.
- `pass_with_review`는 치환 실패가 아니라 현재 제목 정제/매칭 smoke test상 검토 row가 생긴 케이스다. 원문 제목은 저장하지 않고 sanitize된 행 위치만 별도 CSV에 남겼다.
- S2 4컬럼 출력은 여전히 금액 정책 gate를 따른다.

## 플랫폼별 결과

| 플랫폼 | 최신월 | 파일 | 기본 포함 | 표준화 행 | 재료화 | 매핑 smoke | matched | S2 출력 | 금액상태 |
|---|---|---:|---:|---:|---|---|---:|---|---|
| 교보 | 2025-11 | 1 | 1 | 1266 | materialized_default_feed | pass_after_substitution | 1000/1000 | s2_projection_blocked_by_amount_or_policy | needs_reconcile |
| 구글 | 2026-04 | 1 | 1 | 4393 | materialized_default_feed | pass_with_review | 1000/1000 | s2_projection_blocked_by_amount_or_policy | needs_policy |
| 네이버 | 2026-04 | 9 | 8 | 1121 | materialized_default_feed | pass_with_review | 999/1000 | s2_projection_blocked_by_amount_or_policy | needs_policy |
| 노벨피아 | 2026-04 | 1 | 1 | 28 | materialized_default_feed | pass_after_substitution | 28/28 | s2_projection_blocked_by_amount_or_policy | needs_cancel_policy |
| 로망띠끄 | 2025-11 | 1 | 1 | 38 | materialized_default_feed | pass_after_substitution | 38/38 | s2_projection_candidate_after_policy | candidate_confirmed_after_reconcile |
| 리디북스 | 2025-12 | 1 | 1 | 29853 | materialized_default_feed | pass_after_substitution | 1000/1000 | s2_projection_blocked_by_amount_or_policy | needs_policy |
| 모픽 | 2026-04 | 1 | 1 | 154 | materialized_default_feed | pass_after_substitution | 154/154 | s2_projection_blocked_by_amount_or_policy | needs_policy |
| 무툰 | 2026-04 | 1 | 1 | 537 | materialized_default_feed | pass_after_substitution | 537/537 | s2_projection_blocked_by_amount_or_policy | needs_coin_policy |
| 문피아 | 2026-04 | 1 | 1 | 123 | materialized_default_feed | pass_with_review | 122/123 | s2_projection_blocked_by_amount_or_policy | needs_cancel_policy |
| 미소설 | 2026-04 | 1 | 1 | 106 | materialized_default_feed | pass_after_substitution | 106/106 | s2_projection_blocked_by_amount_or_policy | needs_policy |
| 미스터블루 | 2025-11 | 1 | 1 | 868 | materialized_default_feed | pass_after_substitution | 868/868 | s2_projection_blocked_by_amount_or_policy | blocked_until_settlement_basis |
| 밀리의서재 | 2026-04 | 1 | 1 | 1427 | materialized_default_feed | pass_after_substitution | 1000/1000 | s2_projection_candidate_after_policy | candidate_confirmed_after_reconcile |
| 보인&국립장애인도서관 | 2026-03 | 1 | 0 | 0 | materialized_but_blocked_from_default_feed | skipped_blocked | 0/0 | s2_projection_blocked_non_s2_source | blocked_non_sales |
| 부커스 | 2026-04 | 2 | 1 | 176 | materialized_default_feed | pass_after_substitution | 176/176 | s2_projection_blocked_by_amount_or_policy | needs_derived_sale_policy |
| 북큐브 | 2025-11 | 2 | 2 | 740 | materialized_default_feed | pass_after_substitution | 740/740 | s2_projection_candidate_after_policy | candidate_confirmed_after_reconcile |
| 북팔 | 2025-11 | 2 | 2 | 90 | materialized_default_feed | pass_after_substitution | 90/90 | s2_projection_candidate_after_policy | candidate_confirmed_after_reconcile |
| 블라이스 | 2026-04 | 1 | 1 | 42 | materialized_default_feed | pass_after_substitution | 42/42 | s2_projection_candidate_after_policy | candidate_confirmed_after_reconcile |
| 스낵북 | 2026-03 | 1 | 1 | 7 | materialized_default_feed | pass_after_substitution | 7/7 | s2_projection_candidate_after_policy | candidate_confirmed_after_reconcile |
| 스토린랩 | 2026-02 | 1 | 1 | 176 | materialized_default_feed | pass_after_substitution | 176/176 | s2_projection_blocked_by_amount_or_policy | needs_subadapter_policy |
| 신영미디어 | 2025-09 | 1 | 1 | 1 | materialized_default_feed | pass_after_substitution | 1/1 | s2_projection_candidate_after_policy | candidate_confirmed_after_reconcile |
| 알라딘 | 2025-11 | 1 | 1 | 1659 | materialized_default_feed | pass_after_substitution | 1000/1000 | s2_projection_blocked_by_amount_or_policy | needs_cancel_policy |
| 알라딘 종이책 | 2025-11 | 1 | 0 | 0 | materialized_but_blocked_from_default_feed | skipped_blocked | 0/0 | s2_projection_blocked_non_s2_source | blocked_non_content_ledger |
| 에이블리 | 2025-11 | 1 | 1 | 661 | materialized_default_feed | pass_after_substitution | 661/661 | s2_projection_candidate_after_policy | candidate_confirmed_after_reconcile |
| 에피루스 | 2026-03 | 1 | 1 | 493 | materialized_default_feed | pass_with_review | 493/493 | s2_projection_candidate_after_policy | candidate_confirmed_after_reconcile |
| 예스24 | 2025-11 | 1 | 1 | 703 | materialized_default_feed | pass_after_substitution | 703/703 | s2_projection_blocked_by_amount_or_policy | needs_policy |
| 올툰 | 2025-11 | 1 | 1 | 42 | materialized_default_feed | pass_after_substitution | 42/42 | s2_projection_blocked_by_amount_or_policy | needs_fee_policy |
| 원스토어 | 2026-03 | 1 | 1 | 18018 | materialized_default_feed | pass_after_substitution | 1000/1000 | s2_projection_blocked_by_amount_or_policy | needs_policy |
| 윌라 | 2026-03 | 1 | 1 | 940 | materialized_default_feed | pass_after_substitution | 940/940 | s2_projection_blocked_by_amount_or_policy | needs_policy |
| 조아라 | 2026-04 | 4 | 4 | 230 | materialized_default_feed | pass_after_substitution | 230/230 | s2_projection_blocked_by_amount_or_policy | needs_derived_sale_policy |
| 카카오 | 2025-11 | 3 | 3 | 1700 | materialized_default_feed | pass_after_substitution | 1000/1000 | s2_projection_blocked_by_amount_or_policy | needs_tax_policy |
| 큐툰 | 2026-04 | 1 | 1 | 20 | materialized_default_feed | pass_after_substitution | 20/20 | s2_projection_blocked_by_amount_or_policy | needs_coin_policy |
| 토스(구루컴퍼니) | 2026-03 | 1 | 1 | 37 | materialized_default_feed | pass_after_substitution | 37/37 | s2_projection_blocked_by_amount_or_policy | needs_tax_split_policy |
| 판무림 | 2026-04 | 1 | 1 | 2224 | materialized_default_feed | pass_after_substitution | 1000/1000 | s2_projection_blocked_by_amount_or_policy | needs_contextual_amount_policy |
| 피우리(누온) | 2025-11 | 1 | 1 | 23 | materialized_default_feed | pass_after_substitution | 23/23 | s2_projection_candidate_after_policy | candidate_confirmed_after_reconcile |
| 피플앤스토리 | 2026-04 | 1 | 1 | 3 | materialized_default_feed | pass_after_substitution | 3/3 | s2_projection_candidate_after_policy | candidate_confirmed_after_reconcile |
| 하이북 | 2026-04 | 1 | 1 | 316 | materialized_default_feed | pass_after_substitution | 316/316 | s2_projection_blocked_by_amount_or_policy | needs_purchase_rental_policy |
| 한아름 | 2026-04 | 1 | 1 | 10899 | materialized_default_feed | pass_after_substitution | 1000/1000 | s2_projection_blocked_by_amount_or_policy | needs_section_policy |

## 주의

- 네이버 통합본, 부커스 복사본 등 기존 제외 규칙은 유지했다.
- 보인&국립장애인도서관, 알라딘 종이책은 `non_s2_source_blocked`로 기본 feed 제외다.
- smoke test는 실제 S2 커버리지가 아니라, 표준화 산출물이 현재 `build_mapping()`의 정산서 입력 스키마로 들어가는지 확인하는 테스트다.
- 금액 정책이 `needs_*` 또는 `blocked_*`인 플랫폼은 재료화가 가능해도 S2 전송자료 출력은 금지다.

## 산출물

- `latest_settlement_origin_candidates.csv`
- `latest_origin_substitution_materialization_test_platforms.csv`
- `latest_origin_substitution_materialization_test_files.csv`
- `latest_origin_substitution_materialization_review_rows_sanitized.csv`
- `latest_origin_substitution_materialization_test_report.md`
