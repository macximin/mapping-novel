# 2026-02 adapter materialization audit

## Summary
- Platform total: 37
- Platforms with period source: 22
- Period files tested: 35
- Parse errors: 0
- Materialization statuses: `{'materialized_default_feed': 22, 'no_period_source': 15}`
- Mapping smoke statuses: `{'pass_after_substitution': 7, 'pass_with_review': 15, 'skipped_no_default_feed': 15}`
- S2 projection statuses: `{'s2_projection_blocked_by_amount_or_policy': 24, 's2_projection_blocked_non_s2_source': 2, 's2_projection_candidate_after_policy': 11}`

## Platform Results
| platform | files | default_feed_rows | materialization | mapping_smoke | amount_rule | s2_projection |
|---|---:|---:|---|---|---|---|
| 교보 | 0 | 0 | no_period_source | skipped_no_default_feed | needs_reconcile | s2_projection_blocked_by_amount_or_policy |
| 구글 | 1 | 4242 | materialized_default_feed | pass_with_review | needs_policy | s2_projection_blocked_by_amount_or_policy |
| 네이버 | 9 | 1008 | materialized_default_feed | pass_with_review | needs_policy | s2_projection_blocked_by_amount_or_policy |
| 노벨피아 | 1 | 74 | materialized_default_feed | pass_after_substitution | needs_cancel_policy | s2_projection_blocked_by_amount_or_policy |
| 로망띠끄 | 0 | 0 | no_period_source | skipped_no_default_feed | candidate_confirmed_after_reconcile | s2_projection_candidate_after_policy |
| 리디북스 | 0 | 0 | no_period_source | skipped_no_default_feed | needs_policy | s2_projection_blocked_by_amount_or_policy |
| 모픽 | 1 | 332 | materialized_default_feed | pass_with_review | needs_policy | s2_projection_blocked_by_amount_or_policy |
| 무툰 | 1 | 514 | materialized_default_feed | pass_with_review | needs_coin_policy | s2_projection_blocked_by_amount_or_policy |
| 문피아 | 1 | 118 | materialized_default_feed | pass_with_review | needs_cancel_policy | s2_projection_blocked_by_amount_or_policy |
| 미소설 | 2 | 125 | materialized_default_feed | pass_after_substitution | needs_policy | s2_projection_blocked_by_amount_or_policy |
| 미스터블루 | 0 | 0 | no_period_source | skipped_no_default_feed | blocked_until_settlement_basis | s2_projection_blocked_by_amount_or_policy |
| 밀리의서재 | 1 | 1192 | materialized_default_feed | pass_after_substitution | candidate_confirmed_after_reconcile | s2_projection_candidate_after_policy |
| 보인&국립장애인도서관 | 0 | 0 | no_period_source | skipped_no_default_feed | blocked_non_sales | s2_projection_blocked_non_s2_source |
| 부커스 | 1 | 192 | materialized_default_feed | pass_with_review | needs_derived_sale_policy | s2_projection_blocked_by_amount_or_policy |
| 북큐브 | 0 | 0 | no_period_source | skipped_no_default_feed | candidate_confirmed_after_reconcile | s2_projection_candidate_after_policy |
| 북팔 | 0 | 0 | no_period_source | skipped_no_default_feed | candidate_confirmed_after_reconcile | s2_projection_candidate_after_policy |
| 블라이스 | 1 | 63 | materialized_default_feed | pass_after_substitution | candidate_confirmed_after_reconcile | s2_projection_candidate_after_policy |
| 스낵북 | 1 | 7 | materialized_default_feed | pass_after_substitution | candidate_confirmed_after_reconcile | s2_projection_candidate_after_policy |
| 스토린랩 | 1 | 176 | materialized_default_feed | pass_with_review | needs_subadapter_policy | s2_projection_blocked_by_amount_or_policy |
| 신영미디어 | 0 | 0 | no_period_source | skipped_no_default_feed | candidate_confirmed_after_reconcile | s2_projection_candidate_after_policy |
| 알라딘 | 0 | 0 | no_period_source | skipped_no_default_feed | needs_cancel_policy | s2_projection_blocked_by_amount_or_policy |
| 알라딘 종이책 | 0 | 0 | no_period_source | skipped_no_default_feed | blocked_non_content_ledger | s2_projection_blocked_non_s2_source |
| 에이블리 | 0 | 0 | no_period_source | skipped_no_default_feed | candidate_confirmed_after_reconcile | s2_projection_candidate_after_policy |
| 에피루스 | 1 | 461 | materialized_default_feed | pass_with_review | candidate_confirmed_after_reconcile | s2_projection_candidate_after_policy |
| 예스24 | 0 | 0 | no_period_source | skipped_no_default_feed | needs_policy | s2_projection_blocked_by_amount_or_policy |
| 올툰 | 0 | 0 | no_period_source | skipped_no_default_feed | needs_fee_policy | s2_projection_blocked_by_amount_or_policy |
| 원스토어 | 1 | 19607 | materialized_default_feed | pass_with_review | needs_policy | s2_projection_blocked_by_amount_or_policy |
| 윌라 | 1 | 883 | materialized_default_feed | pass_with_review | needs_policy | s2_projection_blocked_by_amount_or_policy |
| 조아라 | 5 | 217 | materialized_default_feed | pass_with_review | needs_derived_sale_policy | s2_projection_blocked_by_amount_or_policy |
| 카카오 | 0 | 0 | no_period_source | skipped_no_default_feed | needs_tax_policy | s2_projection_blocked_by_amount_or_policy |
| 큐툰 | 1 | 25 | materialized_default_feed | pass_with_review | needs_coin_policy | s2_projection_blocked_by_amount_or_policy |
| 토스(구루컴퍼니) | 1 | 45 | materialized_default_feed | pass_after_substitution | needs_tax_split_policy | s2_projection_blocked_by_amount_or_policy |
| 판무림 | 1 | 3331 | materialized_default_feed | pass_with_review | needs_contextual_amount_policy | s2_projection_blocked_by_amount_or_policy |
| 피우리(누온) | 0 | 0 | no_period_source | skipped_no_default_feed | candidate_confirmed_after_reconcile | s2_projection_candidate_after_policy |
| 피플앤스토리 | 1 | 4 | materialized_default_feed | pass_after_substitution | candidate_confirmed_after_reconcile | s2_projection_candidate_after_policy |
| 하이북 | 1 | 355 | materialized_default_feed | pass_with_review | needs_purchase_rental_policy | s2_projection_blocked_by_amount_or_policy |
| 한아름 | 1 | 9253 | materialized_default_feed | pass_with_review | needs_section_policy | s2_projection_blocked_by_amount_or_policy |

## Amount Policy Gate
- Candidate after policy lock: 11
- Still blocked or non-S2: 26
- This audit does not emit final S2 4-column upload rows for blocked amount policies.
