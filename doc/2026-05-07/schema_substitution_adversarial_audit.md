# 스키마 치환 적대적 감리 3회

- 작성일: 2026-05-07
- 대상 루트: `\\172.16.10.120\소설사업부\판무팀_ssot\100_계산서_매출등록_자료`
- 목적: 플랫폼별 `정산상세` 원형 헤더를 표준 스키마/S2 전송자료 스키마로 치환하는 최선안을 정하고, 그 방법을 일부러 공격해 본다.

## 최선안

최선안은 단일 치환표가 아니다. `원본 헤더 -> 표준 필드 rename -> S2 출력` 직행은 금지한다.

대신 아래 3단 구조가 맞다.

```text
1. Adapter detect
   platform + file name + sheet name + schema signature + required columns로 대상 구조를 확정

2. Canonical semantic slots
   원본 헤더를 바로 S2 4컬럼으로 바꾸지 않고, raw_title/gross_sales_amount/settlement_base_amount/offset_amount 같은 의미 슬롯으로 표준화

3. S2 projection gate
   schema_status=parsed, row_status=data, amount_rule_status=confirmed, match_status=matched 인 행만 S2 4컬럼으로 출력
```

핵심은 `missing`과 `0`을 구분하는 것이다. `상계금액` 후보가 없다고 자동으로 0을 넣으면 안 된다. 플랫폼 규칙상 상계가 없다는 것이 확인된 경우만 `offset_amount=0`을 허용한다.

## 표준 치환 스키마

표준 스키마는 세 층으로 나눈다.

| 층 | 역할 | 예시 필드 |
|---|---|---|
| 추적/구조 층 | 어떤 파일/시트/행/adapter에서 온 값인지 보존 | `source_file`, `source_sheet`, `source_row_no`, `adapter_id`, `schema_signature` |
| 의미 슬롯 층 | 플랫폼별 원본 헤더를 공통 의미로 보존 | `raw_title`, `external_work_id`, `gross_sales_amount`, `settlement_base_amount`, `offset_amount` |
| 출력 투영 층 | S2 전송자료 4컬럼으로 최종 변환 | `판매채널콘텐츠ID`, `판매금액`, `정산기준액`, `상계금액` |

상세 필드 계약은 `schema_substitution_target_schema_v1.csv`에 따로 뺐다.

## 현재 근거

- 전수조사 플랫폼-스키마 그룹: 80개
- 전수조사 adapter 후보/manual review: 46 / 34
- 4월 대표 원형 기준: {'D_no_april_source': 21, 'C_special_adapter_required': 4, 'A_generic_adapter_candidate': 8, 'B_amount_semantics_required': 4}
- 4월 기준 일반 어댑터 후보도 S2 금액 확정 전에는 최종 전송자료를 만들면 안 된다. 제목 매핑 가능성과 금액 전송 가능성은 다른 문제다.

## 감리 1회차: 헤더 치환 공격

질문: 원본 헤더 alias dict만으로 `상품명/판매금액/정산기준액/상계금액`을 만들 수 있는가?

결론: 안 된다.

- 전수조사 80개 스키마 그룹 중 34개가 manual review다.
- 구글은 영문 transaction export라 한국어 alias에서 빠진다.
- 네이버/판무림은 병합/다중 헤더 때문에 `판매금액`, `합계` 같은 단일 텍스트가 문맥 없이 무의미해진다.
- 한아름은 한 시트 안에 반복 헤더가 여러 번 나온다. 첫 번째 헤더 후보만 쓰면 요약표를 상세표로 오인한다.
- 윌라는 전수조사 기준 `sheet`와 `콘텐츠 가격 변동 이력` 2시트 구조라 시트 선별 없이 먹이면 보조 시트가 섞일 수 있다.

대응: header alias는 “후보 추천”에만 쓰고, 실제 parse는 adapter별 detect/whitelist/signature로 고정한다.

## 감리 2회차: 금액 의미 공격

질문: 헤더명을 표준 금액 필드로 바꾸면 S2 4컬럼이 의미상 맞는가?

결론: 그대로는 위험하다.

- `합계`, `정산금액`, `판매금액`, `수수료`는 플랫폼별 의미가 다르다.
- 부커스는 판매금액 직접 컬럼이 없어서 `정가 x 열람 횟수` 같은 파생 규칙이 필요할 수 있다.
- 조아라 단행본은 단가 컬럼이 없어 판매금액 산출이 불명확하다.
- 무툰/큐툰은 코인 값을 원화 판매금액으로 봐도 되는지 확인해야 한다.
- 구글은 법인세/원천징수 차감 전후 금액 중 무엇을 쓸지 업무 기준이 필요하다.

대응: 모든 금액 필드는 `amount_rule_id`와 `amount_rule_status`를 가진다. `confirmed` 또는 `derived_confirmed`가 아니면 S2 출력 금지다.

## 감리 3회차: 실행 안전 공격

질문: 파싱된 행을 매칭만 되면 S2로 내보내도 되는가?

결론: 안 된다. 매칭 성공은 전송 가능의 필요조건일 뿐이다.

S2 출력 조건은 전부 만족해야 한다.

```text
schema_status == parsed
row_status == data
amount_rule_status in {confirmed, derived_confirmed}
match_status == matched
sales_channel_content_id not blank
```

추가로 파일/시트 오염을 막아야 한다.

- 부커스 `- 복사본` 제외
- 네이버 `선투자 정산상세(통합)`은 중복 가능성 때문에 review gate
- 판무림 `표지` 제외, `세부내역`만 파싱
- 한아름은 합계/계산서 안내/반복 헤더 row를 `data`로 내보내면 안 됨
- 4월 원형 없는 21개는 catalog상 후보가 있어도 fallback 월 대표 감리 전 구현 금지

## 플랫폼별 전략

| 플랫폼 | 4월 판정 | 전수조사 그룹 | 전략 | 출력 gate | confidence |
|---|---|---:|---|---|---|
| 교보 | 4월 대표 원형 없음 | 1 | fallback_month_manual_structure_audit | no_april_evidence; choose representative_month_before_schema_substitution | catalog_only_low |
| 구글 | 특수 구조 파서 필요 | 2 | structural_special_adapter | no_generic_substitution; s2_projection_blocked_until_special_parser_fixture_passes | low_until_parser |
| 네이버 | 특수 구조 파서 필요 | 2 | structural_special_adapter | no_generic_substitution; s2_projection_blocked_until_special_parser_fixture_passes | low_until_parser |
| 노벨피아 | 일반 어댑터 후보 | 1 | single_header_semantic_adapter | title_mapping_allowed; s2_projection_after_amount_policy_and_total_reconcile | medium_high |
| 로망띠끄 | 4월 대표 원형 없음 | 3 | fallback_month_manual_structure_audit | no_april_evidence; choose representative_month_before_schema_substitution | catalog_only_low |
| 리디북스 | 4월 대표 원형 없음 | 1 | fallback_month_single_or_variant_adapter_audit | no_april_evidence; choose representative_month_before_schema_substitution | catalog_only_medium |
| 모픽 | 일반 어댑터 후보 | 4 | single_header_semantic_adapter | title_mapping_allowed; s2_projection_after_amount_policy_and_total_reconcile | medium_high |
| 무툰 | 금액 의미/파생 규칙 확인 필요 | 2 | single_or_multi_header_adapter_with_amount_policy | title_mapping_allowed_after_parser; s2_projection_blocked_until_amount_rule_confirmed | medium |
| 문피아 | 일반 어댑터 후보 | 1 | single_header_semantic_adapter | title_mapping_allowed; s2_projection_after_amount_policy_and_total_reconcile | medium_high |
| 미소설 | 일반 어댑터 후보 | 2 | single_header_semantic_adapter | title_mapping_allowed; s2_projection_after_amount_policy_and_total_reconcile | medium_high |
| 미스터블루 | 4월 대표 원형 없음 | 2 | fallback_month_mixed_adapter_audit | no_april_evidence; choose representative_month_before_schema_substitution | catalog_only_low_medium |
| 밀리의서재 | 일반 어댑터 후보 | 2 | single_header_semantic_adapter | title_mapping_allowed; s2_projection_after_amount_policy_and_total_reconcile | medium_high |
| 보인&국립장애인도서관 | 4월 대표 원형 없음 | 2 | fallback_month_manual_structure_audit | no_april_evidence; choose representative_month_before_schema_substitution | catalog_only_low |
| 부커스 | 금액 의미/파생 규칙 확인 필요 | 1 | single_or_multi_header_adapter_with_amount_policy | title_mapping_allowed_after_parser; s2_projection_blocked_until_amount_rule_confirmed | medium |
| 북큐브 | 4월 대표 원형 없음 | 1 | fallback_month_manual_structure_audit | no_april_evidence; choose representative_month_before_schema_substitution | catalog_only_low |
| 북팔 | 4월 대표 원형 없음 | 3 | fallback_month_mixed_adapter_audit | no_april_evidence; choose representative_month_before_schema_substitution | catalog_only_low_medium |
| 블라이스 | 일반 어댑터 후보 | 1 | single_header_semantic_adapter | title_mapping_allowed; s2_projection_after_amount_policy_and_total_reconcile | medium_high |
| 스낵북 | 4월 대표 원형 없음 | 1 | fallback_month_single_or_variant_adapter_audit | no_april_evidence; choose representative_month_before_schema_substitution | catalog_only_medium |
| 스토린랩 | 4월 대표 원형 없음 | 10 | fallback_month_mixed_adapter_audit | no_april_evidence; choose representative_month_before_schema_substitution | catalog_only_low_medium |
| 신영미디어 | 4월 대표 원형 없음 | 1 | fallback_month_single_or_variant_adapter_audit | no_april_evidence; choose representative_month_before_schema_substitution | catalog_only_medium |
| 알라딘 | 4월 대표 원형 없음 | 3 | fallback_month_manual_structure_audit | no_april_evidence; choose representative_month_before_schema_substitution | catalog_only_low |
| 알라딘 종이책 | 4월 대표 원형 없음 | 1 | fallback_month_manual_structure_audit | no_april_evidence; choose representative_month_before_schema_substitution | catalog_only_low |
| 에이블리 | 4월 대표 원형 없음 | 2 | fallback_month_single_or_variant_adapter_audit | no_april_evidence; choose representative_month_before_schema_substitution | catalog_only_medium |
| 에피루스 | 4월 대표 원형 없음 | 1 | fallback_month_single_or_variant_adapter_audit | no_april_evidence; choose representative_month_before_schema_substitution | catalog_only_medium |
| 예스24 | 4월 대표 원형 없음 | 1 | fallback_month_manual_structure_audit | no_april_evidence; choose representative_month_before_schema_substitution | catalog_only_low |
| 올툰 | 4월 대표 원형 없음 | 1 | fallback_month_single_or_variant_adapter_audit | no_april_evidence; choose representative_month_before_schema_substitution | catalog_only_medium |
| 원스토어 | 4월 대표 원형 없음 | 1 | fallback_month_single_or_variant_adapter_audit | no_april_evidence; choose representative_month_before_schema_substitution | catalog_only_medium |
| 윌라 | 4월 대표 원형 없음 | 2 | fallback_month_manual_structure_audit | no_april_evidence; choose representative_month_before_schema_substitution | catalog_only_low |
| 조아라 | 금액 의미/파생 규칙 확인 필요 | 5 | single_or_multi_header_adapter_with_amount_policy | title_mapping_allowed_after_parser; s2_projection_blocked_until_amount_rule_confirmed | medium |
| 카카오 | 4월 대표 원형 없음 | 1 | fallback_month_single_or_variant_adapter_audit | no_april_evidence; choose representative_month_before_schema_substitution | catalog_only_medium |
| 큐툰 | 금액 의미/파생 규칙 확인 필요 | 1 | single_or_multi_header_adapter_with_amount_policy | title_mapping_allowed_after_parser; s2_projection_blocked_until_amount_rule_confirmed | medium |
| 토스(구루컴퍼니) | 4월 대표 원형 없음 | 2 | fallback_month_single_or_variant_adapter_audit | no_april_evidence; choose representative_month_before_schema_substitution | catalog_only_medium |
| 판무림 | 특수 구조 파서 필요 | 9 | structural_special_adapter | no_generic_substitution; s2_projection_blocked_until_special_parser_fixture_passes | low_until_parser |
| 피우리(누온) | 4월 대표 원형 없음 | 1 | fallback_month_single_or_variant_adapter_audit | no_april_evidence; choose representative_month_before_schema_substitution | catalog_only_medium |
| 피플앤스토리 | 일반 어댑터 후보 | 2 | single_header_semantic_adapter | title_mapping_allowed; s2_projection_after_amount_policy_and_total_reconcile | medium_high |
| 하이북 | 일반 어댑터 후보 | 1 | single_header_semantic_adapter | title_mapping_allowed; s2_projection_after_amount_policy_and_total_reconcile | medium_high |
| 한아름 | 특수 구조 파서 필요 | 3 | structural_special_adapter | no_generic_substitution; s2_projection_blocked_until_special_parser_fixture_passes | low_until_parser |

## 즉시 구현 순서

1. `schema_substitution_target_schema_v1.csv`의 표준 필드를 코드 계약으로 고정한다.
2. 먼저 `single_header_semantic_adapter` 계열 8개 플랫폼을 fixture 기반으로 만든다. 단 S2 출력은 금액 규칙 확정 전까지 막는다.
3. 무툰/큐툰/부커스/조아라는 amount policy를 먼저 확정한 뒤 adapter를 연다.
4. 구글/네이버/판무림/한아름은 generic parser에 태우지 말고 structural special adapter로만 처리한다.
5. 4월 원형 없는 21개는 최근월 대표 샘플을 잡아서 같은 감리를 반복한 뒤 구현한다.

## 산출물

- `schema_substitution_target_schema_v1.csv`: 표준 치환 스키마 계약
- `schema_substitution_platform_strategy.csv`: 37개 플랫폼별 치환 전략/gate
- `schema_substitution_adversarial_findings.csv`: 적대적 감리 3회 findings
- `schema_substitution_adversarial_summary.json`: 집계 요약
