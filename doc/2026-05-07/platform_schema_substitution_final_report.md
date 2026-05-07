# 플랫폼별 스키마 치환표 최종 종결

- 작성일: 2026-05-07
- 대상 루트: `\\172.16.10.120\소설사업부\판무팀_ssot\100_계산서_매출등록_자료`
- 범위: 파일명에 `정산상세`가 포함된 원형 파일. 기존 전수조사 80개 스키마 그룹과 대표 샘플 재스캔 결과를 결합했다.
- 목적: 각 플랫폼별 3회 적대적 감리 결과를 반영해 표준 스키마 치환표를 최종 확정한다.

## 종결 원칙

1. 원본 헤더를 S2 4컬럼으로 바로 rename하지 않는다.
2. 먼저 `normalized_title`, `external_work_id`, `gross_sales_amount`, `settlement_base_amount`, `offset_amount` 의미 슬롯으로 치환한다.
3. S2 출력은 `schema_status=parsed`, `row_status=data`, `amount_rule_status` 확정, `match_status=matched`, `판매채널콘텐츠ID` 존재가 모두 참일 때만 허용한다.
4. `상계금액`은 없음과 0을 구분한다. 플랫폼 규칙상 없음이 확인된 경우만 0으로 출력한다.

## 감리 3회 기준

| 회차 | 공격 질문 | 종결 기준 |
|---:|---|---|
| 1 | 헤더 alias rename만으로 표준 필드를 만들 수 있는가? | platform/sheet/signature 기반 adapter contract가 있어야 함 |
| 2 | 금액 헤더명이 S2 금액 의미와 일치하는가? | amount_rule_status가 confirmed 계열이어야 함 |
| 3 | 파싱/매칭 성공 행을 바로 전송해도 되는가? | hard gate 통과 행만 S2 출력 |

## 전체 요약

- 플랫폼: 37개
- 플랫폼-스키마 그룹: 80개
- 플랫폼별 감리 row: 111개, 플랫폼마다 3회
- 그룹 action: {'include_after_amount_policy': 40, 'special_parser_only': 8, 'include_candidate_after_rescan': 7, 'include_for_detail_or_reconcile_only': 1, 'blocked_non_s2_source': 3, 'include_candidate': 9, 'exclude_aux_or_summary': 10, 'exclude_or_manual_summary': 2}
- final class: {'single_header_policy_gate': 18, 'special_parser_required': 4, 'wide_header_policy_gate': 3, 'single_header_amount_policy_required': 4, 'mixed_sheet_policy_gate': 3, 'non_s2_source_blocked': 2, 'multi_adapter_policy_gate': 1, 'single_header_with_aux_exclusion': 1, 'multi_schema_amount_policy_required': 1}

## 플랫폼 최종 치환표

| 플랫폼 | 최종 class | parser | 제목 | 외부ID | 판매금액 | 정산기준액 | 상계금액 | gate |
|---|---|---|---|---|---|---|---|---|
| 교보 | single_header_policy_gate | single_header_with_leading_merged_title | 상품명 | 판매상품ID / epub isbn / pdf isbn / 종이책ISBN / 북넘버 | 정산대상판매가총액 또는 판매가 x 판매수량 | 정산액 | 없음 확인 필요 | 대표월 fixture + 판매금액 산식 확정 전 S2 출력 금지 |
| 구글 | special_parser_required | google_transaction_fixed_columns | Title | Id / Product / Primary ISBN | Publisher Revenue / Payment Amount 후보 | 법인세 차감 후 금액 또는 Publisher Revenue 후보 | 미국 원천징수세 / 법인세 차감액 후보 | Google 금액 기준 확정 + 외화/세금 처리 전 S2 출력 금지 |
| 네이버 | special_parser_required | two_row_merged_header_flatten | 컨텐츠 | 컨텐츠No / 공급자코드 | 합계 | 정산금액 있는 그룹만 직접 후보, 그 외 정책 필요 | 마켓수수료(추정치) / 유상 이용권 보정 | 2행 헤더 flatten + 통합/선투자 중복 검산 전 S2 출력 금지 |
| 노벨피아 | single_header_policy_gate | single_header | 상품명 | 작품코드 | 판매합계 또는 판매금액 | 정산금액 | 취소금액 | 취소금액 처리 정책 확정 후 S2 출력 |
| 로망띠끄 | single_header_policy_gate | single_header_row5_with_merged_banner | 도서명 | 도서코드 / isbn | 판매액 | 정산액 | 없음 확인 필요 | 대표월 fixture + 총액 reconcile 후 출력 |
| 리디북스 | wide_header_policy_gate | wide_single_header_limited_columns | 제목 / 시리즈명 | 도서 ID / 시리즈 ID / 전자책ISBN10/13 | 판매액 + 단권/세트/대여 판매액 후보 | 정산액 / 앱마켓 정산대상액 후보 | 취소액 / 앱마켓 수수료 / 앱마켓 취소액 | 판매/취소/앱마켓 산식 확정 전 S2 출력 금지 |
| 모픽 | single_header_policy_gate | single_header_variants | 작품명 | 없음 | 총 매출액 또는 순 매출액 | 정산액 | 총 매출액-순 매출액 후보 | 총/순 매출 기준 확정 후 S2 출력 |
| 무툰 | single_header_amount_policy_required | merged_header_coin_table | 타이틀 | 없음 | 합계 / 사용코인 후보 | 정산총액 또는 정산금액 | 공제수수료 / 취소코인 | 코인-원화 및 취소/공제 기준 확정 전 S2 출력 금지 |
| 문피아 | single_header_policy_gate | single_header_limited_columns | 작품 | 작품코드 | 총매출 | 정산 | 구매취소 / 대여취소 | 취소/IOS/Google 포함 여부 확정 후 S2 출력 |
| 미소설 | single_header_policy_gate | single_header_variants | 타이틀 | 작품번호 | 전체매출(원) | 총지급액(원) 또는 지급액(원) ASP | 결제수수료(원) | 지급액 컬럼 선택 기준 확정 후 S2 출력 |
| 미스터블루 | mixed_sheet_policy_gate | sheet_whitelist_workbook | 작품명 | 작품코드 | 작품별 합계(정액+종량) / 볼륨별 소계 후보 | 정산기준액 후보 없음. 정책 필요 | 없음 확인 필요 | 정산기준액 정의 전 S2 출력 금지 |
| 밀리의서재 | single_header_policy_gate | single_header_variants | 콘텐츠명 | 전자출판물 ISBN / 유통사 상품코드 | 발생 금액 | 정산 예정 금액 / 정산 금액 | 없음 확인 필요 | 에피소드명 집계 단위 확정 + 총액 reconcile 후 출력 |
| 보인&국립장애인도서관 | non_s2_source_blocked | purchase_selection_list | 제목 | 없음 | 판매가 / 구매비 | 정산기준액 없음 | 없음 | S2 판매상세가 아니라 구매/목록선정 자료로 판단. 기본 출력 금지 |
| 부커스 | single_header_amount_policy_required | single_header_with_duplicate_file_filter | 콘텐츠 제목 | ISBN | 정가 x 열람 횟수 후보 | 정산 금액(원) | 정산율 기반 파생 후보 | 판매금액 파생식 확정 전 S2 출력 금지 |
| 북큐브 | single_header_policy_gate | single_header_row2 | 제목 | 도서번호 / ISBN / e-ISBN(PDF) / e-ISBN(ePub) / 제휴도서번호 | 판매액 | 정산액 / 정산대상금액 | 할인 / 수수료 | 대표월 fixture + 할인/수수료 상계 기준 확정 후 출력 |
| 북팔 | mixed_sheet_policy_gate | sheet_whitelist_plus_legacy_exclusion | 작품명 | 아이디 / 순서 후보 | 매출 | 수익금 / 수익 | 없음 확인 필요 | 보조 안내문 시트 제외 + 총액 reconcile 후 출력 |
| 블라이스 | single_header_policy_gate | single_header_with_merged_cells | 작품명 | 작품NO | 판매액 | 정산금액 | 수수료 | 수수료 컬럼명 정규화 + 총액 reconcile 후 출력 |
| 스낵북 | single_header_policy_gate | single_header | 작품명 | 작품ID | 판매(원) | 정산(원) | 없음 확인 필요 | 대표월 fixture + 총액 reconcile 후 출력 |
| 스토린랩 | multi_adapter_policy_gate | schema_signature_subadapters | 채널상품명 / 상품명 / 이용상품명 / 타이틀 | 채널상품ID / 상품ID / 키다리코드 / 이용상품ID | 판매금액 / 정액권 총매출액 / 판매총액 / 합계금액 | 정산액 / 입금액 / 상품별 정산금액 | 취소금액 / 차감 / 취소코인 | 서브어댑터별 amount rule 확정 전 S2 출력 금지 |
| 신영미디어 | single_header_policy_gate | single_header | 제목 | 번호 | 합계 | 소득액 | 없음 확인 필요 | 대표월 fixture + 총액 reconcile 후 출력 |
| 알라딘 | single_header_policy_gate | single_header | 제목 | ItemId / ISBN / CID | 판매가 | 정산액 | 판매형태=취소/원주문일시 기반 취소 처리 | 판매/취소 행 처리 정책 확정 후 S2 출력 |
| 알라딘 종이책 | non_s2_source_blocked | ledger_not_content_sales | 거래내용 후보이나 작품명 아님 | 없음 | 출고금액 / 반품금액 | 현잔액 등 원장 금액 | 반품금액 | 전자 S2 콘텐츠 판매상세 아님. 기본 출력 금지 |
| 에이블리 | mixed_sheet_policy_gate | single_header_plus_summary_exclusion | 작품명 | 작품 ID | 판매 금액 합계 (원) | 정산 금액 합계 (원) | 운영 수수료 합계 (원) | 요약 시트 제외 + 총액 reconcile 후 출력 |
| 에피루스 | single_header_policy_gate | single_header | 제목 | 없음 | 판매금액 | 정산액 | 수수료 | 대표월 fixture + 수수료 상계 정책 확인 후 출력 |
| 예스24 | single_header_policy_gate | single_header | 도서명 | bookID / ePubID / 전자책ISBN / 종이책ISBN / 세트코드 | 서점판매가 또는 출판사판매가 | 출판사정산액 | 서점환불가 / 환불일 기반 취소 처리 | 판매가 기준/환불 처리 정책 확정 후 출력 |
| 올툰 | single_header_amount_policy_required | single_header | 작품명 | 없음 | 총 매출액(원) / 코인 사용수량 | 정산 대상 금액(수수료 제외) | 앱스토어 수수료(원) / 올웨이즈 수수료(원) | 수수료 제외/순매출 기준 확정 후 S2 출력 |
| 원스토어 | wide_header_policy_gate | wide_single_header_limited_columns | 채널상품명 / 상품명 | 채널상품ID / 상품ID / 파트너ID | 판매 / 합계 / 정액제 정산대상액 후보 | 정산지급액 | 취소 / 앱마켓수수료 / 서비스이용료 | 판매/정액제/취소/수수료 산식 확정 전 S2 출력 금지 |
| 윌라 | single_header_with_aux_exclusion | sheet_whitelist_single_header | 콘텐츠명 | 코드 / 전자책ISBN | 공급가 또는 정가 후보 | 정산 금액 | 없음 확인 필요 | 공급가/정가 판매금액 기준 확정 + 가격변동이력 제외 후 출력 |
| 조아라 | multi_schema_amount_policy_required | schema_signature_variants | 작품명 | 작품코드 있는 그룹만 | 단가 x 판매건수 / 금액 / 이용권 후보. 일부 단행본은 판매금액 불명 | 정산금액 | 없음 확인 필요 | 단가 없는 그룹의 판매금액 정책 확정 전 S2 출력 금지 |
| 카카오 | wide_header_policy_gate | merged_header_single_table | 시리즈명 | 시리즈ID / 제품코드 / 계약UID | 공급대가 또는 총합계-순매출 후보 | 공급가액 | 세액은 별도 보존. 상계 아님 | 과/면세 및 공급가액/공급대가 기준 확정 전 출력 금지 |
| 큐툰 | single_header_amount_policy_required | merged_header_coin_table | 타이틀 | 없음 | 합계 / 사용코인 후보 | 정산총액 | 공제수수료 / 취소코인 | 코인-원화 및 취소/공제 기준 확정 전 S2 출력 금지 |
| 토스(구루컴퍼니) | single_header_policy_gate | single_header_variants | 작품명 | 작품번호 | 콘텐츠 매출금액 | 콘텐츠 정산금액 또는 면세+과세 합산 | 결제수수료 | 면세/과세 합산 기준 확정 후 출력 |
| 판무림 | special_parser_required | cover_exclusion_contextual_amount_flatten | 작품 제목 / 회차 제목 | 시리즈 코드 / 각 권 코드 | 소장/대여/정액제/포인트 구간 판매금액 flatten 후 합산 후보 | 표지 정산비율 적용 여부 정책 필요 | 포인트 사용 후보 | 상단 문맥+반복 판매금액 flatten 전 S2 출력 금지 |
| 피우리(누온) | single_header_policy_gate | single_header | 제목 | 피우리상품번호 / CP관리번호 | 매출액 또는 판매가 x 판매수 | 정산액 | 없음 확인 필요 | 대표월 fixture + 판매가/매출액 기준 확인 후 출력 |
| 피플앤스토리 | single_header_policy_gate | single_header_with_small_sample_gate | 작품명 | ISBN / 관리코드 | 판매금액(원) | 정산금액(원) | 없음 확인 필요 | 샘플 행 적음. 다른 월 1개 추가 fixture 후 출력 |
| 하이북 | single_header_policy_gate | single_header | 작품명 | prodSq | 판매금액(구매)+판매금액(대여) 또는 판매금액 | 정산금액(구매)+정산금액(대여) 또는 정산금액 | 없음 확인 필요 | 구매/대여 합산 단위 확정 후 출력 |
| 한아름 | special_parser_required | multi_section_repeated_header_parser | 작품명 | BOOK NO | 건당 로그 금액 / 시간 로그 히트 기반 정책 필요 | 상단 정산금액 요약 및 배분률 대조 | 배분률 기반 차액 후보 | 구간 파서 + 시간/건당 과금 정책 확정 전 S2 출력 금지 |

## 플랫폼별 종결 메모

### 교보
- 최종 class: `single_header_policy_gate`
- 포함: Sheet1
- 제외: -
- 금액 상태: `needs_reconcile`
- 리스크: 4월 샘플 없음. 판매가/정산대상판매가총액 중 판매금액 기준 확정 필요.
- 종결 gate: 대표월 fixture + 판매금액 산식 확정 전 S2 출력 금지

### 구글
- 최종 class: `special_parser_required`
- 포함: GoogleSalesTransactionReport
- 제외: -
- 금액 상태: `needs_policy`
- 리스크: 영문 transaction report라 일반 alias 치환 불가.
- 종결 gate: Google 금액 기준 확정 + 외화/세금 처리 전 S2 출력 금지

### 네이버
- 최종 class: `special_parser_required`
- 포함: contentsSelling_*
- 제외: 복사본, 중복 통합본은 review gate
- 금액 상태: `needs_policy`
- 리스크: 병합 헤더 문맥 없이는 금액 의미가 무너짐.
- 종결 gate: 2행 헤더 flatten + 통합/선투자 중복 검산 전 S2 출력 금지

### 노벨피아
- 최종 class: `single_header_policy_gate`
- 포함: 일별 정산
- 제외: -
- 금액 상태: `needs_cancel_policy`
- 리스크: 판매금액/판매합계/취소금액 처리 방식 필요.
- 종결 gate: 취소금액 처리 정책 확정 후 S2 출력

### 로망띠끄
- 최종 class: `single_header_policy_gate`
- 포함: styleB(바로북)*
- 제외: -
- 금액 상태: `candidate_confirmed_after_reconcile`
- 리스크: 4월 샘플 없음. 헤더 row 5 고정 여부 확인 필요.
- 종결 gate: 대표월 fixture + 총액 reconcile 후 출력

### 리디북스
- 최종 class: `wide_header_policy_gate`
- 포함: calculate_1 및 리디 정산상세 시트
- 제외: -
- 금액 상태: `needs_policy`
- 리스크: 대용량 wide schema. 취소/앱마켓 컬럼이 많아 단순 합산 위험.
- 종결 gate: 판매/취소/앱마켓 산식 확정 전 S2 출력 금지

### 모픽
- 최종 class: `single_header_policy_gate`
- 포함: 작품별정산
- 제외: -
- 금액 상태: `needs_policy`
- 리스크: 외부 ID가 없어 제목 매칭 의존.
- 종결 gate: 총/순 매출 기준 확정 후 S2 출력

### 무툰
- 최종 class: `single_header_amount_policy_required`
- 포함: Sheet
- 제외: -
- 금액 상태: `needs_coin_policy`
- 리스크: 코인을 원화 판매금액으로 단정하면 안 됨.
- 종결 gate: 코인-원화 및 취소/공제 기준 확정 전 S2 출력 금지

### 문피아
- 최종 class: `single_header_policy_gate`
- 포함: 다우인큐브
- 제외: -
- 금액 상태: `needs_cancel_policy`
- 리스크: IOS/Google 매출이 총매출에 포함인지 검산 필요.
- 종결 gate: 취소/IOS/Google 포함 여부 확정 후 S2 출력

### 미소설
- 최종 class: `single_header_policy_gate`
- 포함: cpexcel*
- 제외: 확장체크/사람가공 파일은 fixture 제외
- 금액 상태: `needs_policy`
- 리스크: 수익률은 금액이 아니므로 정산기준액으로 쓰면 안 됨.
- 종결 gate: 지급액 컬럼 선택 기준 확정 후 S2 출력

### 미스터블루
- 최종 class: `mixed_sheet_policy_gate`
- 포함: 작품별 우선, 볼륨별은 검산/보조 후보
- 제외: 정산기준 없는 보조 시트는 출력 제외
- 금액 상태: `blocked_until_settlement_basis`
- 리스크: 작품별/볼륨별 2시트 구조. 정산기준액 후보가 약함.
- 종결 gate: 정산기준액 정의 전 S2 출력 금지

### 밀리의서재
- 최종 class: `single_header_policy_gate`
- 포함: list
- 제외: -
- 금액 상태: `candidate_confirmed_after_reconcile`
- 리스크: 콘텐츠명 중복/에피소드명 존재. 집계 키 결정 필요.
- 종결 gate: 에피소드명 집계 단위 확정 + 총액 reconcile 후 출력

### 보인&국립장애인도서관
- 최종 class: `non_s2_source_blocked`
- 포함: 없음
- 제외: 목록선정*
- 금액 상태: `blocked_non_sales`
- 리스크: 정산상세라기보다 구매/목록선정 자료.
- 종결 gate: S2 판매상세가 아니라 구매/목록선정 자료로 판단. 기본 출력 금지

### 부커스
- 최종 class: `single_header_amount_policy_required`
- 포함: CP 콘텐츠별 정산
- 제외: - 복사본 파일 제외
- 금액 상태: `needs_derived_sale_policy`
- 리스크: 판매금액 직접 컬럼 없음. 복사본 중복 위험.
- 종결 gate: 판매금액 파생식 확정 전 S2 출력 금지

### 북큐브
- 최종 class: `single_header_policy_gate`
- 포함: Sheet1
- 제외: -
- 금액 상태: `candidate_confirmed_after_reconcile`
- 리스크: 4월 샘플 없음. 할인/수수료 컬럼을 상계로 보낼지 정책 필요.
- 종결 gate: 대표월 fixture + 할인/수수료 상계 기준 확정 후 출력

### 북팔
- 최종 class: `mixed_sheet_policy_gate`
- 포함: 마켓, 날짜범위 시트
- 제외: Sheet 안내문/전자계산서 본문 시트 제외
- 금액 상태: `candidate_confirmed_after_reconcile`
- 리스크: 일부 Sheet는 정산 안내문이라 데이터로 먹이면 사고.
- 종결 gate: 보조 안내문 시트 제외 + 총액 reconcile 후 출력

### 블라이스
- 최종 class: `single_header_policy_gate`
- 포함: CP정산*
- 제외: -
- 금액 상태: `candidate_confirmed_after_reconcile`
- 리스크: 긴 병합 수수료 헤더 정규화 필요.
- 종결 gate: 수수료 컬럼명 정규화 + 총액 reconcile 후 출력

### 스낵북
- 최종 class: `single_header_policy_gate`
- 포함: settle_list_*
- 제외: -
- 금액 상태: `candidate_confirmed_after_reconcile`
- 리스크: 4월 샘플 없음. 코인타입별 분리 여부 확인 필요.
- 종결 gate: 대표월 fixture + 총액 reconcile 후 출력

### 스토린랩
- 최종 class: `multi_adapter_policy_gate`
- 포함: 원스, 원스(북패스), 무툰 시트
- 제외: 정리 시트(B2BC 집계형) 제외 또는 별도 수동
- 금액 상태: `needs_subadapter_policy`
- 리스크: 10개 스키마 그룹. 집계형 정리 시트와 상세 시트 혼재.
- 종결 gate: 서브어댑터별 amount rule 확정 전 S2 출력 금지

### 신영미디어
- 최종 class: `single_header_policy_gate`
- 포함: 날짜범위 시트
- 제외: -
- 금액 상태: `candidate_confirmed_after_reconcile`
- 리스크: 4월 샘플 없음.
- 종결 gate: 대표월 fixture + 총액 reconcile 후 출력

### 알라딘
- 최종 class: `single_header_policy_gate`
- 포함: sales_*
- 제외: -
- 금액 상태: `needs_cancel_policy`
- 리스크: 취소 행이 같은 표에 섞일 가능성.
- 종결 gate: 판매/취소 행 처리 정책 확정 후 S2 출력

### 알라딘 종이책
- 최종 class: `non_s2_source_blocked`
- 포함: 없음
- 제외: 거래처별 거래원장
- 금액 상태: `blocked_non_content_ledger`
- 리스크: 거래원장형 종이책 자료라 판매채널콘텐츠ID 매칭 단위가 맞지 않음.
- 종결 gate: 전자 S2 콘텐츠 판매상세 아님. 기본 출력 금지

### 에이블리
- 최종 class: `mixed_sheet_policy_gate`
- 포함: 상세 판매일 시트
- 제외: 행 레이블 pivot/요약 시트는 출력 제외
- 금액 상태: `candidate_confirmed_after_reconcile`
- 리스크: 상세 시트와 pivot 요약형 변형 혼재.
- 종결 gate: 요약 시트 제외 + 총액 reconcile 후 출력

### 에피루스
- 최종 class: `single_header_policy_gate`
- 포함: 월별 정산내역 시트
- 제외: -
- 금액 상태: `candidate_confirmed_after_reconcile`
- 리스크: 외부 ID 없음. 제목 매칭 의존.
- 종결 gate: 대표월 fixture + 수수료 상계 정책 확인 후 출력

### 예스24
- 최종 class: `single_header_policy_gate`
- 포함: Sheet1
- 제외: -
- 금액 상태: `needs_policy`
- 리스크: 판매가 후보가 2개이고 환불 컬럼 존재.
- 종결 gate: 판매가 기준/환불 처리 정책 확정 후 출력

### 올툰
- 최종 class: `single_header_amount_policy_required`
- 포함: 시트1
- 제외: -
- 금액 상태: `needs_fee_policy`
- 리스크: 코인, 총매출, 순매출, 수수료가 함께 있음.
- 종결 gate: 수수료 제외/순매출 기준 확정 후 S2 출력

### 원스토어
- 최종 class: `wide_header_policy_gate`
- 포함: multimedia
- 제외: -
- 금액 상태: `needs_policy`
- 리스크: wide schema라 금액 컬럼 문맥 보존 필요.
- 종결 gate: 판매/정액제/취소/수수료 산식 확정 전 S2 출력 금지

### 윌라
- 최종 class: `single_header_with_aux_exclusion`
- 포함: sheet
- 제외: 콘텐츠 가격 변동 이력 시트 제외
- 금액 상태: `needs_policy`
- 리스크: 2시트 구조. 가격 변동 이력은 정산상세 입력 아님.
- 종결 gate: 공급가/정가 판매금액 기준 확정 + 가격변동이력 제외 후 출력

### 조아라
- 최종 class: `multi_schema_amount_policy_required`
- 포함: 작품별 정산리스트*, 후원쿠폰*
- 제외: -
- 금액 상태: `needs_derived_sale_policy`
- 리스크: 5개 스키마. 단행본/후원쿠폰/이용권 변형 존재.
- 종결 gate: 단가 없는 그룹의 판매금액 정책 확정 전 S2 출력 금지

### 카카오
- 최종 class: `wide_header_policy_gate`
- 포함: 정산리포트_카카오페이지_*
- 제외: 미발행/선투자 파일은 중복 검토
- 금액 상태: `needs_tax_policy`
- 리스크: 세액/공급가액/공급대가 분리, 선투자 파일 혼재.
- 종결 gate: 과/면세 및 공급가액/공급대가 기준 확정 전 출력 금지

### 큐툰
- 최종 class: `single_header_amount_policy_required`
- 포함: Sheet
- 제외: -
- 금액 상태: `needs_coin_policy`
- 리스크: 무툰과 같은 코인형. 금액 의미 확정 필요.
- 종결 gate: 코인-원화 및 취소/공제 기준 확정 전 S2 출력 금지

### 토스(구루컴퍼니)
- 최종 class: `single_header_policy_gate`
- 포함: 정산_공급사_*
- 제외: -
- 금액 상태: `needs_tax_split_policy`
- 리스크: 콘텐츠 정산금액_면세/과세 분리 변형 존재.
- 종결 gate: 면세/과세 합산 기준 확정 후 출력

### 판무림
- 최종 class: `special_parser_required`
- 포함: 세부내역
- 제외: 표지 시트 제외
- 금액 상태: `needs_contextual_amount_policy`
- 리스크: 판매금액 헤더 반복. 표지는 보조/요약.
- 종결 gate: 상단 문맥+반복 판매금액 flatten 전 S2 출력 금지

### 피우리(누온)
- 최종 class: `single_header_policy_gate`
- 포함: Worksheet
- 제외: -
- 금액 상태: `candidate_confirmed_after_reconcile`
- 리스크: 4월 샘플 없음.
- 종결 gate: 대표월 fixture + 판매가/매출액 기준 확인 후 출력

### 피플앤스토리
- 최종 class: `single_header_policy_gate`
- 포함: 다운로드
- 제외: -
- 금액 상태: `candidate_confirmed_after_reconcile`
- 리스크: 4월 샘플 행 수가 적어 변형 검증력 낮음.
- 종결 gate: 샘플 행 적음. 다른 월 1개 추가 fixture 후 출력

### 하이북
- 최종 class: `single_header_policy_gate`
- 포함: 정산리스트
- 제외: -
- 금액 상태: `needs_purchase_rental_policy`
- 리스크: 구매/대여 분리 컬럼과 총합 컬럼이 함께 있음.
- 종결 gate: 구매/대여 합산 단위 확정 후 출력

### 한아름
- 최종 class: `special_parser_required`
- 포함: 건당 로그/시간 로그 반복 섹션
- 제외: 요약, 계산서 안내, 합계, 반복 헤더 row 제외
- 금액 상태: `needs_section_policy`
- 리스크: 한 시트 안에 요약/상세/계산서 안내/반복 헤더가 섞임.
- 종결 gate: 구간 파서 + 시간/건당 과금 정책 확정 전 S2 출력 금지

## 산출물

- `platform_schema_substitution_final.csv`: 37개 플랫폼별 최종 치환표
- `platform_schema_substitution_final_groups.csv`: 80개 플랫폼-스키마 그룹별 최종 action/치환표
- `platform_schema_substitution_final_adversarial_audits.csv`: 플랫폼별 3회 감리 결과, 총 111행
- `platform_schema_substitution_representative_rescan.csv`: 대표 샘플 재스캔 근거
- `platform_schema_substitution_final_summary.json`: 집계 요약
