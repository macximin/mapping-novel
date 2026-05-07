# KISS 지급 정산 관리 목록 최신화 흐름

## 판단
- `지급_정산_관리_목록`은 기존 S2 콘텐츠 리스트와 성격이 다르다.
- 이 파일은 지급정산마스터/상세 기준이며 `콘텐츠ID`와 `판매채널콘텐츠ID`를 함께 포함한다.
- 따라서 정산서 매핑용 S2 lookup을 최신화하는 보조 캐시로 쓸 수 있다.

## 샘플 구조
- 샘플 파일: `temp/지급_정산_관리_목록 (2).xlsx`
- 행 수: 1,000
- 컬럼 수: 33
- 콘텐츠형태: 소설 1,000
- 지급정산상태: 운영중 1,000
- 콘텐츠ID: 1,000 nonblank / 182 unique
- 판매채널콘텐츠ID: 997 nonblank / 968 unique
- 등록일 범위: 2026-04-24 18:41:59 ~ 2026-05-07 15:54:34

## 구현
- `kiss_payment_settlement.py`
  - KISS xlsx style XML 오류를 피하는 OOXML fallback reader
  - 지급 정산 관리 목록 필수 컬럼 검증
  - S2 lookup 변환
  - 기존 캐시와 merge
- `scripts/import_kiss_payment_settlement.py`
  - 다운로드 엑셀을 로컬 캐시로 반영
  - `data/kiss_payment_settlement_cache.csv`
  - `data/kiss_payment_settlement_s2_lookup.csv`
- `app.py`
  - S2 콘텐츠 리스트 업로드
  - 지급 정산 관리 목록 업로드
  - 로컬 지급정산 lookup 캐시 사용

## 운영 주의
- URL의 `pageSize=1000`은 최신 1,000건만 받을 가능성이 있다.
- 전체 최신화를 하려면 `pageNum=1,2,3...` 페이지를 끝까지 내려받거나, 기존 캐시와 계속 merge해야 한다.
- 캐시는 `지급정산상세ID` 기준으로 dedupe한다.
- S2 lookup은 `판매채널콘텐츠ID` 기준으로 dedupe한다.
- 지급 정산 관리 목록은 S2 검색 목록의 완전 대체재가 아닐 수 있으므로, 당장은 보조/캐시 lookup으로 운용한다.
