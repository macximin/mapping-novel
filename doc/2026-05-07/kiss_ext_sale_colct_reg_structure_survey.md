# KISS 외부매출 수집 등록 화면 구조 분석

- 대상 URL: `https://kiss.kld.kr/sale/ext/ext-sale-colct-reg`
- 분석일: 2026-05-07
- 목적: KISS 외부매출 수집 등록 화면의 구조와 내부 API 흐름을 파악하고, `mapping-novel`에서 S2 콘텐츠 리스트 수동 다운로드를 제거할 수 있는지 검토한다.
- 보안 원칙: 로그인 ID, 비밀번호, 토큰, 실제 거래처 코드/판매채널 ID 값은 문서에 기록하지 않는다.

## 결론

`ext-sale-colct-reg`는 서버 렌더링 화면이 아니라 Vue SPA 라우트다. 브라우저가 정적 HTML을 받은 뒤 `index-DhALS8nb.js`에서 라우트를 찾고, 실제 화면은 `ext-sale-colct-reg-fmAHl6Mk.js` 청크를 동적 로드한다.

화면 뒤에는 내부 REST API가 있다. API base는 `https://kiss-api.kld.kr`이고, 로그인 후 Bearer 토큰으로 접근한다. 따라서 "사이트에서 사람이 파일을 내려받아 업로드해야만 한다"는 구조라기보다는, 현재 화면이 사람이 API를 쓰도록 감싼 형태에 가깝다.

키다리스튜디오 소설만 대상으로 좁힐 경우 핵심 코드는 `ctnsStleCd=102`다. 프론트 상수에서 `101=웹툰`, `102=웹소설`, `301=영상` 등이 확인된다. 이 값과 거래처/판매채널 조건을 고정하면 판매채널콘텐츠 검색 API를 직접 호출해서 S2 콘텐츠 리스트를 자동 확보할 가능성이 높다.

단, 등록/저장 API는 회계/전자결재/전표분할까지 엮여 있다. `mapping-novel` 1차 개선 범위에서는 쓰기 API를 건드리지 말고, 읽기 API로 판매채널콘텐츠 목록을 가져와 매핑 입력을 자동화하는 쪽이 안전하다.

## 분석 근거

### SPA 라우트

초기 HTML은 화면 본문을 포함하지 않고 정적 자산만 참조한다.

- HTML: `/sale/ext/ext-sale-colct-reg`
- 메인 번들: `/assets/index-DhALS8nb.js`
- 라우트 청크: `/assets/ext-sale-colct-reg-fmAHl6Mk.js`

`index-DhALS8nb.js`에서 확인된 관련 라우트는 다음과 같다.

| 라우트 | 의미 |
| --- | --- |
| `/sale/ext/ext-salm` | 외부매출현황 목록 |
| `/sale/ext/ext-sale-colct-reg` | 외부 매출 등록 |
| `/sale/ext/ext-sale-colct-dtls` | 외부 매출 상세 |
| `/sale/ext/ext-sale-colct-updt` | 외부 매출 수정 |
| `/sale/ext/popup/schn-ctns-search-popup` | 판매채널콘텐츠 검색 팝업 |
| `/sale/ext/popup/ext-sale-colct-schn-ctns-upload-popup` | 판매채널콘텐츠 ID 기반 매출 등록 팝업 |
| `/sale/ext/popup/ext-sale-colct-extrl-upload-popup` | 외부판매사 ID 기반 매출 등록 팝업 |

### 인증 모델

기존 사내 자동화 코드 기준 KISS 설정은 다음과 같다.

| 항목 | 값 |
| --- | --- |
| Web base | `https://kiss.kld.kr` |
| API base | `https://kiss-api.kld.kr` |
| API login path | `/user/login` |
| User info path | `/user/info` |
| Company code | `1000` |

로그인 흐름은 `POST /user/login`에 `username`, `password`, `cprCd`를 보내고 응답 JWT를 `Authorization: Bearer ...`로 붙이는 방식이다. 기존 로컬 자동화 환경의 인증 함수로 로그인 성공만 확인했고, 인증 정보 값은 기록하지 않았다.

## 화면 구조

`ext-sale-colct-reg` 화면 제목은 `외부 매출 등록`이다. 화면은 크게 다음 영역으로 구성된다.

| 영역 | 역할 |
| --- | --- |
| 거래처 정보 | 거래처 검색/선택, 거래처명/코드/사업자번호/구분/연락처/이메일/주소 표시 |
| 콘텐츠 목록 | 판매채널콘텐츠 또는 외부판매사 ID를 기준으로 매출 행 등록 |
| 매출정보 | 판매년월, 세금계산서/인보이스 일자, 정산항목, 통화, 환율, 금액, 세금, 적요, 코스트센터 입력 |
| 분할정보 | 전표분할 사용 시 분할 금액/분개유형/코스트센터/적요 입력 |
| 하단 액션 | 취소, 임시저장, 결재요청 |

콘텐츠 목록 영역의 주요 버튼은 다음과 같다.

| 버튼 | 동작 |
| --- | --- |
| 판매채널콘텐츠 검색 | 검색 팝업을 열어 KISS 내부 판매채널콘텐츠를 선택 |
| 판매채널콘텐츠 매출 등록 | 판매채널콘텐츠ID, 판매금액, 정산기준액, 상계금액을 엑셀/그리드로 입력 후 서버 검증 |
| 외부 판매사ID 등록 | 외부판매사ID, 외부판매사 콘텐츠명, 금액을 입력 후 서버 매핑 검증 |
| 행삭제 | 그리드 선택 행 삭제 |
| 엑셀 다운로드 | 현재 콘텐츠 목록 그리드 다운로드 |

## 주요 그리드 스키마

### 등록/수정 화면 콘텐츠 목록

| 필드 | 화면명 | 비고 |
| --- | --- | --- |
| `schnCtnsId` | 판매채널콘텐츠ID | KISS/S2 쪽 핵심 입력 ID |
| `schnId` | 판매채널ID | 판매채널 식별자 |
| `schnNm` | 판매채널명 | 표시용 |
| `ctnsId` | 콘텐츠ID | 콘텐츠마스터 ID |
| `ctnsNm` | 콘텐츠명 | 표시용 |
| `extSleCtnsCd` | 외부판매사ID | 외부 판매사 기준 매핑 ID |
| `extSleCtnsNm` | 외부판매사 콘텐츠명 | 표시용 |
| `sleAmt` | 판매금액 | 숫자, 편집 가능 |
| `setlStdAmt` | 정산기준액 | 숫자, 편집 가능 |
| `stoffAmt` | 상계금액 | 숫자, 편집 가능 |
| `errorType` | 에러 | 검증 실패 표시용 숨김 필드 |

### 판매채널콘텐츠 검색 팝업

검색 조건:

| 필드 | 의미 |
| --- | --- |
| `bcncCd` | 거래처 코드, 팝업 prop으로 전달 |
| `schnIds` | 판매채널 다중 선택 |
| `ctnsStleCd` | 콘텐츠형태 코드 |
| `ctnsNm` | 콘텐츠명 검색어 |

결과 컬럼:

| 필드 | 화면명 |
| --- | --- |
| `schnCtnsId` | 판매채널콘텐츠ID |
| `schnId` | 판매채널ID |
| `ctnsId` | 콘텐츠ID |
| `schnNm` | 판매채널명 |
| `ctnsNm` | 콘텐츠명 |
| `ctnsStleCdNm` | 콘텐츠형태 |

### 판매채널콘텐츠 매출 등록 팝업

입력 컬럼:

| 필드 | 화면명 |
| --- | --- |
| `schnCtnsId` | 판매채널콘텐츠ID |
| `sleAmt` | 판매금액 |
| `setlStdAmt` | 정산기준액 |
| `stoffAmt` | 상계금액 |
| `isMapped` | 매핑여부 |
| `errorType` | 에러 |

주의: 이 청크의 fields 정의에는 `schnCtnsID`처럼 대문자 `ID`가 섞여 있는 부분이 보인다. 컬럼 정의는 `schnCtnsId`라서 실제 동작에 문제가 없을 수 있지만, 자동화 구현 시 응답/요청 필드명을 반드시 실제 API 응답 기준으로 검증해야 한다.

### 외부판매사 ID 매출 등록 팝업

입력 컬럼:

| 필드 | 화면명 |
| --- | --- |
| `extSleCtnsCd` | 외부판매사ID |
| `extSleCtnsNm` | 외부판매사 콘텐츠명 |
| `sleAmt` | 판매금액 |
| `setlStdAmt` | 정산기준액 |
| `stoffAmt` | 상계금액 |
| `isMapped` | 매핑여부 |
| `errorType` | 에러 |

## API 맵

### 읽기 API

| 메서드 | 경로 | 용도 |
| --- | --- | --- |
| `GET` | `/sale/ext/ext-salm` | 외부매출현황 목록 조회 |
| `GET` | `/sale/ext/ext-salm/{extSalmId}` | 외부매출 단건 기본정보 조회 |
| `GET` | `/sale/ext/ext-salm/{extSalmId}/sale-colct` | 외부매출 단건의 콘텐츠 목록 조회 |
| `GET` | `/sale/ext/ext-salm/{extSalmId}/split` | 전표분할 정보 조회 |
| `GET` | `/sale/ext/ext-salm/schn-ctns` | 판매채널콘텐츠 검색 |
| `GET` | `/ssmgmt/cmm/schn` | 거래처 기준 판매채널 목록 조회 |
| `GET` | `/mst/cmmi/exchr/exchr-by-search` | 환율 조회 |
| `GET` | `/mst/cmmi/nation-whtax-taxrt/rate` | 국가별 원천세율 조회 |
| `GET` | `/mst/cmmi/jrnlz-typ-cd` | 분개유형 코드 조회 |

### 쓰기/검증 API

| 메서드 | 경로 | 용도 |
| --- | --- | --- |
| `POST` | `/sale/ext/ext-salm` | 외부매출 신규 저장 |
| `PUT` | `/sale/ext/ext-salm/{extSalmId}/save-temp` | 외부매출 임시저장/수정 저장 |
| `POST` | `/sale/ext/ext-salm/split` | 전표분할 정보 저장 |
| `PUT` | `/sale/ext/ext-salm/{extSalmId}/elap-req` | 전자결재 요청 |
| `POST` | `/sale/ext/ext-salm/check-schn-ctns/{bcncCd}` | 판매채널콘텐츠ID 기반 매출 행 검증/보강 |
| `POST` | `/sale/ext/ext-salm/mapping-extrl-schn-ctns/{bcncCd}` | 외부판매사ID 기반 매출 행 매핑 검증/보강 |
| `DELETE` | `/sale/ext/ext-salm/{extSalmId}` | 외부매출 삭제 |
| `PUT` | `/sale/ext/ext-salm/iss-txbil` | 세금계산서 조회/발행 연계 |
| `PUT` | `/sale/ext/ext-salm/iss-invc` | 인보이스 발행 연계 |
| `PUT` | `/sale/ext/ext-salm/{extSalmId}/resend-slip` | ERP 전표 재발행 |

## 읽기 API 실측

인증된 세션으로 읽기 API만 호출해 확인했다.

| 확인 항목 | 결과 |
| --- | --- |
| 로그인 | 성공 |
| `GET /sale/ext/ext-salm`, 2026-04 범위 | 157행 |
| 첫 외부매출의 `sale-colct` 상세 | 41행 |
| 거래처 기준 판매채널 조회 | HTTP 200 |
| 웹소설 채널 존재 여부 | 확인됨 |
| `GET /sale/ext/ext-salm/schn-ctns`, `ctnsStleCd=102` | HTTP 200 |
| 웹소설 전체 검색 샘플 | 1,299행 |
| 특정 웹소설 판매채널 검색 샘플 | 505행 |

목록 응답의 대표 필드는 다음과 같다.

```text
bcncCd, bcncNm, chgerNm, chrgDeptNm, crrxAmt, crrxAmtInclsYnNm,
elapDataKey, elapDocId, elapStsCd, elapStsCdNm, exchr, extSalmId,
extSalmTyCd, extSalmTyCdNm, moduleMngNo, saleAmt, setlCrcy,
setlLastAmt, setlStdAmt, setlStdMdatAmt, sleAmt, sleYm, slipNo,
slipProcStsCd, slipProcStsCdNm, srcUntAmt, stoffAmt, taxTyCd,
taxTyCdNm, txbilNo, txbilPubTyCd, txbilPubTyCdNm, updatedAt, vatAmt
```

상세 콘텐츠 응답의 대표 필드는 다음과 같다.

```text
chgerNm, chrgDeptNm, ctnsId, ctnsNm, extSchnCtnsMapgId,
extSleCtnsCd, extSleCtnsNm, pencNm, schnCtnsId, schnId, schnNm,
setlStdAmt, sleAmt, stoffAmt, wrtrNm
```

판매채널콘텐츠 검색 응답의 대표 필드는 다음과 같다.

```text
ctnsId, ctnsNm, ctnsStleCdNm, schnCtnsId, schnId, schnNm
```

## `mapping-novel`에 연결할 때의 판단

현재 `mapping-novel`의 불편점 1번, 즉 "S2 콘텐츠 리스트를 사용자가 직접 다운로드해야 하는 문제"는 읽기 API로 제거할 수 있을 가능성이 높다.

우선순위는 다음이 적절하다.

1. KISS 로그인 세션 생성은 기존 사내 자동화 코드의 방식을 재사용한다.
2. 키다리스튜디오 소설 조건을 고정한다.
   - 콘텐츠형태: `ctnsStleCd=102`
   - 거래처/판매채널: 운영에서 쓰는 값만 allowlist로 관리
3. `GET /ssmgmt/cmm/schn`으로 거래처의 판매채널을 확인한다.
4. `GET /sale/ext/ext-salm/schn-ctns`로 판매채널콘텐츠 목록을 가져온다.
5. 가져온 목록을 기존 S2 업로드 파일과 동일한 내부 스키마로 변환한다.
6. 처음에는 "자동 수집 + 미리보기 + 다운로드/매핑"까지 읽기 전용으로 제공한다.
7. 등록/저장/결재 API는 별도 승인 전까지 건드리지 않는다.

## 적대적 감리

### 1차: API가 있다고 바로 자동화해도 되는가

아니다. 읽기 API는 자동화 후보지만, 저장 API는 회계/전자결재/ERP 전표와 연결되어 있다. 특히 `POST /sale/ext/ext-salm`, `PUT /elap-req`, `POST /split`은 잘못 호출하면 실제 업무 데이터가 생성되거나 결재가 올라갈 수 있다. 1차 범위는 판매채널콘텐츠 목록 수집과 매핑 보조로 제한해야 한다.

### 2차: `ctnsStleCd=102`만으로 키다리스튜디오 소설이 보장되는가

보장되지 않는다. `102`는 웹소설이라는 콘텐츠형태일 뿐이고, 키다리스튜디오 여부는 거래처/판매채널/귀속법인 조건과 함께 검증해야 한다. 자동화에서는 `ctnsStleCd=102`에 더해 허용된 거래처 코드와 판매채널 ID를 설정으로 고정하고, 결과 행의 채널명을 검증해야 한다.

### 3차: 판매채널콘텐츠 검색 API만 있으면 기존 매핑 문제가 해결되는가

아니다. API는 S2 수동 다운로드 문제를 줄여주지만, 제목 정규화와 중복 후보 문제는 별도다. 판매채널콘텐츠 목록을 자동으로 가져와도 같은 정제 제목에 여러 `ctnsId`/`schnCtnsId`가 있으면 자동 확정하면 안 된다. 현재 개선 브랜치처럼 `ambiguous`, `no_match`, `matched` 상태를 분리하는 방향이 필요하다.

## 남은 확인 사항

- 운영에서 "키다리스튜디오 소설"로 간주할 정확한 거래처 코드/판매채널 ID 목록 확정
- KISS 계정별 권한 차이에 따른 API 응답 범위 확인
- `schnIds` 배열 파라미터 직렬화 방식 고정
- 판매채널콘텐츠 검색 API의 대량 조회 제한, 타임아웃, 페이징 여부 확인
- 자동 수집한 S2 목록의 캐시 주기 결정
- 읽기 API 실패 시 기존 수동 업로드 fallback 유지 여부 결정
- Streamlit 배포 시 `.env`/인증정보 주입 방식 결정

## 권장 실행안

1차 구현은 "KISS에서 키다리스튜디오 웹소설 판매채널콘텐츠 목록 자동 수집" 기능으로 자르는 게 맞다.

- 쓰기 API 호출 없음
- 로그인 후 읽기 API만 사용
- 수집 결과를 기존 S2 DataFrame 스키마로 변환
- 앱 화면에는 수집 행 수, 판매채널, 콘텐츠형태, 마지막 수집시각을 표시
- 실패하면 명확한 오류와 수동 업로드 fallback 제공
- 수집 결과와 매핑 결과의 중복/미매핑 리포트는 별도 시트 유지

이 범위면 사용자에게 파일을 수동 다운로드시키는 가장 큰 병목은 줄이면서, KISS의 회계/결재 쪽 위험 영역은 건드리지 않을 수 있다.
