# mapping-novel Survey / Adversarial Audit

Date: 2026-05-07
Target: `C:\Users\wjjo\Desktop\mapping-novel`
Reference commit: `46ad3bdb6617c79fe00914012ca9cd0eecafd436`

## Executive Summary

현재 앱은 Streamlit으로 정산서, S2 콘텐츠 리스트, IPS 콘텐츠마스터 엑셀을 받아 정제 제목 기준으로 ID를 붙이는 도구다. 그러나 운영 관점에서는 네 가지 문제가 크다.

1. S2 콘텐츠 리스트를 사용자가 KISS에서 직접 조회/다운로드/재저장해서 업로드해야 한다.
2. 실제 사용 범위는 키다리스튜디오 소설인데 UI는 레진KR/웹툰까지 선택하게 한다.
3. 제목 정제 로직이 숫자, 괄호, 구분자, 키워드를 너무 공격적으로 삭제한다.
4. 매핑 결과가 성공/실패/중복/미매핑을 분리하지 않고 fallback 문자열로 섞는다.

가장 위험한 결론은 이것이다.

현재 앱은 "틀리면 멈추는 도구"가 아니라 "틀려도 그럴듯한 엑셀을 만들어주는 도구"에 가깝다.

## Current Flow

근거:

- `app.py:95-100`: S2 콘텐츠 리스트 업로드 UI
- `app.py:103-107`: 플랫폼별 정산서 업로드 UI
- `app.py:109-136`: 콘텐츠마스터 법인 선택 UI
- `app.py:165-193`: 세 엑셀을 읽고 제목 정제 후 map 수행
- `app.py:295-330`: 결과 엑셀 생성/다운로드

현재 흐름:

1. 사용자가 `https://kiss.kld.kr/mst/sch/schn-ctns-search` 에서 판매채널을 선택 조회한다.
2. 사용자가 S2 콘텐츠 리스트 엑셀을 다운로드한다.
3. 사용자가 해당 파일을 열고 "다른 이름으로 저장"한다.
4. 사용자가 앱에 S2 파일을 업로드한다.
5. 사용자가 플랫폼 정산서를 업로드한다.
6. 사용자가 `키다리스튜디오 소설`, `레진KR`, `키다리스튜디오 웹툰` 중 하나를 선택한다.
7. 앱은 선택값에 따라 `data/kidari_contents.xlsx`, `data/lezhin_contents.xlsx`, `data/kidari_webtoon.xlsx` 중 하나를 읽는다.
8. `clean_title()` 정제 키로 S2와 IPS를 각각 매핑한다.
9. 결과 엑셀을 만든다.

## Data Survey

Repo data files:

| File | Sheet | Rows | Main Columns |
|---|---:|---:|---|
| `data/kidari_contents.xlsx` | `콘텐츠 목록` | 28,626 | `콘텐츠ID`, `콘텐츠형태`, `귀속법인`, `콘텐츠명`, `서비스유형`, `작가필명`, `담당부서`, `담당자명` |
| `data/lezhin_contents.xlsx` | `콘텐츠 목록` | 29,340 | same broad schema |
| `data/kidari_webtoon.xlsx` | `콘텐츠 목록` | 30,764 | same broad schema |
| `data/all_contents.xlsx` | `콘텐츠 목록` | 59,251 | same broad schema |
| `data/lezhinjp_contents.xlsx` | `Sheet1` | sampled only | `콘텐츠ID`, `콘텐츠명` |

`kidari_contents.xlsx` is already only:

- `귀속법인 = 키다리스튜디오`
- `콘텐츠형태 = 소설`

This supports removing the legal-entity selector if the operational target is only Kidari novel.

## Survey 1: S2/KISS Manual Download Is the Biggest UX Problem

근거:

- `app.py:96-99`

Current UI literally asks the user to download S2 content list from KISS:

```text
https://kiss.kld.kr/mst/sch/schn-ctns-search 에서 판매채널을 선택 조회 후 엑셀 다운로드
※ S2에서 다운로드한 파일은 '열기' -> '다른 이름으로 저장' 후 업로드
```

Problems:

- 사용자가 판매채널을 잘못 고르면 앱이 알 수 없다.
- 다운로드 시점이 낡아도 앱이 알 수 없다.
- "열기 -> 다른 이름으로 저장" 같은 엑셀 파일 위생 작업을 사용자에게 떠넘긴다.
- Streamlit Cloud 같은 공개/공유 앱으로 만들면 사용자별 KISS 권한, 로그인, 다운로드 과정이 전부 앱 밖으로 빠진다.
- 매핑 실패 원인이 앱 로직인지, 오래된 S2 파일인지, 잘못 받은 S2 파일인지 구분이 어렵다.

Nearby automation assets:

`SIAAN Project`에는 이미 KISS API 로그인/다운로드 자동화 흔적이 있다.

- `SIAAN Project/download_kiss_pymt_setl.py`
- `SIAAN Project/download_kiss_ext_sales_batch.py`
- `SIAAN Project/ips/sites/kiss.py`
- `SIAAN Project/ips_harness.py`

이 앱 안에는 아직 `schn-ctns-search` 직접 수집 로직이 없지만, 완전 신규가 아니라 기존 KISS 자동화 자산을 참고할 수 있다.

Recommended direction:

1. Best: S2 콘텐츠 리스트를 KISS API/내부 배치로 자동 수집한다.
2. Middle: Streamlit에서 판매채널만 고르면 서버가 최신 S2 목록을 가져오게 한다.
3. Minimum: 수동 업로드를 유지하더라도 파일 검증을 강하게 한다.

Minimum validation should include:

- required columns
- row count
- 판매채널명/판매채널ID
- download date or freshness marker if available
- duplicate normalized title count
- unmatched rate preview before download

## Survey 2: Kidari Novel Only, but UI Allows Wrong Corporate Master

근거:

- `app.py:109-120`
- `app.py:123-136`

Current mapping:

```python
mapping3 = {
    "키다리스튜디오 소설": "kidari_contents.xlsx",
    "레진KR": "lezhin_contents.xlsx",
    "키다리스튜디오 웹툰": "kidari_webtoon.xlsx",
}
```

Problem:

The operational requirement is "키다리스튜디오 소설만 사용". The app still lets users pick Lezhin KR or Kidari Webtoon. This creates a user-driven false-success path: wrong master selected, app still produces an Excel.

Recommended direction:

- Remove the selector.
- Hardcode master path to `data/kidari_contents.xlsx`.
- Display master metadata instead of asking for choice:
  - file name
  - row count
  - last updated date
  - `귀속법인`
  - `콘텐츠형태`

If future multi-entity support is needed, it should be behind an admin/config mode, not an everyday operator choice.

## Survey 3: Text Hygiene Logic Is Unsafe

근거:

- `app.py:31-89`

The current `clean_title()` is a single destructive function. It removes punctuation, underscores, digits, brackets, and many keywords. It is useful for rough matching, but unsafe as the only key generator.

Observed outputs:

| Input | Current Output |
|---|---|
| `작품명_작가명_123_456_확정` | `작품명작가명확정` |
| `<작품명>_작가명_123_456_확정` | `작품명작가명확정` |
| `그 남자의 비밀_홍길동_1001234_2002345_확정` | `그남자의비밀홍길동확정` |
| `<그 남자의 비밀>_홍길동_1001234_2002345_확정` | `그남자의비밀홍길동확정` |
| `너_그리고_나_홍길동_1001234_2002345_확정` | `너그리고나홍길동확정` |
| `<너_그리고_나>_홍길동_1001234_2002345_확정` | `너그리고나홍길동확정` |
| `1983 1화` | empty string |
| `99강화나무몽둥이` | `강화나무몽둥이` |

Issues:

- `<작품명>` syntax currently gives no benefit. `<` and `>` are removed; the parser does not extract inside text.
- Legacy structured IPS names are not parsed. Author/code/status leak into the title key.
- `re.sub(r"\d+", "", t)` deletes all digits, including title-identifying digits.
- Parentheses/brackets are removed wholesale, even when they contain original title, subtitle, or distinguishing metadata.
- keyword removal uses plain `replace`, not token-aware removal.
- exceptions use substring matching and immediately return the exception value.

Recommended direction:

Split normalization into stages:

1. Source-specific parser:
   - S2 title parser
   - settlement title parser
   - IPS content master parser
2. Structured IPS extraction:
   - `<작품명>_작가명_저작권코드_선인세코드_확정` -> `작품명`
   - `작품명_작가명_저작권코드_선인세코드_확정` -> `작품명`
3. Conservative title normalizer:
   - remove episode/volume suffixes only when unit-bound
   - preserve meaningful digits
   - keep aliases separately
4. Golden tests:
   - number titles
   - date titles
   - bracket bilingual titles
   - IPS structured names
   - known exception titles

## Survey 4: Mapping Logic Collapses Status Into Values

근거:

- `app.py:182-186`
- `app.py:189-193`

Current mapping:

```python
df2["매핑결과"] = df2["정제_상품명"].map(map1).fillna(df2["정제_상품명"])
df2["최종_매핑결과"] = df2["정제_상품명"].map(map3).fillna(df2["매핑결과"])
```

This makes one column carry multiple meanings:

- S2 matched -> sales-channel content ID
- S2 failed -> normalized product name
- IPS matched -> content master ID
- IPS failed but S2 matched -> sales-channel content ID
- both failed -> normalized product name

Later, `최종_매핑결과` is renamed to `매핑_콘텐츠마스터ID` at `app.py:286`. Therefore, a column named "content master ID" may contain:

- actual content master ID
- sales-channel content ID
- normalized title string

This is a correctness failure, not a presentation issue.

Recommended direction:

Never use fallback values inside ID columns. Use explicit columns:

- `정제_상품명`
- `S2_매칭상태`
- `S2_판매채널콘텐츠ID`
- `IPS_매칭상태`
- `IPS_콘텐츠마스터ID`
- `후보수`
- `미매핑사유`
- `검토필요(Y/N)`

## Survey 5: Duplicate Keys Are Structural, Not Rare

근거:

- `app.py:182-190`
- local data survey of `data/*.xlsx`

Current app uses:

```python
df3.drop_duplicates("정제_콘텐츠3명").set_index("정제_콘텐츠3명")[id3]
```

This silently keeps the first row for duplicated normalized titles.

Observed duplicate normalized keys:

| File | Rows | Blank Keys | Duplicate Rows | Duplicate Keys |
|---|---:|---:|---:|---:|
| `kidari_contents.xlsx` | 28,626 | 15 | 5,011 | 1,901 |
| `lezhin_contents.xlsx` | 29,340 | 15 | 9,054 | 2,918 |
| `kidari_webtoon.xlsx` | 30,764 | 15 | 6,929 | 2,498 |
| `all_contents.xlsx` | 59,251 | 30 | 13,103 | 4,561 |

Examples from `kidari_contents.xlsx`:

- `우선남동생부터숨기자`: 본편, 특별 외전, 외전 세트, 카카오, 원작, 원스토어 variants all collapse.
- `금발의정령사`: 본편, 1부, 2부, 3부, 세트, 카카오 variants collapse.
- `엉큼한맞선`: 본편, 세트, 카카오 특별 해제, 원작, 오디오북 variants collapse.

Some duplicate keys also span multiple authors/departments after normalization:

- `매지컬써전`: 6 rows, 6 distinct author labels
- `헌터의신석기`: 6 rows, 6 distinct author labels
- `스폰서`: 6 rows, 5 distinct author labels
- `밤의꽃`: 5 rows, 4 distinct author labels, 3 departments

Recommended direction:

- Treat duplicate normalized keys as ambiguous by default.
- Do not auto-select first row.
- Generate candidate sheet:
  - normalized key
  - candidate count
  - candidate IDs
  - candidate titles
  - author labels
  - service type
  - department
  - reason why auto-selection is blocked

## Survey 6: Output Workbook Mixes Row-Level Results and Summary Lists

근거:

- `app.py:195-226`
- `app.py:257-287`

`pairs_unique` and `pairs_same` are sorted independent lists. They are then placed at the top of `df2`:

```python
df2["매핑콘텐츠명"] = list(pairs_unique["매핑콘텐츠명"]) + [""] * pad_u
df2["콘텐츠ID"] = list(pairs_unique["콘텐츠ID"]) + [""] * pad_u
```

This means the row relationship is broken. A value in `매핑콘텐츠명` may not belong to the same settlement row.

Also, rename creates duplicate output column names:

```python
"콘텐츠ID" -> "매핑_콘텐츠마스터ID"
"최종_매핑결과" -> "매핑_콘텐츠마스터ID"
```

Recommended direction:

Use separate sheets:

- `입력검증`
- `행별매핑결과`
- `중복후보`
- `미매핑`
- `정제키_진단`

Row-level sheet must preserve original row index.

## Survey 7: Hidden Columns Are Not a Safety Mechanism

근거:

- `app.py:295-324`

The app hides every column not included in `visible`. But hidden columns remain in the Excel file.

Risks:

- settlement input may contain sales, revenue, quantity, or other sensitive columns
- hidden diagnostic columns are easy to miss
- users may not know the file contains more data

Recommended direction:

- Explicitly select output columns.
- Put diagnostics in visible audit sheets.
- Remove unnecessary raw columns from delivery workbook.

## Survey 8: Error Handling and Test Coverage Are Thin

근거:

- `app.py:165-174`
- `app.py:308-313`
- repo has no tests
- README is only two lines

Problems:

- S2 ID column is hardcoded as `판매채널콘텐츠ID`, while other ID columns use candidates.
- empty input after filtering can break width calculation at `result.iloc[0, col_idx]`.
- Excel parse failures become raw Streamlit exceptions.
- no regression tests for `clean_title()`.
- no sample input/output.
- no documented data refresh process.

Recommended direction:

- Add schema validation layer.
- Add friendly error messages.
- Add unit tests for title normalization and matching status.
- Add smoke test for a tiny sample workbook.
- Add README with run, deploy, data refresh, expected input schema.

## Proposed North Star

The target app should not be "upload two files and hope". It should be an auditable mapping workflow:

1. 정산서 업로드
2. 판매채널 감지 또는 선택
3. S2 콘텐츠 리스트 자동 수집 or validated latest cache
4. 키다리스튜디오 소설 master 고정 로드
5. 입력 검증
6. 정제 키 생성
7. exact/structured match
8. ambiguous candidate split
9. preview summary
10. workbook export with audit sheets

## Adversarial Audit Round 1: "Are We Overreacting? Maybe It Works Enough"

Challenge:

The current app may have been used successfully because many settlement titles are simple. The destructive normalizer may intentionally collapse platform variants and episode suffixes.

Counter:

The issue is not that loose normalization exists. The issue is that loose normalization is treated as a definitive ID selector. `kidari_contents.xlsx` has 1,901 duplicated normalized keys. This is too common to silently keep first row.

Verdict:

Loose normalization can stay as a candidate generator, but not as a final selector.

## Adversarial Audit Round 2: "Do We Really Need S2 Automation Now?"

Challenge:

KISS automation may require credentials, session handling, and security review. Manual upload is simpler and safer in the short term.

Counter:

Manual upload can remain as fallback, but the current app does not validate the uploaded S2 file strongly enough. It asks users to do fragile work, then provides little feedback if they did it wrong.

Verdict:

Immediate requirement is not necessarily full automation. Immediate requirement is to stop trusting manual S2 upload blindly. Add validation and freshness visibility first; automate next.

## Adversarial Audit Round 3: "If We Only Use Kidari Novel, Is This Easy?"

Challenge:

Removing the legal-entity selector and fixing `<작품명>` parsing may seem enough.

Counter:

Even `kidari_contents.xlsx` alone has major duplicate key collisions. Hardcoding Kidari novel removes one UX failure path, but it does not fix false-positive ID selection.

Verdict:

Kidari-only is a good simplification, but not the whole fix. The matching model still needs explicit statuses, ambiguous candidate handling, and audit sheets.

## Prioritized Improvement Plan

### P0: Stop Silent Wrong Results

- Split ID values from match status.
- Remove fallback of normalized title into ID columns.
- Block or flag duplicate normalized keys.
- Preserve row-level alignment.
- Remove duplicate output column names.

### P1: Make It Kidari Novel Only

- Remove legal-entity selectbox.
- Always use `data/kidari_contents.xlsx`.
- Show master metadata.
- Remove unused Lezhin/Webtoon data from deployment or keep only in archive.

### P2: Fix S2 Flow

- Add S2 schema validation.
- Add upload summary.
- Add duplicate/missing key diagnostics.
- Reuse KISS automation assets where possible.
- Eventually replace manual download with API/cache.

### P3: Replace One Big `clean_title()`

- Add source-specific parsing.
- Add IPS structured title extraction.
- Preserve meaningful digits.
- Convert aggressive deletion rules into tested, explicit cases.
- Add golden tests.

### P4: Make Output Auditable

- Separate result sheets.
- Make diagnostics visible.
- Export only whitelisted delivery columns.
- Include summary counts and warnings.

## Appendix: Specific Findings by File/Line

| Area | File/Line | Finding |
|---|---|---|
| S2 manual flow | `app.py:95-100` | User is instructed to download S2 list manually from KISS and resave it |
| Entity selector | `app.py:109-120` | UI allows three masters despite Kidari novel-only requirement |
| Master load | `app.py:165-168` | master xlsx loaded every run without caching |
| S2 ID hardcode | `app.py:182-185`, `app.py:244-251` | `판매채널콘텐츠ID` hardcoded |
| destructive normalization | `app.py:31-89` | one function performs parsing and destructive cleanup |
| duplicate first-row selection | `app.py:182-190` | `drop_duplicates` silently keeps first candidate |
| fallback status collapse | `app.py:186-193` | ID columns can contain IDs or normalized titles |
| row alignment break | `app.py:209-226` | sorted unique lists inserted into settlement rows |
| duplicate output headers | `app.py:278-287` | two source columns rename to `매핑_콘텐츠마스터ID` |
| hidden raw columns | `app.py:295-324` | hidden columns remain in output workbook |
| thin docs | `README.md:1-2` | no operational instructions |

