# 매핑 속도 개선안 적대적 감리 3회

작성일: 2026-05-13

대상 문서:

- `mapping_speed_parallel_deepdive_survey.md`
- `mapping_speed_improvement_report.md`

목적: 속도 개선 주장과 실행 순서가 실제 서비스 품질을 해치지 않는지 반대 관점에서 검토한다.

## 1차 감리: 정합성 훼손 위험

### 공격 질문

반복 계산을 제거하면 결과가 빨라지는 것은 맞지만, 매핑 결과가 조금이라도 달라질 가능성은 없는가?

### 위험 지점

1. S2 context cache가 stale해질 수 있다.
   - 사용자가 수동 S2 기준 파일을 바꾸거나, Cloud 배포 S2 파일이 갱신된 뒤에도 이전 context를 재사용하면 오매핑 가능성이 있다.

2. 후보군 정렬이 달라질 수 있다.
   - 현재 `_sort_candidates_for_auto_selection()`은 group 내부에서 날짜를 파싱하고 정렬한다.
   - 이를 frame 단위 precompute로 바꾸면 invalid date, blank date, 동률 날짜의 fallback order가 달라질 수 있다.

3. guard annotation 의미가 바뀔 수 있다.
   - `annotate_mapping_result()`는 missing/billing/service/payment/master index를 통해 후처리 컬럼을 만든다.
   - index 생성 위치를 바꾸면서 key normalization 방식이 미세하게 달라지면 결과 컬럼이 바뀔 수 있다.

4. workbook streaming은 merged cell 처리와 충돌할 수 있다.
   - 현재 `_sheet_rows()`는 merged cell 값을 보정한다.
   - `read_only=True`를 무작정 적용하면 일부 플랫폼의 헤더/행 해석이 깨질 수 있다.

5. lazy artifact는 성공 시점의 의미를 바꿀 수 있다.
   - 지금은 매핑 실행 중 XLSX 생성까지 끝난다.
   - lazy로 바꾸면 화면상 성공 후 다운로드 시점에 export 실패가 발생할 수 있다.

### 방어 조건

Phase 2 이상은 golden output 비교 없이는 적용하지 않는다.

필수 비교 항목:

- 매핑 결과 row count
- `매핑상태`
- `S2_ID`
- 자동 선택된 후보
- 검토 사유
- guard 관련 annotation 컬럼
- ZIP entry 이름
- CSV row count와 sort order

### 1차 판정

Phase 1은 정합성 위험이 낮다.

Phase 2는 효과가 크지만, 후보군 정렬과 guard index 결과를 보존하는 테스트가 먼저 필요하다.

Phase 3의 workbook streaming은 플랫폼별 fixture 통과 전까지 기본값으로 켜면 안 된다.

## 2차 감리: 성능 근거 부족 위험

### 공격 질문

개선 효과 35-60% 같은 숫자는 실제 38개 파일 실행 시간이 아니라 합성 측정과 코드 구조 분석에 기반한다. 너무 낙관적인 것 아닌가?

### 위험 지점

1. 합성 측정과 실제 파일 분포가 다를 수 있다.
   - 실제 38개 파일이 대부분 작다면 S2/guard context 효과는 줄어든다.
   - 반대로 대형 파일 몇 개가 tail을 만들면 export 개선 효과가 더 커질 수 있다.

2. ThreadPool 환경에서 stage별 시간이 단순 합산되지 않는다.
   - worker 2 병렬에서는 느린 파일 하나가 전체 시간을 좌우한다.
   - 평균 절감률보다 P95/P100 파일 시간이 더 중요할 수 있다.

3. ZIP/CSV quick win은 파일 크기에 따라 효과가 낮을 수 있다.
   - XLSX 재압축 회피는 큰 XLSX가 많을수록 효과가 있다.
   - 작은 파일 위주라면 체감은 몇 초 수준일 수 있다.

4. workbook load 병목은 플랫폼별로 다르다.
   - 리디북스/미스터블루/원스토어는 load 비용이 컸지만, 모든 채널이 그런 것은 아니다.

5. export benchmark는 synthetic 30,000행 기준이다.
   - 실제 정산서 행 수가 작으면 export 개선은 작다.
   - 실제 결과 시트가 더 크면 export 개선은 더 클 수 있다.

### 방어 조건

속도 개선 patch 전에 계측을 먼저 넣는다.

필수 측정값:

- 파일별 전체 seconds
- 파일별 normalize seconds
- 파일별 mapping seconds
- 파일별 annotation seconds
- 파일별 export seconds
- batch report seconds
- ZIP seconds
- worker count
- 입력 파일 수
- 성공/실패/차단 건수

### 2차 판정

효과 추정은 방향성 판단에는 충분하지만, 약속 가능한 SLA로 쓰기에는 부족하다.

따라서 보고서의 숫자는 “예상 범위”로만 사용하고, 첫 patch는 반드시 계측 + low-risk 개선으로 제한해야 한다.

## 3차 감리: 운영/Cloud 안정성 위험

### 공격 질문

빠르게 하려다가 Streamlit Cloud에서 메모리 초과, rerun 꼬임, 다운로드 실패가 더 자주 생기지 않는가?

### 위험 지점

1. worker 증가는 Cloud memory pressure를 키운다.
   - workbook, DataFrame, XLSX bytes, ZIP bytes가 동시에 메모리에 올라온다.

2. session state가 이미 무겁다.
   - 각 result에 DataFrame과 bytes가 함께 보관된다.
   - ZIP bytes까지 즉시 저장하면 peak memory가 커진다.

3. lazy ZIP은 Streamlit rerun과 충돌할 수 있다.
   - 다운로드 버튼 클릭 시점의 state가 보존되어야 한다.
   - 생성 시간이 길면 UI가 멈춘 것처럼 보일 수 있다.

4. progress 업데이트가 너무 잦으면 UI overhead가 생긴다.
   - 하지만 너무 줄이면 사용자는 멈춘 것으로 느낄 수 있다.

5. ProcessPool은 Cloud 기본값으로 부적합하다.
   - 큰 DataFrame과 bytes를 pickle로 넘기면 오히려 느리거나 불안정할 수 있다.

### 방어 조건

Cloud 기본 동작은 보수적으로 유지한다.

- 기본 worker 2 유지
- ProcessPool 미사용
- local-only worker 증가는 별도 env opt-in
- batch context도 run 단위로만 유지
- lazy artifact는 별도 feature flag 뒤에 둔다
- memory peak가 줄어드는 변경부터 적용한다

### 3차 판정

운영 관점에서도 Phase 1이 가장 안전하다.

Phase 2는 정합성 테스트를 통과하면 Cloud에도 유리하다. 반복 index를 줄이면 CPU뿐 아니라 memory churn도 줄 가능성이 있다.

Phase 3의 lazy artifact는 가장 큰 UX 변경이므로, 실제 stage timing에서 export/ZIP이 지배적인 경우에만 진행하는 것이 좋다.

## 최종 감리 결론

다음 순서가 가장 방어 가능하다.

1. 계측 추가
2. combined/CSV/ZIP quick win
3. 동일 입력 38개 파일 기준 before/after 기록
4. S2/guard context 도입
5. export width sampling
6. workbook streaming 또는 lazy artifact는 마지막에 선택 적용

현재 개선 효과 추정은 합리적이지만, Phase 2 이상의 숫자는 실제 38개 파일 계측 후 보정해야 한다.

