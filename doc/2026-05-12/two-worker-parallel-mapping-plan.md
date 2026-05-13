# 2-worker 병렬 매핑 처리 구현 계획 및 적대적 감리

작성일: 2026-05-12

## 결론

정산서 40~50개 단위 일괄 처리 속도 개선은 `파일 수를 줄여서 업로드`하는 방식으로 해결하지 않는다.

품질을 유지하면서 적용할 수 있는 1차 개선안은 **파일 단위 2-worker 병렬 처리**다. 각 정산서 파일은 서로 독립적으로 `어댑터 정규화 -> S2 판매채널 필터 -> 매핑 -> guard annotation -> 결과 엑셀 생성`을 수행하므로, 동일 S2 기준과 guard를 읽기 전용으로 공유한 뒤 파일별 작업만 병렬화할 수 있다.

단, Streamlit Community Cloud는 자원이 제한된 실행 환경이다. 공식 문서상 Community Cloud는 자원 한계에 도달하면 앱이 느려지거나 비정상 상태가 될 수 있으며, 2024-02 기준 한계는 대략 CPU 최대 2 cores, 메모리 최대 2.7GB 수준으로 공지되어 있다.

따라서 기본 구현은 다음 원칙을 따른다.

- 기본 병렬도는 `2`를 넘기지 않는다.
- `ProcessPoolExecutor`는 Cloud 기본 경로로 사용하지 않는다.
- worker thread는 Streamlit UI를 직접 갱신하지 않는다.
- 업로드 파일 객체를 worker 간 공유하지 않는다.
- 결과 순서는 업로드 순서로 고정한다.
- 품질을 낮추는 CSV-only, 서식 제거, 엑셀 시트 축소는 이번 개선 범위에서 제외한다.

참고: Streamlit Community Cloud resource limits
`https://docs.streamlit.io/deploy/streamlit-community-cloud/manage-your-app`

## 현재 구조 Survey

현재 `app.py`의 실행 버튼 처리 흐름은 순차 처리다.

1. `load_selected_s2_basis()`로 S2 기준과 guard를 1회 로딩한다.
2. `settlement_files`를 `for` 루프로 순회한다.
3. 파일마다 `process_settlement_batch_item()`을 호출한다.
4. 모든 결과가 모이면 `build_mapping_session_state()`에서 summary, 종합 CSV, ZIP bytes를 만든다.

핵심 함수:

- `app.py::process_settlement_batch_item`
  - 파일 1개를 처리하는 단위 함수다.
  - `normalize_settlement()`, `filter_s2_by_sales_channel()`, `build_mapping()`, `annotate_mapping_result()`, `build_s2_transfer()`, `export_mapping()`을 순서대로 호출한다.
- `settlement_adapters.py::normalize_settlement`
  - `openpyxl` 또는 OOXML fallback으로 workbook을 읽고 플랫폼별 표준 행으로 변환한다.
- `mapping_core.py::build_mapping`
  - S2 기준 후보 index를 만들고 정산서 정제키에 매핑한다.
- `mapping_core.py::export_mapping`
  - 결과 workbook을 만들고 시트별 freeze pane, filter, header style, 검토필요 row fill, column width를 적용한다.
- `app.py::build_batch_zip`
  - 결과 엑셀 bytes와 종합 CSV를 ZIP으로 압축한다.

현재 UI는 단일 caption만 갱신한다.

```text
29/47 처리 중: 파일명.xlsx
S2 기준과 정산서 엑셀을 처리하는 중...
```

2-worker 병렬 처리에서는 이 UI를 worker slot 2개가 보이도록 바꾼다.

## 목표 UI

권장 UI는 큰 카드 2개가 아니라, 좁은 상태 패널 1개 안에 worker slot 2개를 표시하는 방식이다.

```text
29/47 완료 · 2개 처리 중

[1] 2026-02__네이버_연재__...xlsx
    엑셀 정규화 중

[2] 2026-02__알라딘(소설)__...xlsx
    S2 매핑 및 결과 엑셀 생성 중
```

완료/실패 카운트도 함께 표시한다.

```text
성공 27 · 차단 1 · 실패 1
```

진행 단계는 다음 정도로 충분하다.

- 대기
- 엑셀 정규화 중
- S2 필터/매핑 중
- guard/전송자료 검증 중
- 결과 엑셀 생성 중
- 완료
- 실패

주의:

- worker thread에서 `st.*`를 호출하지 않는다.
- worker는 progress event만 큐에 넣는다.
- 메인 스레드가 큐를 읽어 `progress_slot.markdown()`을 갱신한다.
- 너무 잦은 UI 갱신을 피하기 위해 단계 변경 또는 파일 완료 시점 중심으로 갱신한다.

## 구현 계획

### 1. 병렬도 설정

환경변수로 rollback 가능한 병렬도 설정을 둔다.

```text
MAPPING_PARALLEL_WORKERS=1  # 기존 순차 처리
MAPPING_PARALLEL_WORKERS=2  # Cloud 권장 병렬 처리
```

앱 내부 helper는 다음 원칙으로 worker 수를 결정한다.

- 파일이 0~1개면 `1`
- 환경변수 미설정이면 `2`
- 환경변수가 잘못된 값이면 `1`
- Cloud 기본 상한은 `2`
- 로컬 실험을 위해 상한을 늘리더라도 별도 opt-in 없이 3 이상을 쓰지 않는다.

### 2. 업로드 파일 payload 분리

Streamlit `UploadedFile` 객체 자체를 worker에 공유하지 않는다.

권장 방식:

1. task submit 직전에 해당 파일의 bytes를 읽는다.
2. worker 안에서 `NamedBytesIO(payload, name, size)` 같은 독립 file-like 객체를 만든다.
3. 기존 `process_settlement_batch_item()`에는 독립 file-like 객체를 넘긴다.

이렇게 하면 worker마다 seek pointer가 독립적이고, 동시에 같은 업로드 객체를 읽는 위험을 피할 수 있다.

중요한 메모리 정책:

- 47개 파일 전체를 한 번에 bytes snapshot하지 않는다.
- 실행 중인 2개 task에 대해서만 payload 복사본을 보유한다.
- 다음 task는 worker slot이 비었을 때 bytes를 읽어 submit한다.

### 3. worker 함수

기존 `process_settlement_batch_item()`을 크게 바꾸지 않는다. 대신 선택적 `progress_callback` 인자를 추가한다.

권장 stage 삽입 지점:

1. 파일 시작
2. `normalize_settlement()` 전
3. `build_mapping()` 전
4. `annotate_mapping_result()`/`build_s2_transfer()` 전
5. `export_mapping()` 전
6. 완료 또는 실패

`progress_callback`은 순수 함수로 취급한다.

```python
progress_callback(index, source_name, stage)
```

Streamlit UI 호출은 callback 내부에서도 하지 않고, callback은 queue에 event만 넣는다.

### 4. 2-slot scheduler

`ThreadPoolExecutor(max_workers=worker_count)`를 사용한다.

권장 scheduler 방식은 모든 파일을 한 번에 submit하는 방식이 아니라, **동시에 최대 2개만 submit**하는 방식이다.

이유:

- 활성 task payload만 bytes 복사본으로 들고 있을 수 있다.
- UI slot을 `1`, `2`로 안정적으로 배정하기 쉽다.
- 특정 task가 끝나는 즉시 같은 slot에 다음 파일을 넣을 수 있다.

의사 흐름:

```text
results = [None] * len(files)
active = {}
next_index = 0

submit(slot 1)
submit(slot 2)

while active:
    progress queue drain
    done future 확인
    results[index] = result
    slot 비움
    next 파일이 있으면 같은 slot에 submit
    UI 갱신
```

완료 후 `results`의 `None`이 남아 있으면 실패 result로 채운다.

### 5. 결과 순서 보장

병렬 실행 완료 순서는 파일 업로드 순서와 다를 수 있다.

따라서 result dict에는 다음 필드를 추가한다.

- `input_index`
- `worker_slot`
- `started_at`
- `finished_at`
- `elapsed_seconds`

최종 `summary_frame`, `combined_report_frame`, ZIP 파일 순서는 `input_index` 기준으로 정렬한다.

### 6. ZIP 생성 위치

1차 구현에서는 기존처럼 모든 파일 처리 완료 후 `build_mapping_session_state()`에서 ZIP을 생성한다.

품질 유지가 우선이므로 `export_mapping()` 결과 엑셀은 그대로 포함한다.

단, 메모리 위험을 줄이기 위해 다음은 2차 개선 후보로 남긴다.

- ZIP 생성 시 결과 DataFrame에서 엑셀 bytes를 지연 생성
- ZIP 작성 후 result dict에서 큰 bytes를 제거
- session_state에 full result 객체 대신 다운로드 bytes와 summary만 남기는 구조 축소

이번 1차 병렬화에서는 산출물 구조를 바꾸지 않는다.

## 예상 개선

2-worker thread 병렬의 기대치는 보수적으로 본다.

- 47개 파일 기준 전체 대기시간: 약 1.2~1.7배 개선 예상
- 파일별 엑셀 파싱이 큰 경우: 개선폭 증가 가능
- 결과 엑셀 export와 ZIP 생성 비중이 큰 경우: 개선폭 제한

절반 시간까지 기대하지 않는다. Python thread는 CPU-bound 구간에서 GIL 영향을 받는다. 다만 엑셀 읽기, 압축, pandas/openpyxl 내부 처리 일부가 섞여 있어 2-worker 정도는 체감 개선이 가능하다.

## 위험사항

### 메모리 피크

2개 worker가 동시에 다음 객체를 만든다.

- workbook
- 파싱 DataFrame
- S2 필터 결과
- MappingResult DataFrame
- export 중인 openpyxl workbook
- 결과 xlsx bytes

마지막에는 전체 결과 ZIP bytes도 추가된다.

대응:

- worker 수 기본 2 제한
- 활성 task만 payload bytes 복사
- 환경변수로 즉시 1-worker rollback
- 처리 완료 뒤 불필요한 중간 payload 참조 제거

### UI thread 안정성

Streamlit UI는 메인 스레드에서만 갱신한다.

대응:

- worker는 queue event만 발행
- 메인 스레드가 event를 읽고 placeholder 갱신
- callback 예외는 worker 작업을 죽이지 않도록 방어

### 결과 순서 뒤섞임

병렬 처리는 완료 순서가 달라진다.

대응:

- `input_index` 기준 result list에 저장
- summary/ZIP 생성 전 순서 고정
- 파일명 충돌 처리는 기존 `unique_archive_name()` 유지

### 특정 파일 장기 실행

큰 파일 하나가 오래 걸리면 worker slot 하나가 묶인다.

대응:

- 나머지 slot은 계속 다음 파일 처리
- UI에 오래 걸리는 파일명이 남아 사용자가 병목 파일을 확인 가능
- 파일별 elapsed time을 summary에 진단 컬럼으로 남김

### Cloud 동시 사용자

한 사용자가 47개 파일 병렬 처리 중일 때 다른 사용자가 앱을 열면 같은 앱 자원을 나눠 쓴다.

대응:

- worker 수 2 제한
- Cloud 문제가 있으면 `MAPPING_PARALLEL_WORKERS=1`
- 장기적으로는 관리자 로컬 배치/비동기 job queue 분리 검토

## 하지 않는 것

이번 개선에서 하지 않는다.

- 결과 엑셀 시트 제거
- 엑셀 서식 제거
- CSV-only 모드 전환
- S2 매핑 기준 축소
- 검토필요/중복후보 시트 생략
- `ProcessPoolExecutor` Cloud 기본 적용
- worker 4개 이상 병렬 처리
- Streamlit worker thread 직접 UI 조작

## 테스트 계획

### 단위 테스트

가능하면 순수 helper를 작게 분리해 테스트한다.

- worker 수 clamp
  - 파일 1개면 1
  - env 미설정이면 2
  - env `1`이면 1
  - env `2`이면 2
  - env `99`여도 Cloud 기본 상한 2
  - env 비정상 문자열이면 1

- 결과 순서 보장
  - 일부 task가 역순으로 완료되어도 결과 list는 input order 유지

- 실패 격리
  - 한 파일 worker가 예외를 내도 전체 batch는 계속 진행
  - 실패 result에 파일명, 상태, 오류 메시지 보존

### 회귀 테스트

기존 테스트를 유지한다.

```powershell
python -m unittest discover -s tests -v
python -m py_compile app.py mapping_core.py settlement_adapters.py
```

추가로 병렬 smoke test를 만든다.

- 작은 in-memory workbook 3개
- worker count 2
- 결과 3개 모두 생성
- summary 순서가 입력 순서와 동일

### 수동 검증

Cloud 전:

- 로컬 `MAPPING_PARALLEL_WORKERS=1`로 기존 결과와 비교
- 로컬 `MAPPING_PARALLEL_WORKERS=2`로 같은 파일 set 처리
- ZIP 내부 파일 목록 비교
- 대표 파일 2~3개 엑셀 시트/행 수 비교

Cloud 후:

- 5개 파일 smoke
- 15개 파일 smoke
- 47개 파일 실사용 검증
- Cloud logs에서 memory/resource error 확인

## Rollback 계획

긴급 rollback은 코드 revert 없이 환경변수로 한다.

```text
MAPPING_PARALLEL_WORKERS=1
```

이 값이면 기존 순차 처리와 동일한 실행 경로를 사용한다.

문제가 크면 병렬 runner 호출부만 순차 루프로 되돌린다. `process_settlement_batch_item()`의 `progress_callback` 인자는 optional이므로 기존 경로와 호환되어야 한다.

## 적대적 감리 1회차: 결과 순서와 파일 포인터 오염

공격:

병렬 worker가 같은 `UploadedFile` 객체를 공유하면 `seek(0)`과 `read()`가 서로 간섭할 수 있다. 파일 A worker가 읽는 도중 파일 B worker 또는 메인 스레드가 같은 객체의 pointer를 움직이면 엑셀 파싱이 실패하거나 잘못된 파일 내용이 들어갈 수 있다. 또한 먼저 끝난 파일 순서대로 results를 append하면 ZIP 순서와 summary 순서가 업로드 순서와 달라진다.

위험:

- 간헐적 `BadZipFile`, `InvalidFileException`, header not found
- 파일명과 실제 payload 불일치
- summary/ZIP 순서 뒤섞임
- 사용자가 특정 파일 오류를 잘못 해석

보정:

- worker에는 원본 `UploadedFile`이 아니라 독립 `NamedBytesIO`를 넘긴다.
- payload bytes는 task submit 시점에 해당 파일에서 한 번만 읽는다.
- 동시에 submit하는 task 수만큼만 payload 복사본을 보유한다.
- result는 append하지 않고 `results[input_index] = result`로 저장한다.
- result dict에 `input_index`, `source_name`, `worker_slot`을 기록한다.

판정:

2-worker 병렬 처리 가능. 단, 업로드 객체 공유와 append 기반 결과 수집은 금지한다.

## 적대적 감리 2회차: Cloud 메모리 피크와 앱 재시작

공격:

2개 worker는 계산만 2배가 아니라 중간 객체도 2배 만든다. 특히 `export_mapping()`은 결과 DataFrame을 openpyxl workbook으로 다시 만들고, 각 worksheet cell을 훑어 style/width를 적용한다. 여기에 결과 bytes와 최종 ZIP bytes까지 session_state에 올라가면 Streamlit Cloud 메모리 한계에 닿을 수 있다.

위험:

- 앱이 갑자기 느려짐
- Streamlit resource limit 오류
- `Oh no` 오류 화면
- 대량 처리 완료 직전 ZIP 생성 단계에서 실패

보정:

- Cloud 기본 worker 수는 2로 고정한다.
- 3 이상은 환경변수와 로컬 실험 전용으로만 허용한다.
- 모든 파일 payload를 선복사하지 않는다.
- active slot 2개만 payload를 보유한다.
- 완료된 task의 payload 참조는 즉시 제거한다.
- 1차 구현에서는 품질 유지를 위해 엑셀 export를 유지하되, 파일별 elapsed time과 stage를 남겨 다음 병목을 계측한다.
- Cloud에서 문제가 생기면 `MAPPING_PARALLEL_WORKERS=1`로 즉시 복귀한다.

판정:

2-worker는 제한적으로 허용한다. 4-worker thread 또는 process pool은 Cloud 기본 경로로 부적합하다.

## 적대적 감리 3회차: 품질 보존과 숨은 race

공격:

속도 개선을 위해 worker 안에서 일부 결과 시트를 생략하거나, export를 CSV로 대체하거나, S2 기준/guard를 얕은 복사 없이 수정하면 품질이 떨어지거나 파일 간 결과가 오염될 수 있다. `build_mapping()`은 내부에서 `drop_disabled_rows()`와 copy를 수행하지만, 이후 단계에서 공유 DataFrame을 직접 수정하는 코드가 추가되면 병렬 race가 생길 수 있다.

위험:

- 기존 산출물과 시트 구성 불일치
- `검토필요`, `중복후보`, `입력검증` 누락
- S2 기준 DataFrame 공유 오염
- 누락/청구 guard annotation 누락
- S2 전송자료 차단 조건 회귀

보정:

- 산출물 시트 구성은 그대로 유지한다.
- `export_mapping()`과 `export_s2_transfer()`를 그대로 사용한다.
- worker는 전달받은 `s2_df`, `s2_guards`, `s2_guard_filter`를 읽기 전용으로 취급한다.
- 필요한 경우 worker 진입 전에 `s2_df`를 수정하지 않는다는 규칙을 테스트/리뷰 체크리스트에 넣는다.
- 결과 품질 검증은 worker 1과 worker 2 결과를 같은 입력 set으로 비교한다.
- `s2_transfer`의 차단 조건은 기존 테스트 `tests/test_s2_transfer.py`로 회귀 확인한다.

판정:

품질 저하 없는 병렬화는 가능하다. 단, 품질을 지키려면 병렬화 범위를 파일 단위 scheduling으로 제한하고, 매핑/export 내부 알고리즘은 변경하지 않는다.

## 최종 구현 순서

1. `MAPPING_PARALLEL_WORKERS` 설정 helper 추가
2. `NamedBytesIO` 또는 동등한 독립 upload payload wrapper 추가
3. `process_settlement_batch_item()`에 optional `progress_callback` 추가
4. 2-slot ThreadPool scheduler 추가
5. progress UI를 전체 진행률 + worker slot 2개 형태로 교체
6. 결과 순서 `input_index` 기준 고정
7. 파일별 elapsed time 진단값 summary에 추가
8. worker 1 경로로 기존 결과와 비교
9. worker 2 경로로 smoke 및 47개 실사용 검증
10. Cloud에서 문제 발생 시 env로 worker 1 rollback

## 최종 판정

진행해도 된다.

단, 이번 개선의 목표는 무리한 최대 속도 달성이 아니라 **품질 유지 상태에서 Cloud가 감당 가능한 수준의 체감 개선**이다. 따라서 `ThreadPoolExecutor(max_workers=2)`를 1차 상한으로 삼고, process pool과 3개 이상 worker는 별도 실험이 끝나기 전까지 운영 기본값으로 쓰지 않는다.
