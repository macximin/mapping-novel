# 매핑 속도 개선 보고서

작성일: 2026-05-13

대상: Streamlit `어댑터 정규화 및 S2 매핑 실행` 구간

## 1. 결론

현재 병목은 단순히 병렬 처리 수가 부족해서 생기는 문제가 아니다.

가장 큰 비용은 다음 세 가지다.

1. 여러 정산서 파일을 처리할 때마다 S2 기준 데이터와 guard 인덱스를 반복 생성한다.
2. 각 파일 처리 중에 매핑 XLSX를 즉시 생성하고, 이후 ZIP/CSV도 즉시 전부 생성한다.
3. 원본 Excel 로딩과 시트 전체 materialization 비용이 큰 플랫폼이 있다.

따라서 권장 방향은 `worker 2 -> 3/4` 같은 병렬 수 증가가 아니라, 먼저 반복 계산 제거와 결과물 생성 비용 축소다.

## 2. 현재 관찰된 근거

### 2.1 매핑/guard 반복 계산

탐색 중 합성 Yes24 케이스에서 확인한 대략적인 비용은 다음과 같다.

| 구간 | 측정값 |
| --- | ---: |
| S2 채널 필터링 | 약 0.043초 |
| `build_mapping(filtered_s2, 1000행 정산서)` | 약 6.845초 |
| `annotate_mapping_result(..., full S2 frame)` | 약 8.665초 |

즉, S2 채널 필터 자체보다 매핑 후보군 구성, 날짜 파싱, guard/payment 인덱스 재구성이 훨씬 크다.

38개 파일을 처리한다고 가정하면, 위 두 구간만 단순 합산해도 파일당 약 15.5초 수준이다. 2개 worker 병렬을 감안해도, 이 구간만 약 4.9분 규모의 체감 시간을 만들 수 있다.

### 2.2 XLSX export

합성 30,000행 x 36열 매핑 결과에서 `export_mapping()`은 약 32.38초가 측정되었다.

현재 export는 다음 작업을 포함한다.

- 여러 시트 생성
- 헤더 스타일 적용
- 검토 대상 행 색상 처리
- 모든 셀을 훑는 열 너비 자동 조정

파일이 큰 경우 이 단계가 단일 파일 tail latency를 크게 만든다.

### 2.3 원본 Excel 로딩

51개 fixture 정규화 측정 결과는 다음과 같다.

| 항목 | 측정값 |
| --- | ---: |
| 전체 normalize | 약 44.17초 |
| 리디북스 | 약 14.84초 |
| 미스터블루 | 약 9.15초 |
| 원스토어 | 약 8.91초 |

상위 플랫폼은 workbook load 자체가 대부분을 차지했다. `read_only=False` 로딩과 전체 시트 materialization이 원인으로 보인다.

## 3. 개선 효과 추정

실제 38개 파일 동일 입력에 대한 stopwatch가 아직 없으므로 아래 값은 관측값 기반의 추정치다. 기준 시간을 100으로 두고 판단했다.

| 개선 묶음 | 기대 절감 | 누적 체감 |
| --- | ---: | ---: |
| Phase 1: 보고서/ZIP/CSV 중복 제거, XLSX ZIP 재압축 회피, progress 정리 | 5-20% | 5-20% |
| Phase 2: S2/guard batch context, 후보군/date 인덱스 재사용 | 추가 20-40% | 25-55% |
| Phase 3: workbook streaming, export width sampling/lazy artifact | 추가 15-35% | 45-70% |

보수적으로 보면 Phase 1만 적용해도 38개 파일 처리에서 수십 초 단위 개선 가능성이 있다.

Phase 2까지 적용하면 가장 현실적인 목표는 전체 실행 시간 35-60% 단축이다. 예를 들어 현재 6분 걸리는 작업이라면 2.5-4분대까지 줄어드는 수준이다.

Phase 3까지 안정화하면 특정 대형 파일이 섞인 배치에서는 50-70% 단축도 가능하다. 다만 이 영역은 플랫폼별 Excel 구조 차이와 다운로드 UX 변경이 얽혀 있어 테스트 비용이 더 크다.

## 4. 왜 병렬 수 증가가 1순위가 아닌가

현재 `parallel_mapping.py`는 기본 worker 2, 최대 worker 2로 제한되어 있다. Streamlit Cloud에서는 이 판단이 안전하다.

worker를 늘릴 때의 위험은 다음과 같다.

- openpyxl workbook/export가 메모리를 크게 사용한다.
- DataFrame과 bytes 결과물이 동시에 session state에 쌓인다.
- Cloud 환경에서 CPU보다 memory pressure가 먼저 문제가 될 수 있다.
- Python loop와 openpyxl 작업은 thread worker 증가만으로 선형 개선되지 않는다.

따라서 Cloud 기본값은 2를 유지하고, 로컬 실험용 opt-in만 나중에 여는 것이 맞다.

## 5. 우선순위 판단

### 최우선: 계측

정확한 의사결정을 위해 각 파일마다 stage seconds를 남겨야 한다.

필요한 stage는 다음과 같다.

- workbook normalize
- S2 filter
- build mapping
- guard annotation
- transfer build
- XLSX export
- batch report build
- ZIP build

이 계측은 기능 변경이 아니라 관측성 추가이므로 위험이 낮다.

### 1차 개선: 낮은 위험 quick wins

1. combined mapping report를 한 번만 만들고 PD work order가 재사용하게 한다.
2. CSV bytes를 한 번만 만들고 ZIP에서도 재사용한다.
3. ZIP 안의 XLSX는 `ZIP_STORED`로 저장해 이미 압축된 XLSX를 다시 deflate하지 않는다.
4. `summarize_normalization()`에서 default feed rows copy를 줄인다.
5. 후처리 단계 progress를 별도 표시한다.

예상 효과: 5-20%

### 2차 개선: 핵심 병목 제거

1. batch-level S2 context를 만든다.
2. clean title, normalized channel, parsed registration/payment date를 미리 계산한다.
3. guard/payment/master index를 파일마다 다시 만들지 않는다.
4. candidate index의 date parsing을 group 내부 반복이 아니라 frame 단위 1회로 줄인다.
5. row loop는 가능한 `itertuples()` 또는 vectorized mask로 바꾼다.

예상 효과: 누적 25-55%

### 3차 개선: 구조 개선

1. merged-cell expansion이 필요 없는 플랫폼부터 workbook `read_only=True` 경로를 도입한다.
2. XLSX export 열 너비 계산을 전체 셀 scan에서 header + sample 기반으로 바꾼다.
3. 매핑 완료와 artifact 생성 시점을 분리한다.
4. ZIP은 사용자가 다운로드를 누르는 시점에 만들 수 있는지 검토한다.

예상 효과: 누적 45-70%

## 6. 권장 의사결정

바로 대형 구조 변경을 시작하기보다, 다음 순서가 좋다.

1. 계측을 먼저 넣는다.
2. Phase 1 quick wins를 적용한다.
3. 38개 파일 동일 입력으로 before/after를 찍는다.
4. 결과에서 `build_mapping`/`annotate_mapping` 비중이 크면 Phase 2로 간다.
5. 결과에서 workbook load/export 비중이 크면 Phase 3 일부를 먼저 당긴다.

즉, 지금의 최선은 “병렬 수를 늘리는 것”이 아니라 “반복 계산과 즉시 생성 비용을 줄이고, 숫자로 확인하면서 다음 병목으로 이동하는 것”이다.

