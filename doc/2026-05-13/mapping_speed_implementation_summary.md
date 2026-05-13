# 매핑 속도 개선 구현 요약

작성일: 2026-05-13

## 적용 범위

속도 개선 survey와 execution plan의 내용을 앱 기본 경로에 반영했다.

기본 활성화된 개선:

- 파일별 stage timing 계측
- batch 후처리 timing 계측
- combined report 1회 생성 후 PD 작업지시 재사용
- CSV bytes 1회 생성 후 다운로드/ZIP 재사용
- ZIP 내부 XLSX 재압축 회피
- S2 판매채널 필터 cache
- 판매채널별 S2 mapping reference prebuild
- guard/payment/master index run 단위 prebuild
- S2 후보 자동선택 날짜 parsing precompute
- `검토필요사유` row-wise apply 제거
- guard annotation row loop 개선
- export column width 전체 scan을 sample 기반으로 변경
- adapter default feed row count copy 축소

보수적으로 열어둔 개선:

- `MAPPING_PARALLEL_WORKERS_MAX`: Cloud 기본 max 2는 유지하고, 로컬에서만 상향 실험 가능
- `SETTLEMENT_READ_ONLY_PLATFORMS`: workbook streaming은 기본 off, 플랫폼별 opt-in
- `MAPPING_EXPORT_WIDTH_SAMPLE_ROWS`: export width sampling row 수 조정 가능

## 실행 중 표시되는 새 지표

처리 결과 summary에 파일별 stage seconds가 추가된다.

- `정규화초`
- `S2필터초`
- `매핑초`
- `guard초`
- `엑셀생성초`
- `전송자료초`
- `전송엑셀초`

후처리 expander에는 다음 값이 표시된다.

- `summary_frame_seconds`
- `combined_report_seconds`
- `work_order_report_seconds`
- `csv_encode_seconds`
- `zip_build_seconds`
- `postprocess_total_seconds`

## 기대 효과

코드상 가장 큰 변화는 S2/guard 반복 계산을 run 단위로 끌어올린 것이다.

38개 파일 처리 기준 기대 범위:

- 보고서/CSV/ZIP quick win: 5-20%
- S2/guard/context 개선 포함: 누적 25-55%
- 대형 결과 파일에서 export sampling 효과 포함: 누적 35-60%

실제 효과는 처리 결과 summary의 stage seconds로 확인해야 한다.

## 의도적으로 기본 off인 항목

workbook streaming과 worker 3개 이상은 기본으로 켜지 않았다.

이유:

- 일부 플랫폼은 merged cell 보정에 의존한다.
- Streamlit Cloud에서 worker 증가는 memory pressure를 키울 수 있다.
- 기본 경로는 정합성 우선이어야 한다.

로컬 실험 예시:

```powershell
$env:MAPPING_PARALLEL_WORKERS=3
$env:MAPPING_PARALLEL_WORKERS_MAX=3
$env:MAPPING_EXPORT_WIDTH_SAMPLE_ROWS=300
$env:SETTLEMENT_READ_ONLY_PLATFORMS="리디북스,미스터블루,원스토어"
streamlit run app.py
```

## 검증 결과

전체 테스트를 통과했다.

```text
112 passed, 1 warning
```

