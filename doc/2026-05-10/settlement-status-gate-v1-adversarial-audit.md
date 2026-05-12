# 정산상태 게이트 v1 적대적 감리

작성일: 2026-05-10

대상 브랜치: `feature/settlement-status-gate-v1`

대상 구현:

- `settlement_status_gate.py`
- `scripts/build_settlement_status_gate.py`
- `tests/test_settlement_status_gate.py`

범위:

- 앱 연결 전 1차 구현만 감리한다.
- 기존 rename 결과는 건드리지 않는다.
- S2 전송자료 이중 잠금과 Streamlit UI 연결은 다음 단계로 둔다.

## 감리 1회차: unknown ID 과통과

공격 질문:

> `payment_lookup_safe`라는 이름 때문에 수동 S2 파일에서도 정산상태표에 없는 ID가 통과되는 것 아닌가?

발견:

- 최초 구현은 `source_kind == payment_lookup` 또는 `mode == payment_lookup_safe`면 unknown ID를 `OK_PAYMENT_LOOKUP_SOURCE`로 인정했다.
- 이 조건은 너무 넓다.
- 수동 S2 파일에서 사용자가 `payment_lookup_safe` 모드를 선택하면 확인 불가 ID가 통과될 수 있다.

조치:

- unknown ID를 `OK_PAYMENT_LOOKUP_SOURCE`로 인정하는 조건을 `source_kind == "payment_lookup"`으로 좁혔다.
- `mode == "payment_lookup_safe"`만으로는 unknown ID를 통과시키지 않는다.
- 테스트 추가: `test_payment_lookup_safe_mode_does_not_allow_unknown_manual_s2_ids`

## 감리 2회차: 콘텐츠ID 공란 mixed 오판

공격 질문:

> 콘텐츠ID가 공란인 행들이 같은 빈 문자열 그룹으로 묶이면, 정상 행 하나 때문에 다른 공란 누락 행이 `HOLD_MIXED_CONTENT_RISK`로 오판되는 것 아닌가?

발견:

- 최초 구현은 콘텐츠ID별 지급정산관리 존재 여부를 groupby로 계산했다.
- 콘텐츠ID 공란도 하나의 그룹으로 취급될 수 있었다.

조치:

- mixed 위험 판정에 `콘텐츠ID`가 비어 있지 않아야 한다는 조건을 추가했다.
- 콘텐츠ID 공란 + 지급정산관리 없음은 `HOLD_NO_PAYMENT_SETTLEMENT`로 남긴다.
- 테스트 추가: `test_blank_content_id_does_not_create_false_mixed_risk`

## 감리 3회차: 0건 부서 누락

공격 질문:

> 확정 담당부서에 `소설1팀`, `소설사업부`가 포함되어 있는데, 문제 건수가 0이면 요약에서 사라지는 것 아닌가?

발견:

- 최초 `status_by_department`는 실제 행이 있는 부서만 출력했다.
- `소설1팀`처럼 0건인 부서가 요약에서 빠질 수 있었다.

조치:

- `status_by_department()`가 확정 담당부서 목록을 받아 0건 부서도 출력하게 수정했다.
- 최신 재생성 결과에서 `소설1팀`은 모든 상태 0건으로 명시된다.

## 재현 결과

명령:

```powershell
python scripts\build_settlement_status_gate.py --output data\settlement_status_gate_latest.csv --summary data\settlement_status_gate_latest_summary.json
```

전체 최신 S2 대조:

- 판매채널콘텐츠 행: 98,113개
- 지급정산관리 없음: 4,710개
- A/B 체크리스트: 1,373개
- 혼합 위험: 351개

확정 소설 담당부서:

- 판매채널콘텐츠 행: 83,233개
- 지급정산관리 없음: 3,647개
- A/B 체크리스트: 381개
- 혼합 위험: 261개
- 혼합 위험 콘텐츠ID: 102개

## 테스트

```powershell
python -m unittest discover -s tests -p test_settlement_status_gate.py
python -m unittest discover -s tests
```

결과:

- 게이트 테스트: 9개 통과
- 전체 테스트: 64개 통과, 17개 skip

## 남은 의도적 미구현

- 앱 UI 연결
- `mapping_core.build_mapping()` 앞단 연결
- `s2_transfer.py` 정산상태 이중 잠금
- `정산상태게이트` 탭
- 운영 다운로드 UI

위 항목은 다음 작은 브랜치에서 진행한다.
