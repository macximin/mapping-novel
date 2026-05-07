# 적대적 감리 3회 - 전량 처리 이후

## 범위

- KISS 지급정산 최신화 버튼/CLI
- 최신화 이력 SQLite 기록
- S2 전송자료 4컬럼 hard gate
- Streamlit 운영 표면

## 1회차: 최신화/동시 실행 공격

공격 질문:

- 사용자가 최신화 버튼을 중복 클릭하면 캐시가 동시에 써지는가?
- 실패한 최신화도 운영 이력에 남는가?
- 잠금 실패가 앱/CLI에서 traceback으로 노출되는가?

검증:

- `temp/adversarial_active.lock`를 선점한 상태에서 최신화 CLI 실행
- 결과: exit code `1`
- 실패 이력 DB 기록 확인: `status=failed`, `source=kiss_api`, `error_message=KISS 최신화가 이미 실행 중입니다`

조치:

- `kiss_refresh_lock.py` 추가
- `scripts/refresh_kiss_payment_settlement.py`에 lock 적용
- lock 실패 시 traceback 대신 짧은 실패 메시지로 종료
- stale lock 회수 테스트 추가

판정:

- 최신화 동시 실행은 차단됨.
- 실패 run도 SQLite에 남음.

## 2회차: S2 전송자료 금액/매칭 공격

공격 질문:

- 금액 정책이 미확정인 플랫폼도 S2 4컬럼을 다운로드할 수 있는가?
- 일부 행만 matched일 때 부분 전송자료가 조용히 생성되는가?
- `상계금액` 빈 값을 자동 0으로 덮는가?

검증:

- synthetic mapping rows 3개로 preflight 실행
- `amount_policy_locked=False`: 후보 0행, 차단 3행
- `amount_policy_locked=True` + 일부 실패: 후보 1행, 차단 2행, exportable `False`

조치:

- `s2_transfer.py` 추가
- S2 전송자료 hard gate:
  - `amount_policy_locked=True`
  - 모든 행 `S2_매칭상태 == matched`
  - `판매채널콘텐츠ID` 존재
  - 판매금액/정산기준액/상계금액 후보가 모두 숫자
  - 차단 행이 하나라도 있으면 다운로드 버튼 비노출
- Streamlit에 `S2 전송자료 사전검증` 섹션 추가

판정:

- 금액 미확정/부분 실패/상계 누락 전송은 차단됨.
- 현재 37개 플랫폼 중 S2 금액 잠금 후보는 11개, non-S2 차단은 2개, 나머지 24개는 계속 출력 차단.

## 3회차: UI/보안/운영 표면 공격

공격 질문:

- 앱이 계속 뜨는가?
- `.env`, 캐시, SQLite, lock이 git에 올라갈 수 있는가?
- 최신화 성공 경로가 여전히 살아 있는가?

검증:

- Streamlit `http://localhost:8501` 응답 `200`
- `git check-ignore` 확인:
  - `.env`
  - `data/kiss_refresh_history.sqlite`
  - `data/kiss_payment_settlement_cache.csv`
  - `data/kiss_payment_settlement_s2_lookup.csv`
  - `temp/*`
- KISS API 1페이지 probe 성공:
  - total `7,536`
  - fetched `1`
  - s2 lookup `1`

조치:

- 앱 사이드바에 최신화 상태/이력 표시 추가
- 최신화 실행 로그 expander 추가
- README 운영 설명 업데이트

판정:

- 현재 단일 사용자 로컬 Streamlit 운용 기준으로 문제 없음.
- 팀 동시 다중 사용자/장기 백그라운드 작업 큐는 아직 범위 밖.

## 최종 판정

전량 처리 후 핵심 안전장치는 들어갔다.

- KISS 최신화: 실행 가능, 기록 가능, 동시 실행 차단
- 로컬 캐시: gitignore 보호
- S2 전송자료: hard gate로 잠금
- 테스트: `19 tests OK`

남은 고위험 영역은 코드 실행 문제가 아니라 플랫폼별 금액 정책 확정이다. 특히 24개 플랫폼은 S2 4컬럼 출력 금지 상태를 유지해야 한다.
