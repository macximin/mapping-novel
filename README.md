# mapping-novel

IPS/S2 소설 매핑 Streamlit 앱입니다.

## 실행

```powershell
pip install -r requirements.txt
streamlit run app.py
```

## 현재 동작

- S2 기준을 중심으로 매핑합니다.
- IPS 기준 `data/kidari_contents.xlsx`는 있으면 보조 검산으로만 사용합니다.
- S2 기준은 다음 중 하나를 사용합니다.
  - 앱에서 전체 교체한 S2 캐시
  - 수동 S2 리스트 업로드
  - 수동 S2 원천 엑셀 업로드
- 플랫폼별 정산서는 어댑터를 거쳐 표준 재료로 변환한 뒤 행별 매핑 결과를 생성합니다.
- 매핑 상태를 `matched`, `no_match`, `ambiguous`, `blank_key`로 분리합니다.
- 중복 후보는 자동 선택하지 않고 `중복후보` 시트로 분리합니다.
- 결과 엑셀은 `요약`, `입력검증`, `행별매핑결과`, `검토필요`, `중복후보` 시트를 포함합니다.
- S2 최신화는 앱 사이드바 버튼 또는 CLI로 실행할 수 있으며, 실행 이력은 로컬 DB에 남깁니다.
- S2 전송자료 4컬럼은 금액 정책 잠금, S2 matched, 금액 후보 검증을 모두 통과한 경우에만 다운로드됩니다.

## 테스트

```powershell
python -m unittest discover -s tests -v
python -m py_compile app.py mapping_core.py
```

## S2 최신화

앱 사이드바의 `S2 기준 전체 교체` 버튼으로 S2 기준 캐시를 갱신합니다.
기본 조회는 시작일/종료일을 비운 무기간 조회이며, 기존 S2 캐시는 새 조회 결과로 통째로 교체합니다.

플랫폼 자동 선택은 엑셀 파일명에 포함된 플랫폼명/별칭을 기준으로만 동작합니다.

생성되는 S2 캐시, 최신화 이력 DB, 실행 잠금 파일은 로컬 운영 파일이며 git에 올리지 않습니다.
