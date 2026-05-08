# 정산정보없음 IPS rename 실행 요약

생성시각: 2026-05-08T16:07:50

## 결과

- 원 후보 콘텐츠ID: 1,195건
- 수동검토 보류: 127건
  - 사유: 같은 콘텐츠ID에 지급정산관리 정상 채널도 존재함
- 웹툰/공란 보류: 464건
  - 사유: 소설 매핑 범위 밖이므로 live rename 미실행
- 소설 live rename 시도: 604건
- 성공 및 재조회 검증 완료: 498건
- 실패: 106건

## 실패 원인

실패 106건은 API 인증/권한 문제가 아니라 KIPM 서버 저장 오류입니다.
서버 응답은 `AFCH_DATA` 변경이력 컬럼 길이 초과입니다.
위험한 기본값 채우기 우회는 실행하지 않았습니다.

## 산출물

- 성공 목록: `C:\Users\wjjo\Desktop\mapping-novel\판매채널-지급정산 불일치\04_IPS_보조자료\IPS_정산정보없음_rename_20260508\latest__정산정보없음_novel_ips_live_rename_success.csv`
- 실패 목록: `C:\Users\wjjo\Desktop\mapping-novel\판매채널-지급정산 불일치\04_IPS_보조자료\IPS_정산정보없음_rename_20260508\latest__정산정보없음_novel_ips_live_rename_failed.csv`
- 재검증 잔여 목록: `C:\Users\wjjo\Desktop\mapping-novel\판매채널-지급정산 불일치\04_IPS_보조자료\IPS_정산정보없음_rename_20260508\latest__정산정보없음_novel_ips_live_rename_remaining.csv`
- 웹툰/공란 보류 목록: `C:\Users\wjjo\Desktop\mapping-novel\판매채널-지급정산 불일치\04_IPS_보조자료\IPS_정산정보없음_rename_20260508\20260508_160238__정산정보없음_write_보류_웹툰_공란.csv`
- 수동검토 보류 목록: `C:\Users\wjjo\Desktop\mapping-novel\판매채널-지급정산 불일치\04_IPS_보조자료\IPS_정산정보없음_rename_20260508\20260508_155823__정산정보없음_rename_위험_수동검토.csv`
