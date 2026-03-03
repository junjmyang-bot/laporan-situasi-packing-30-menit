# Packing Laporan Situasi (30 Menit)

Streamlit 기반 현장 운영 리포트 앱입니다.
목표는 모바일 입력 속도, Telegram 우선 전송, Google Sheets 백업 안정성입니다.

## 파일 구조
- `app.py`: 메인 앱 (lock/takeover, idempotency, telegram fallback, sheets backup 포함)
- `.gitignore`: 캐시/상태파일 제외 규칙
- `.laporan_situasi_state.json`: 운영 상태 저장 파일 (Git 제외)

## 환경 변수 / Secrets
필수:
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

권장(백업):
- `GOOGLE_SHEETS_WEBHOOK_URL`

팀 PIN 설정 (하드코딩 금지):
- 방법 A: `st.secrets["TEAM_PASSWORDS"]`에 맵으로 설정
- 방법 B: `TEAM_PASSWORDS_JSON` (JSON 문자열)
- 방법 C: `TEAM_PIN_PACKING_1`, `TEAM_PIN_PACKING_2`, `TEAM_PIN_PACKING_3`

예시 (`TEAM_PASSWORDS_JSON`):
```json
{"PACKING-1":"xxxx","PACKING-2":"yyyy","PACKING-3":"zzzz"}
```

## 실행
```bash
streamlit run app.py
```

## 운영 체크리스트
1. `Buka Tim / Take Over Tim` 잠금 동작 확인
2. 같은 데이터 재전송 시 idempotency retry-safe 확인
3. Telegram edit 실패(`message not found`, `can't edit`) 시 새 메시지 fallback 확인
4. 긴 메시지 part rollover(슬롯 단위 분할) 확인
5. Sheets webhook 미설정 안내와 실제 HTTP 실패 에러 분리 확인
6. `Asia/Jakarta` 시간대 표시/저장 일관성 확인
