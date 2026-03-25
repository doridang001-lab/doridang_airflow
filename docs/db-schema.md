# DB 스키마 참조

## PostgreSQL
- 접속: `doridang:doridang@host.docker.internal:5434/doridangdb` (로컬: `127.0.0.1:5434`)
- 스키마: `public`
- 테이블: `baemin_sales` (order_id PK, order_number, order_date, order_time, order_amount)

## 함수
```python
# DB 저장 (pk_col 기준 중복 제외)
from modules.transform.utility.db import postgre_db_save
postgre_db_save(df, table, schema="public", pk_col="order_id", if_exists="append")

# DB 조회
from modules.transform.utility.db import read_table
read_table(table, schema="public", columns=None, where=None, order_by=None)

# OneDrive CSV 저장/병합/백업
from modules.transform.utility.onedrive import save_to_onedrive_csv, merge_to_onedrive, backup_to_onedrive
```

## 경로 상수 (paths.py)
| 상수 | 용도 |
|------|------|
| COLLECT_DB | 수집 데이터 (OneDrive/Docker 자동감지) |
| LOCAL_DB | C:/Local_DB (충돌 방지) |
| ONEDRIVE_DB | OneDrive Repository |
| TEMP_DIR | 임시 parquet |
| ANALYTICS_DB | 분석/집계 데이터 |
