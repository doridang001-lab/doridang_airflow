# 아키텍처

## ETL 흐름
```mermaid
graph LR
    subgraph Extract
        CR[크롤링/Selenium] --> DF[DataFrame]
        GS[GSheet] --> DF
        DB[(PostgreSQL)] --> DF
    end
    subgraph Transform
        DF --> PL[pipelines/sales/ & strategy/]
        PL --> PQ[Parquet via XCom]
    end
    subgraph Load
        PQ --> CSV[OneDrive CSV]
        PQ --> GS2[GSheet]
        PQ --> DB2[(PostgreSQL)]
    end
```

## 모듈 구조
```mermaid
graph TD
    DAG[dags/sales/ & strategy/] -->|import| PIP[pipelines/sales/ & strategy/]
    PIP -->|사용| UT[utility/]
    UT --- io[io.py]
    UT --- paths[paths.py]
    UT --- schedule[schedule.py]
    UT --- mailer[mailer.py]
    UT --- gsheet[gsheet.py]
    UT --- db[db.py]
    UT --- onedrive[onedrive.py]
    DAG -->|import| EX[extract/]
    DAG -->|import| LD[load/]
```

## utility 선택 기준
| 필요한 것 | 파일 |
|---------|------|
| 경로 상수 | paths.py |
| 파일 로드/저장 | io.py |
| 스케줄 상수 | schedule.py |
| 이메일 발송 | mailer.py |
| GSheet 읽기/쓰기 | gsheet.py |
| DB 조회/저장 | db.py |
| OneDrive CSV | onedrive.py |
