import subprocess


def main() -> int:
    conf = '{"sale_date":"2026-06-09"}'
    cmd = [
        "/home/airflow/.local/bin/airflow",
        "dags",
        "trigger",
        "DB_UnifiedSales",
        "-c",
        conf,
    ]
    subprocess.run(cmd, check=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
