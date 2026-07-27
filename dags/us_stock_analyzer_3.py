from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from zoneinfo import ZoneInfo

COMPOSE_DIR = "/Docker/stacks/us-stock-analysis-rt-pipe"

START = datetime(2024, 1, 1, tzinfo=ZoneInfo('America/New_York'))

with DAG(
    dag_id="us_stock_rt_open",
    schedule="25 9 * * 1-5",   # 09:25 開盤前 5 分鐘
    start_date=START,
    catchup=False,
    tags=["stock", "us", "realtime"],
) as _:
    BashOperator(
        task_id="us_stock_rt_open",
        bash_command=f"cd {COMPOSE_DIR} && docker compose up -d &",
        retries=2
    )

with DAG(
    dag_id="us_stock_rt_close",
    schedule="5 16 * * 1-5",   # 16:05 收盤後 5 分鐘
    start_date=START,
    catchup=False,
    tags=["stock", "us", "realtime"],
) as _:
    BashOperator(
        task_id="us_stock_rt_close",
        bash_command=f"cd {COMPOSE_DIR} && docker compose stop &",
        retries=2
    )