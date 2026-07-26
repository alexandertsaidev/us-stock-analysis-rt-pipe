import os
from dotenv import load_dotenv, find_dotenv

import duckdb

load_dotenv(find_dotenv())

MINIO_ENDPOINT    = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY  = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY  = os.environ["MINIO_SECRET_KEY"]

# DuckDB — 查詢操作（SELECT、WHERE、JOIN）
def get_duckdb_conn() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    conn.execute(f"""
        SET s3_endpoint          = '{MINIO_ENDPOINT.replace("http://", "").replace("https://", "")}';
        SET s3_access_key_id     = '{MINIO_ACCESS_KEY}';
        SET s3_secret_access_key = '{MINIO_SECRET_KEY}';
        SET s3_use_ssl   = false;
        SET s3_url_style = 'path';
    """)
    return conn