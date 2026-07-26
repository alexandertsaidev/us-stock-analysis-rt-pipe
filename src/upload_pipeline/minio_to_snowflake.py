import io
import sys

import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

from pathlib import Path

from datetime import datetime
from zoneinfo import ZoneInfo

from ..notify.slack_notify import slack_pipe_notify
from ..config.minio_conn import s3_client, MINIO_BUCKET
from ..config.snowflake_conn import snow_conn

from ..utils.helpers import countdown

# ── Step 1：從 MinIO 讀取今日 Parquet ────────────────────────────────────────
def get_today_parquet_buffers(
        bucket: str,
        prefix: str = "",
    ):

    """
    從 MinIO 列出指定 bucket/prefix 下，今日修改的 .parquet 檔案，
    讀入記憶體後回傳 BytesIO buffer 列表，供 DuckDB 直接查詢使用。

    Args:
        bucket: MinIO bucket 名稱，例如 "stock-bucket"
        prefix: 資料夾路徑前綴，例如 "raw/"；預設為根目錄

    Returns:
        list[dict]，每筆包含：
            - ticker        (str)     : 股票代碼（從檔名取得，例如 "KO"）
            - object_name   (str)     : MinIO 完整物件路徑
            - last_modified (datetime): 最後修改時間（台灣時區 UTC+8）
            - size          (int)     : 檔案大小（bytes）
            - buffer        (BytesIO) : Parquet 內容，指標在位置 0，可直接傳入 DuckDB
    """

    tz_taipei = ZoneInfo("Asia/Taipei")
    today = datetime.now(tz_taipei).date()  # 台灣時間今天日期，用於過濾當日檔案

    # 列出 bucket/prefix 下所有物件，沒有檔案時 Contents 不存在，預設空 list
    response = s3_client.list_objects(Bucket=bucket, Prefix=prefix)
    objects = response.get('Contents', [])

    results = []
    for obj in objects:

        # 過濾非今日修改的檔案（LastModified 預設 UTC，需轉台灣時區再比對）
        if obj['LastModified'].astimezone(tz_taipei).date() != today:
            continue
        if "final_all" in obj["Key"]:          # ← 排除 final_all 目錄
            continue
        if "conclusion" in obj["Key"]:          # ← 排除 conclusion 目錄
            continue
        # 只處理 .parquet 檔，跳過其他格式
        if not obj['Key'].endswith(".parquet"):
            continue

        try:
            # 從 MinIO 下載檔案內容到記憶體（不落地）
            res = s3_client.get_object(Bucket=bucket, Key=obj['Key'])
            buffer = io.BytesIO(res['Body'].read())
            buffer.seek(0)  # 重置指標到起始位置，確保後續讀取從頭開始

            results.append({
                "ticker":        Path(obj['Key']).stem,              # 去除路徑與副檔名，取純檔名作為股票代碼
                "object_name":   obj['Key'],                         # MinIO 完整路徑
                "last_modified": obj['LastModified'].astimezone(tz_taipei),  # 轉台灣時區
                "size":          obj['Size'],                        # 檔案大小（bytes）
                "buffer":        buffer,                             # 記憶體中的 Parquet 內容
            })
            print(f"✓ {obj['Key']}")

        except Exception as e:
            # 單一檔案失敗不中斷整體流程，印出錯誤繼續處理下一個
            print(f"✗ {obj['Key']}: {e}")

    print(f"\n共 {len(results)} 個檔案")
    return results

# ── 工具：自動產生 MERGE SQL ──────────────────────────────────────────────────
def build_merge_sql(df: pd.DataFrame, key_cols: list[str]) -> str:
    """
    依據 df 的欄位自動產生 MERGE SQL。
    key_cols : 唯一鍵欄位（用於 ON 條件）
    其餘欄位 : WHEN MATCHED UPDATE / WHEN NOT MATCHED INSERT
    """
    all_cols  = list(df.columns)
    val_cols  = [c for c in all_cols if c not in key_cols]

    on_clause     = "\n        AND ".join(
        [f'target."{c}" = source."{c}"' for c in key_cols]
    )
    update_clause = ",\n            ".join(
        [f'"{c}" = source."{c}"' for c in val_cols]
    )
    insert_cols   = ", ".join([f'"{c}"' for c in all_cols])
    insert_vals   = ", ".join([f'source."{c}"' for c in all_cols])

    return f"""
        MERGE INTO STOCK.US.PRICES AS target
        USING STOCK.US.PRICES_STAGING AS source
        ON  {on_clause}
        WHEN MATCHED THEN UPDATE SET
            {update_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols})
        VALUES ({insert_vals})
    """

# ── Step 2：Buffer → Snowflake（staging + MERGE）────────────────────────────
def buffers_to_snowflake(results: list[dict], conn):
    KEY_COLS = ["Date", "ticker", "period"]

    success_items = []  # 成功清單
    failed_items  = []  # 失敗清單

    for item in results:
        stem   = Path(item["object_name"]).stem
        period = stem.split("_")[1]

        try:
            item["buffer"].seek(0)
            df = pd.read_parquet(item["buffer"])

            tickers     = df["ticker"].unique().tolist()
            tickers_str = ", ".join(f"'{t}'" for t in tickers)

            # ── Step 1：先寫 STAGING（DDL 隱式 commit，獨立在外）──────────
            write_pandas(
                conn              = conn,
                df                = df,
                table_name        = "PRICES_STAGING",
                database          = "STOCK",
                schema            = "US",
                auto_create_table = True,
                overwrite         = True,
            )
            print(f"📥 {item['object_name']} → staging {len(df)} 筆")

            # ── Step 2：DELETE + MERGE 同一個 transaction ─────────────────
            conn.autocommit(False)
            try:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        DELETE FROM STOCK.US.PRICES
                        WHERE "period" = '{period}'
                        AND "ticker" IN ({tickers_str})
                        AND "Date" IN (
                            SELECT "Date" FROM (
                                SELECT "Date",
                                    ROW_NUMBER() OVER (
                                        PARTITION BY "ticker", "period"
                                        ORDER BY "Date" DESC
                                    ) AS rn
                                FROM STOCK.US.PRICES
                                WHERE "period" = '{period}'
                                AND "ticker" IN ({tickers_str})
                            )
                            WHERE rn <= 2
                        )
                    """)
                    print(f"🗑 {period} 刪除 {cur.rowcount} 筆舊資料")

                    merge_sql = build_merge_sql(df, KEY_COLS)
                    cur.execute(merge_sql)
                    merge_rows = cur.rowcount
                    print(f"✓ {period} MERGE 完成｜rows={merge_rows}")

                conn.commit()
                success_items.append({           # ← 記錄成功
                    "period": period,
                    "file":   item["object_name"],
                    "rows":   merge_rows,
                })

            except Exception as e:
                conn.rollback()
                print(f"✗ 失敗，已 rollback：{e}")
                raise

            finally:
                conn.autocommit(True)

        except Exception as e:
            print(f"✗ {item['object_name']}: {e}")
            failed_items.append({                # ← 記錄失敗
                "period": period,
                "file":   item["object_name"],
                "error":  str(e),
            })

    # ── 產生 Summary String ───────────────────────────────────────────────
    now = datetime.now(ZoneInfo("Asia/Taipei")).strftime("%Y-%m-%d %H:%M")

    lines = [f"📊 4.Snowflake 上傳報告｜{now}"]
    lines.append(f"總計：{len(results)} 個檔案｜✅ {len(success_items)} 成功｜❌ {len(failed_items)} 失敗")
    lines.append("")

    if success_items:
        lines.append("✅ 成功項目")
        for s in success_items:
            lines.append(f"  • [{s['period']}] {s['file']}｜{s['rows']} rows")

    if failed_items:
        lines.append("")
        lines.append("❌ 失敗項目")
        for f in failed_items:
            lines.append(f"  • [{f['period']}] {f['file']}")
            lines.append(f"    原因：{f['error']}")

    summary = "\n".join(lines)
    print("\n" + summary)
    return summary                               # ← 回傳給 main() 備用

def main():
    try:
        results = get_today_parquet_buffers(
            bucket = MINIO_BUCKET,
            prefix = "stock/history/prices/gold/"
        )

        summary = buffers_to_snowflake(results, snow_conn)
        slack_pipe_notify(summary) 

    finally:
        snow_conn.close()
        print("\nSnowflake 連線已關閉")

if __name__ == "__main__":
    main()
    countdown(10)

# 強制關閉程序
sys.exit()