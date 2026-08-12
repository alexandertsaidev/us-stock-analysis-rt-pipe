import os
import threading
import shelve

import json
import logging

import time
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

from kafka import KafkaConsumer
from dotenv import load_dotenv, find_dotenv

import duckdb
from botocore.exceptions import ClientError

from ...config.minio_conn import MINIO_BUCKET
from ...config.minio_duckdb_conn import get_duckdb_conn

from ...notify.slack_notify import slack_rt_pipe_notify

load_dotenv(find_dotenv())

logger = logging.getLogger(__name__)

EXCLUDE_CODES = {"24", "25"} # 盤外成交

def is_holiday(date_str: str) -> str | None:
    """
    查詢指定日期是否為假日。
    回傳：
      None          → 非節日，正常交易
      ""            → 休市
      "09:30-13:00" → 節日，但縮短交易
    """
    url = f"https://finnhub.io/api/v1/stock/market-holiday?exchange=US&token={os.environ['FINNHUB_API_KEY_1']}"
    try:
        response = requests.get(url, timeout=10)
        data = response.json()
        for event in data.get("data", []):
            if event.get("atDate") == date_str:

                return event.get("tradingHour", "")
        
        return None  # 非節日清單
    
    except Exception as e:
        logger.info(f"⚠️ 假日 API 失敗，預設當正常交易日: {e}")
        return None  # API 炸了就當正常日處理，不中斷

def parse_open_close_time(trading_hour: str):
    """解析 '09:30-13:00' → (time(9,30), time(13,0))"""
    start_str, end_str = trading_hour.split("-")
    sh, sm = map(int, start_str.split(":"))
    eh, em = map(int, end_str.split(":"))

    return dtime(sh, sm), dtime(eh, em)

def get_open_close_time(tz):
    """
    回傳今日交易時段 (open_time, close_time)。
    None 表示今日休市。
    """
    now_dt = datetime.now(ZoneInfo(tz))

    # 週末直接休市
    if now_dt.weekday() >= 5:
        slack_rt_pipe_notify("🛑 美股今日休市 , realtime alert 程式已關閉 !")
        os._exit(0)  # 整個 process 直接結束

    date_str = now_dt.strftime("%Y-%m-%d")
    holiday_hours = is_holiday(date_str)

    if holiday_hours is None:
        # 正常交易日
        return dtime(9, 30), dtime(16, 0)
    
    elif holiday_hours == "":
        # 節日休市
        logger.info("今日休市，結束程式")
        slack_rt_pipe_notify("🛑 美股今日休市 , realtime alert 程式已關閉 !")
        os._exit(0)  # 整個 process 直接結束

    else:
        # 節日縮短交易
        return parse_open_close_time(holiday_hours)

def market_close_watcher(tz, close_time):
    """
    背景執行緒：每 x 秒檢查是否已收盤，收盤後關閉 ws 並結束程式。
    """
    while True:
        now_time = datetime.now(ZoneInfo(tz)).time()

        if now_time >= close_time:
            logger.info(f"已收盤（{close_time}），停止程式")
            slack_rt_pipe_notify("🛑 美股 realtime alert 程式已關閉 !")
            os._exit(0)  # 整個 process 直接結束

        time.sleep(120)

def get_co_fetch_list(
    conn: duckdb.DuckDBPyConnection,
    bucket: str,
    object_name: str
    ) -> list[str]:

    try:
        tickers = conn.execute(f"""
            SELECT DISTINCT "ticker"
            FROM read_parquet('s3://{bucket}/{object_name}')
            WHERE "created_at" >= CURRENT_DATE - INTERVAL '2 years'
        """).df()["ticker"].tolist()
        
        logger.info(f"從 {bucket}/{object_name} 取得 {len(tickers)} 檔")
        return tickers

    except duckdb.HTTPException as e:
        logger.error(f"DuckDB 讀取 S3 失敗 ({bucket}/{object_name}): {e}", exc_info=True)
        raise FileNotFoundError(f"找不到檔案: s3://{bucket}/{object_name}") from e

    except ClientError as e:
        # 保留 boto3 ClientError，以防其他地方仍用 s3_client
        logger.error(f"MinIO 讀取失敗 ({bucket}/{object_name}): {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"讀取 {bucket}/{object_name} 發生未知錯誤: {e}", exc_info=True)
        raise

def load_alert_config() -> dict:

    with get_duckdb_conn() as conn:
        tickers = get_co_fetch_list(
            conn = conn,
            bucket= MINIO_BUCKET,
            object_name= f"stock/screening/us_all_co_screen.parquet"
        )

        result = conn.execute("""
            SELECT "ticker", "upperband", "lowerband", "upper_1_7", "lower_1_7"
            FROM read_parquet('s3://us-stock/stock/history/prices/gold/final_all/us_all_prices.parquet')
            WHERE "period" = 'D'
            AND "ticker" = ANY($1)
            QUALIFY ROW_NUMBER() OVER (PARTITION BY "ticker" ORDER BY "Date" DESC) = 1
        """, [tickers]).fetchall()

    config = {}
    for ticker, upper, lower, upper_17, lower_17 in result:
        config[ticker] = [
            {"threshold": upper, "cooldown": 60, "level": "critical", "emoji":"🚨", "label": "upperband"},
            {"threshold": lower, "cooldown": 60, "level": "critical", "emoji":"🚨", "label": "lowerband"},
            {"threshold": upper_17, "cooldown": 30, "level": "warning", "emoji":"⚠️", "label": "upper_1_7"},
            {"threshold": lower_17, "cooldown": 30, "level": "warning", "emoji":"⚠️", "label": "lower_1_7"},
        ]

    # 找出沒有對應 Parquet 資料的 ticker，方便 debug
    missing = set(tickers) - set(config.keys())
    if missing:
        logger.info(f"[load_alert_config] ,無資料的 ticker：{missing}")
        slack_rt_pipe_notify(f"[load_alert_config] ⚠️⚠️  無資料的 ticker：{missing}")

    return config

def make_consumer(
    group_id,
    fetch_max_wait_ms=500,
    fetch_min_bytes=1
    ):
    """
    建立並回傳一個 KafkaConsumer 實例
    每個 Consumer 傳入不同 group_id，確保各自維護獨立的 offset
    
    Args:
        group_id: Consumer Group 名稱
                  ex: "minio-consumer" / "alert-consumer" / "monitor-consumer"
    """
    
    consumer = KafkaConsumer(
        "trades", # 訂閱 Producer 送進來的的資料 topic
        bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"], # Kafka Broker 位置

        # Producer 送進來的bytes → decode("utf-8") → JSON 字串 → dict
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),

        # Producer 送進來的bytes → decode("utf-8") → 字串
        key_deserializer=lambda k: k.decode("utf-8"),

        # 同一個 group_id 的 Consumer 共享 同一組 offset,不同各自獨立消費
        group_id=group_id,
        enable_auto_commit=False,  # 關掉 auto commit

        fetch_max_wait_ms=fetch_max_wait_ms,
        fetch_min_bytes=fetch_min_bytes,
    )

    return consumer

def run_alert_consumer():
    alert_config = load_alert_config()

    consumer = make_consumer(
        "alert-consumer",
        fetch_max_wait_ms=50,
        fetch_min_bytes=1,
    )

    prev = {}  # symbol → 上一筆成交價

    with shelve.open("alert_state") as last_sent:
        for msg in consumer:
            trade = msg.value
            if EXCLUDE_CODES.intersection(trade.get("c", [])):
                consumer.commit()
                continue

            symbol = trade["s"]
            price  = float(trade["p"])
            p      = prev.get(symbol)

            if p is not None:
                for cfg in alert_config.get(symbol, []):
                    threshold = cfg["threshold"]
                    cooldown  = cfg["cooldown"]
                    level     = cfg["level"]
                    label     = cfg["label"]
                    emoji     = cfg["emoji"]

                    crossed   = False

                    if "upper" in label and p <= threshold and price > threshold:
                        direction = " ▲ 升破"
                        crossed   = True
                    elif "lower" in label and p >= threshold and price < threshold:
                        direction = " ▼ 跌破"
                        crossed   = True

                    if crossed:
                        now = time.time()
                        # 冪等 key：同一筆 trade 重跑時 p、price 相同 → key 相同 → cooldown 擋住
                        idempotent_key = (
                            f"{symbol}:{label}:{threshold:.3f}:{p:.3f}:{price:.3f}"
                        )

                        if now - last_sent.get(idempotent_key, 0) > cooldown:

                            text = (
                                f"{emoji} [ {level.upper():<8}] "
                                f"{symbol:<6} {direction} "
                                f"{label:<12}({threshold:>8.3f})："
                                f"{p:>8.3f} → {price:>8.3f}"
                            )
                            print(text)
                            slack_rt_pipe_notify(text)

                            last_sent[idempotent_key] = now
                            last_sent.sync()  # 強制 flush，確保 crash 後狀態不丟

            prev[symbol] = price
            consumer.commit()

def main():
    tz = "America/New_York"
    slack_rt_pipe_notify("🟢 美股 realtime alert 程式已開啟 !")

    open_time, close_time = get_open_close_time(tz)

    if datetime.now(ZoneInfo(tz)).time() >= close_time:
        slack_rt_pipe_notify("🛑 執行時已超過美股收盤時間，alert 程式已關閉 !")
        os._exit(0)

    # 1.thread: market_close_watcher 監控收盤時間
    watcher = threading.Thread(
        target=market_close_watcher,
        args=(tz, close_time),
        daemon=True
    )
    watcher.start()

    # 2.thread:
    consumer_thread = threading.Thread(target=run_alert_consumer)
    consumer_thread.start()
    consumer_thread.join()

if __name__ == "__main__":
    main()
