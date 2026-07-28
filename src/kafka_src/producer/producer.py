import websocket

import requests

import os
import sys
import threading
import random

import json

import logging

import time
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo

from kafka import KafkaProducer

import duckdb
from dotenv import load_dotenv, find_dotenv

from ...config.minio_conn import MINIO_BUCKET
from ...config.minio_duckdb_conn import get_duckdb_conn

from ...notify.slack_notify import slack_rt_pipe_notify

load_dotenv(find_dotenv())

logger = logging.getLogger(__name__)

producer = KafkaProducer(
    bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    # Broker 為此 producer 分配唯一 PID（Producer ID）
    # 每筆訊息附帶遞增 sequence number
    # Broker 偵測到相同 PID + sequence → 丟棄重複，不重複寫入
    # 同時強制覆蓋：acks="all"、retries=INT_MAX
    enable_idempotence=True,
    
    # 需所有 ISR（In-Sync Replicas）都寫入後才回傳 ack
    # enable_idempotence=True 已強制此設定，這裡顯式寫出提高可讀性
    # acks=0：不等回應（最快，可能丟失）
    # acks=1：只等 leader（leader 掛掉會丟）
    # acks="all"：等所有 replica（最安全）
    acks="all",
    
    # broker 無回應或可重試錯誤時的最大重送次數
    # enable_idempotence=True 已強制 retries=INT_MAX（無限）
    # 顯式設 5 避免無限卡住；依業務容忍度調整
    retries=5,
    
    # 同時允許幾筆「已送出但尚未收到 ack」的請求
    # 未開 idempotent 時設 >1 可能因重試導致亂序
    # 開啟 idempotent 後 broker 用 PID+sequence 自動偵測並拒絕亂序
    # Kafka 協議保證 idempotent 模式下最大安全值為 5
    max_in_flight_requests_per_connection=5,
)


def is_holiday(date_str: str) -> str | None:
    """
    查詢指定日期是否為假日。
    回傳：
      None          → 非節日，正常交易
      ""            → 休市
      "09:30-13:00" → 節日，但縮短交易
    """
    url = f"https://finnhub.io/api/v1/stock/market-holiday?exchange=US&token={os.environ['FINNHUB_API_KEY']}"
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
        slack_rt_pipe_notify("🔴 美股今日休市 💤  , producer 程式已關閉 !")
        os._exit(0)  # 整個 process 直接結束

    date_str = now_dt.strftime("%Y-%m-%d")
    holiday_hours = is_holiday(date_str)

    if holiday_hours is None:
        # 正常交易日
        return dtime(9, 30), dtime(16, 0)
    
    elif holiday_hours == "":
        # 節日休市
        logger.info("今日休市，結束程式")
        slack_rt_pipe_notify("🔴 美股今日休市 💤 , producer 程式已關閉 !")
        os._exit(0)  # 整個 process 直接結束

    else:
        # 節日縮短交易
        return parse_open_close_time(holiday_hours)


def market_open_watcher(tz, open_time, close_time):

    slack_rt_pipe_notify("🟢 美股 realtime producer 程式已開啟 !")

    if datetime.now(ZoneInfo(tz)).time() >= close_time:
        slack_rt_pipe_notify("🔴 執行時已超過美股收盤時間，producer 程式已關閉 !")
        os._exit(0)
    
    while datetime.now(ZoneInfo(tz)).time() < open_time:

        now_time = datetime.now(ZoneInfo(tz)).time()

        remaining = (
            datetime.combine(datetime.now(ZoneInfo(tz)).date(), open_time) -
            datetime.combine(datetime.now(ZoneInfo(tz)).date(), now_time)
        ) # timedelta

        h = remaining.seconds // 3600
        m = (remaining.seconds % 3600) // 60
        s = remaining.seconds % 60

        if remaining > timedelta(hours=1):
            logger.info(f"距離美股開盤剩餘: {h} 時 {m} 分 {s} 秒")
            slack_rt_pipe_notify(f"⏳ 距離美股開盤剩餘: {h} 時 {m} 分 {s} 秒 !")
            time.sleep(1800)  # 30 分鐘檢查一次

        elif timedelta(minutes=6) < remaining <= timedelta(minutes=60):
            logger.info(f"距離美股開盤剩餘: {h} 時 {m} 分 {s} 秒")
            slack_rt_pipe_notify(f"⏳ 距離美股開盤剩餘: {h} 時 {m} 分 {s} 秒 !")
            time.sleep(300)  # 5 分鐘檢查一次

        else:
            logger.info(f"距離美股開盤剩餘: {h} 時 {m} 分 {s} 秒")
            slack_rt_pipe_notify(f"⏳ 距離美股開盤剩餘: {h} 時 {m} 分 {s} 秒 !")
            time.sleep(5)  # 5 s 檢查一次

    logger.info("美股已經開盤 !")
    slack_rt_pipe_notify(f"🔔 美股已經開盤, 收盤時間為 {tz} {close_time.strftime('%H:%M')} !")

    return

def market_close_watcher(ws_ref, tz, close_time):
    """
    背景執行緒：每 x 秒檢查是否已收盤，收盤後關閉 ws 並結束程式。
    """
    while True:
        now_time = datetime.now(ZoneInfo(tz)).time()

        remaining = (
            datetime.combine(datetime.now(ZoneInfo(tz)).date(), close_time) -
            datetime.combine(datetime.now(ZoneInfo(tz)).date(), now_time)
        ) # timedelta

        h = remaining.seconds // 3600
        m = (remaining.seconds % 3600) // 60
        s = remaining.seconds % 60

        if now_time >= close_time:
            logger.info(f"已收盤（{close_time}），停止程式")
            try:
                slack_rt_pipe_notify("🔴 美股現在已經收盤 🔕 (程式延後關閉) !")
                producer.flush()
                producer.close()
                ws_ref.close()

            except Exception as e:
                logger.error(f"關閉時發生錯誤: {e}")

            finally:
                os._exit(0)  # 整個 process 直接結束

        elif remaining <= timedelta(hours=1):
            logger.info(f"距離美股收盤剩餘: {h} 時 {m} 分 {s} 秒") 
            slack_rt_pipe_notify(f"⏳ 距離美股收盤剩餘: {h} 時 {m} 分 {s} 秒 !")
            time.sleep(600)  # 10 分鐘檢查一次

        else:
            logger.info(f"距離美股收盤剩餘: {h} 時 {m} 分 {s} 秒") 
            slack_rt_pipe_notify(f"⏳ 距離美股收盤剩餘: {h} 時 {m} 分 {s} 秒 !")
            time.sleep(1800) # 30 分鐘檢查一次


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

def on_message(ws, message):
    try:
        data = json.loads(message)
        if data.get("type") == "trade":
            for trade in data.get("data", []):
                future = producer.send(
                    "trades",
                    value=trade,
                    key=trade.get("s"),
                )
                future.add_errback(lambda e: logger.error(f"⚠️ Kafka 發送失敗: {e}"))

    except (json.JSONDecodeError, KeyError) as e:
        logger.error(f"⚠️ 訊息解析失敗: {e}")

def on_error(ws, error):
    print(error)

def on_close(ws, close_status_code, close_msg):
    print("### closed ###")

def on_open(ws, sleep_range=(0, 0.05)):
    
    with get_duckdb_conn() as conn:

        tickers = get_co_fetch_list(
            conn = conn,
            bucket= MINIO_BUCKET,
            object_name= f"stock/screening/us_all_co_screen.parquet"
        )

    for symbol in tickers:
        try:
            ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
            time.sleep(random.uniform(*sleep_range))

        except Exception as e:
            logger.warning(f"on_open: 訂閱中斷 ({symbol}): {e}")
            break

def main():
    tz = "America/New_York"
    open_time, close_time = get_open_close_time(tz)

    market_open_watcher(tz, open_time, close_time)

    websocket.enableTrace(True)
    ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={os.environ['FINNHUB_API_KEY']}",
                            on_message = on_message,
                            on_error = on_error,
                            on_close = on_close,
                            on_open = on_open)

    watcher = threading.Thread(
        target=market_close_watcher,  # 要在背景執行的函式
        args=(ws, tz, close_time),    # 傳入 ws 讓 watcher 能關閉它
        daemon=True                   # 主程式結束時自動殺掉此執行緒
    )
    watcher.start()  # 背景執行的函式啟動，不等待它結束，繼續往下跑

    ws.run_forever(reconnect=15, ping_interval=60, ping_timeout=20)

if __name__ == "__main__":
    main()
