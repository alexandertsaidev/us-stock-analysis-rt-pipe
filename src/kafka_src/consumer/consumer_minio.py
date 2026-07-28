import os
import sys
import threading
import requests

import json
from collections import defaultdict

import logging

import time
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

from kafka import KafkaConsumer
from dotenv import load_dotenv, find_dotenv

from ...config.minio_conn import get_s3_client, MINIO_BUCKET

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
        slack_rt_pipe_notify("🔴 美股今日休市 , realtime minio 程式已關閉 !")
        os._exit(0)  # 整個 process 直接結束

    date_str = now_dt.strftime("%Y-%m-%d")
    holiday_hours = is_holiday(date_str)

    if holiday_hours is None:
        # 正常交易日
        return dtime(9, 30), dtime(16, 0)
    
    elif holiday_hours == "":
        # 節日休市
        logger.info("今日休市，結束程式")
        slack_rt_pipe_notify("🔴 美股今日休市 , realtime minio 程式已關閉 !")
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
            slack_rt_pipe_notify("🔴 美股 realtime minio 程式已關閉 !")
            os._exit(0)  # 整個 process 直接結束

        time.sleep(120)

def make_consumer(
    group_id,
    fetch_max_wait_ms=2000,
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

def run_minio_consumer():

    s3_client = get_s3_client()
    consumer = make_consumer(
        "minio-consumer",
        fetch_max_wait_ms=10000,   # batch 寫入，不需要快
        fetch_min_bytes=2048       # 湊滿 2KB 再回傳，減少小批次):
    )

    buf = []
    last_flush = time.time()
    tz = "America/New_York"
    
    for msg in consumer:
        try:
            trade = msg.value
            if EXCLUDE_CODES.intersection(trade.get("c", [])):   # 盤外略過
                continue

            buf.append(msg) # 存完整 msg
            
            if len(buf) >= 1000 or time.time() - last_flush > 120:
                try:
                    groups = defaultdict(list)
                    for record in buf:
                        # groups[record.value["s"]].append(record)

                        trade_date = datetime.fromtimestamp(
                            record.value["t"] / 1000, tz=ZoneInfo(tz)
                        ).strftime("%Y-%m-%d")

                        # key 改成 (symbol, date) 二元組，避免跨日混批
                        groups[(record.value["s"], trade_date)].append(record)

                    for (sym, trade_date), msgs in groups.items():   # ← 直接解包
                        records = [m.value for m in msgs]

                        t_start = datetime.fromtimestamp(records[0]["t"] / 1000, tz=ZoneInfo(tz))
                        first_msg, last_msg = msgs[0], msgs[-1]

                        key = (
                            f"stock/real-time/prices/bronze/{sym}/{trade_date}/"
                            f"p{first_msg.partition}_{first_msg.offset}-{last_msg.offset}.jsonl"
                        )

                        jsonl_bytes = "\n".join(json.dumps(r) for r in records).encode("utf-8")
                        s3_client.put_object(
                            Bucket=MINIO_BUCKET,
                            Key=key,
                            Body=jsonl_bytes,
                            ContentLength=len(jsonl_bytes),
                            ContentType="application/octet-stream",
                        )
                        logger.info(f"MinIO flush {len(records):>4} 筆  {sym:<6} → {key}")

                    # 所有 symbol 都成功才執行以下兩行
                    buf = []
                    last_flush = time.time()
                    consumer.commit()

                except Exception as e:
                    # 任一 symbol 失敗 → buf 保留 → offset 不移動 → 下次重試
                    logger.error(f"flush 失敗，保留 buf 等待重試: {e}")

        except Exception as e:
            logger.error(f"minio-consumer 錯誤: {e}")

def main():

    tz = "America/New_York"
    slack_rt_pipe_notify("🟢 美股 realtime minio 程式已開啟 !")

    open_time, close_time = get_open_close_time(tz)

    if datetime.now(ZoneInfo(tz)).time() >= close_time:
        slack_rt_pipe_notify("🔴 執行時已超過美股收盤時間，minio 程式已關閉 !")
        os._exit(0)

    # 1.thread: market_close_watcher 監控收盤時間
    watcher = threading.Thread(
        target=market_close_watcher,
        args=(tz, close_time),
        daemon=True
    )
    watcher.start()
    # 2.thread: run_minio_consumer 監控 Kafka 消費
    consumer_thread = threading.Thread(target=run_minio_consumer)
    consumer_thread.start()
    consumer_thread.join()  # 主程式在這裡等

if __name__ == "__main__":
    main()
