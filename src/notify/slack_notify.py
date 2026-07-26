import os
from dotenv import load_dotenv, find_dotenv

from slack_sdk.webhook import WebhookClient

load_dotenv(find_dotenv())

def slack_pipe_notify(message: str):

    url = os.getenv("SLACK_PIPE_WEBHOOK_URL")  # 從環境變數讀取 Webhook URL
    webhook = WebhookClient(url)  # 建立 Webhook 客戶端實例

    # 發送純文字訊息到 Slack 頻道
    response = webhook.send(text = message)

    # 驗證是否發送成功
    assert response.status_code == 200  # HTTP 狀態碼應為 200
    assert response.body == "ok"        # Slack 回傳內容應為 "ok"

def slack_price_notify(message: str):

    url = os.getenv("SLACK_PRICE_WEBHOOK_URL")  # 從環境變數讀取 Webhook URL
    webhook = WebhookClient(url)  # 建立 Webhook 客戶端實例

    # 發送純文字訊息到 Slack 頻道
    response = webhook.send(text = message)

    # 驗證是否發送成功
    assert response.status_code == 200  # HTTP 狀態碼應為 200
    assert response.body == "ok"        # Slack 回傳內容應為 "ok"


