# US Stock Analysis — Realtime Pipeline

![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=flat&logo=apachekafka&logoColor=white)
![MinIO](https://img.shields.io/badge/MinIO-C72E49?style=flat&logo=minio&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)
![Slack Alerts](https://img.shields.io/badge/🔔%20Slack%20Alerts-4A154B?style=flat&logoColor=white)
![Finnhub](https://img.shields.io/badge/Finnhub%20API-1DB954?style=flat&logo=adminer&logoColor=white)

A **production-grade real-time data pipeline** that ingests live US stock market data via the **Finnhub Stock API**, streams it through **Apache Kafka**, stores it in **MinIO** object storage, and sends pipeline alerts to **Slack**.

✅ **Achievement**

Built a **fault-tolerant real-time stock data pipeline** that continuously ingests, streams, and stores live US market data across **100+ tickers** with **zero message loss**.

📈 **Metric**

Processes **~3 tick records per second per ticker** from Finnhub (e.g. AAPL generates **~188 records / 60 sec**), batching into MinIO every **2 minutes** (**~300–400 records per batch per ticker**) with **at-least-once delivery guarantees** across the full pipeline.

⚡ **Action**

Engineered with **Python Kafka producers** (`acks=all`, manual offset commit), **Docker Compose** multi-service orchestration, and **MinIO S3-compatible object storage** — with 🔔 **Slack webhook alerts** for real-time pipeline observability.

---

## 📑 Table of Contents

- [Architecture](#-architecture)
- [Layer Responsibilities](#-layer-responsibilities)
- [Getting Started](#-getting-started)
  - [Prerequisites](#prerequisites)
  - [Environment Variables](#environment-variables)
  - [Installation](#installation)
- [Usage](#-usage)
- [Slack Notifications](#-slack-notifications)
- [Tech Stack](#-tech-stack)
- [License](#-license)
- [Connect](#-connect)

---

## 🏗️ Architecture

```
          ┌──────────────────────┐
          │   Finnhub Stock API  │  Real-time market data source
          └──────────┬───────────┘
                     │
                     ▼
          ┌──────────────────────┐
          │    Kafka Broker      │  Message queue & topic management
          │                      │
          │   Producer ──►       │  at-least-once delivery (acks=all)
          │   Consumer ──►       │  offset-based consumption
          └──────────┬───────────┘
                     │
                     ▼
          ┌──────────────────────┐
          │        MinIO         │  Object storage (data lake / staging)
          └──────────┬───────────┘
                     │
                     ▼
          ┌──────────────────────┐
          │    Slack Webhook     │  Pipeline alerts & error notifications
          └──────────────────────┘
```

---

## 📦 Layer Responsibilities

### 1. Finnhub Stock API — Data Source

- Provides **real-time US stock quotes** (price, volume, timestamp) via WebSocket
- Each producer poll fetches the **latest tick** for a configured list of tickers
- Requires a **free API key** from [finnhub.io](https://finnhub.io/)

### 2. Kafka — Message Broker

**Producer**
- Fetches real-time tick data from Finnhub and publishes to a **dedicated Kafka topic per ticker**
- Configured with **`acks=all`** and **`retries`** to guarantee **at-least-once delivery** — no message is dropped even under transient failures
- Each message is serialized as **JSONL** with ticker symbol, price, volume, and timestamp

**Consumer**
- Subscribes to Kafka topics using a **named consumer group** for offset tracking
- Commits offsets **manually (sync)** after successful write to MinIO, ensuring **at-least-once processing** semantics
- On restart, **resumes from the last committed offset** — no data loss on failure

**Broker**
- Decouples producers from consumers, allowing **independent scaling** of each side
- Manages **topic partitioning** for parallel processing across multiple tickers
- Provides **durable, replayable message logs** for fault tolerance and replay capability

### 3. MinIO — Object Storage (Data Lake)

- Acts as a **staging / landing zone** for consumed streaming data
- Stores data in **Jsonl format**, partitioned by **ticker** and **date**
- **S3-compatible** object storage — portable to **AWS S3** with minimal config changes

### 4. Slack — Alerts & Notifications

- Receives **pipeline status updates** via incoming webhook
- Notifies on **successful batch writes**, **consumer lag warnings**, and **pipeline errors**
- Keeps operators informed **without needing to monitor logs manually**

---

## 🚀 Getting Started

### Prerequisites

- [Docker](https://www.docker.com/) & **Docker Compose** installed
- [Finnhub API key](https://finnhub.io/) (**free tier** available)
- **Slack incoming webhook URL** (see [Slack Notifications](#-slack-notifications))
- **`.env` file** configured (see below)

### Environment Variables

Create a `.env` file in the project root:

```env
# ── MinIO 連線設定 ──────────────────────────────────────────
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
# ── MinIO 業務設定 ──────────────────────────────────────────
MINIO_SECURE=false
# ── Kafka 設定 ──────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
# ── bucket 市場 ─────────────────────────────────────────────
MINIO_BUCKET=stock-rt-data
# ── Slack 通知設定 ──────────────────────────────────────────
SLACK_BATCH_PIPE_WEBHOOK_URL=https://hooks.slack.com/services/XXXXXXXXX/XXXXXXXXX/xxxxxxxxxxxxxxxxxxxxxxxx
SLACK_RT_PIPE_WEBHOOK_URL=https://hooks.slack.com/services/XXXXXXXXX/YYYYYYYYY/yyyyyyyyyyyyyyyyyyyyyyyyyy
# ── Finnhub API 設定 ───────────────────────────────────────
FINNHUB_API_KEY=your_finnhub_api_key_here
```

### Installation

```bash
# 1. Clone the repo
git clone https://github.com/alexandertsaidev/us-stock-analysis-rt-pipe.git
cd us-stock-analysis-rt-pipe

# 2. Copy and fill in environment variables
cp .env.example .env

# 3. Start all services
docker-compose up -d

# 4. Check running services
docker-compose ps
```

### Services

| Service       | Port | Description                    |
|---------------|------|--------------------------------|
| **Kafka**     | **9092** | Message broker             |
| **MinIO**     | **9000** | Object storage             |
| **MinIO Console** | **9001** | MinIO web UI           |

### Stop the Pipeline

```bash
docker-compose down
```

---

## 📖 Usage

Once all services are running, the **producer** will begin polling Finnhub every few seconds and publishing messages to Kafka. The **consumer** reads from Kafka and writes **batched records** to MinIO.

**Check MinIO for stored data:**
Open [http://localhost:9001](http://localhost:9001) in your browser and log in with your `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY`. Data will appear under the configured **bucket**, partitioned by **ticker** and **date**.

**Monitor Kafka consumer lag:**
```bash
docker exec -it kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group stock-consumer-group
```

---

## 🔔 Slack Notifications

This pipeline sends **real-time alerts** to a Slack channel via incoming webhook.

**Setup:**
1. Go to [Slack API: Incoming Webhooks](https://api.slack.com/messaging/webhooks)
2. Create a new app → **Enable Incoming Webhooks** → Add to a channel
3. Copy the webhook URL into your `.env` as `SLACK_WEBHOOK_URL`

**Notification events:**
- ✅ **Successful batch write** to MinIO (ticker count, record count, timestamp)
- ⚠️ **Consumer lag** exceeds threshold
- ❌ **Producer or consumer error** with stack trace summary

---

## 🧰 Tech Stack

| Layer | Technology |
|---|---|
| **Data Source** | Finnhub Stock API |
| **Ingestion** | Python |
| **Messaging** | Apache Kafka |
| **Storage** | MinIO (S3-compatible object store) |
| **Alerts** | Slack Incoming Webhook |
| **Orchestration** | Docker Compose |

---

## 📄 License

This project is licensed under the [MIT License](LICENSE).

---

## 📬 Connect

[![Hotmail](https://img.shields.io/badge/Hotmail-0078D4?style=flat&logo=microsoft-outlook&logoColor=white)](mailto:caiyuexun.hcd520201@hotmail.com)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=flat&logo=linkedin&logoColor=white)](https://linkedin.com/in/alexander-tsai-tw-eu)
