import psycopg2
import configparser
from sqlalchemy import create_engine

def get_conn(config_file, section):
    """
    從 config.ini 建立 PostgreSQL 連線
    """
    config = configparser.ConfigParser()
    config.read(config_file)

    try:
        db_config = {
            "host": config.get(section, "host"),
            "port": config.getint(section, "port"),
            "database": config.get(section, "database"),
            "user": config.get(section, "user"),
            "password": config.get(section, "password")
        }

        conn = psycopg2.connect(**db_config)
        return conn

    except (psycopg2.Error, configparser.Error) as e:
        print(f"db 連線或設定錯誤: {e}")
        return None

# 全專案共用 engine
engine = create_engine(
    "postgresql+psycopg2://postgres:11111111@localhost:5433/stock",
    pool_size=10,        # 連線池大小
    max_overflow=20,     # 超過 pool_size 的最大連線
    pool_pre_ping=True   # 自動檢查連線可用
)

