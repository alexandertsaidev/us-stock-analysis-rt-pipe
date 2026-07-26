import os
from dotenv import load_dotenv, find_dotenv

import snowflake.connector

load_dotenv(find_dotenv())

snow_conn = snowflake.connector.connect(
    user      = os.getenv("SNOWFLAKE_USER"),
    password  = os.getenv("SNOWFLAKE_PASSWORD"),
    account   = os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE"),
    database  = os.getenv("SNOWFLAKE_DATABASE"),
    schema    = os.getenv("SNOWFLAKE_SCHEMA"),
)