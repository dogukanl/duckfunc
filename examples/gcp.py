import os
import duckdb
import structlog
from textwrap import dedent
from duckfunc.providers.gcp import CloudFunctionsProvider

REGION = os.environ.get("REGION")
PROJECT = os.environ.get("PROJECT")
FUNCTION_NAME = os.environ.get("FUNCTION_NAME")
URL = f"https://{REGION}-{PROJECT}.cloudfunctions.net/{FUNCTION_NAME}"

structlog.get_logger().info(f"using: {URL}")

sql = dedent("""
    INSTALL httpfs;
    LOAD httpfs;

    SELECT *
    FROM read_csv('s3://datasets-documentation/nyc-taxi/taxi_zone_lookup.csv')
    WHERE
        Borough = 'Manhattan' AND 
        Zone LIKE 'Upper West Side %';
""")

conn = duckdb.connect()

fp = CloudFunctionsProvider(url=URL)
response = fp.query(sql)

if not response.success:
    print(response.error)
    exit(1)

conn.register("response", response.table)
print(conn.query("select * from response"))
