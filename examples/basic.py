import duckdb
from textwrap import dedent
from duckfunc.providers.basic import BasicProvider

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

bp = BasicProvider("http://localhost:8080", timeout=3600)
response = bp.query(sql)

if not response.success:
    print(response.error)
    exit(1)

conn.register("response", response.table)
print(conn.query("select * from response"))
