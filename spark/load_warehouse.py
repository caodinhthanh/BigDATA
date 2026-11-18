import os
from io import StringIO

import psycopg2
from psycopg2 import sql

from pipeline import cli, object_store, processed_path, spark_session

COLUMNS = "review_month, app_id, playtime_band, activity_band, reviews, recommendations, known_playtime, total_playtime_minutes"
TABLE_SQL = """
    review_month date NOT NULL,
    app_id bigint NOT NULL,
    playtime_band text NOT NULL,
    activity_band text NOT NULL,
    reviews bigint NOT NULL CHECK (reviews > 0),
    recommendations bigint NOT NULL CHECK (recommendations BETWEEN 0 AND reviews),
    known_playtime bigint NOT NULL CHECK (known_playtime BETWEEN 0 AND reviews),
    total_playtime_minutes numeric NOT NULL CHECK (total_playtime_minutes >= 0),
    PRIMARY KEY (review_month, app_id, playtime_band, activity_band)
"""


def load(cursor, rows, schema):
    cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
    table = sql.Identifier(schema, "review_aggregates")
    cursor.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(table, sql.SQL(TABLE_SQL)))
    cursor.execute(sql.SQL("CREATE TEMP TABLE next_aggregates (LIKE {} INCLUDING ALL)").format(table))
    buffer = StringIO()
    total = 0
    for row in rows:
        buffer.write("\t".join(str(value) for value in row) + "\n")
        total += 1
        if total % 10000 == 0:
            buffer.seek(0)
            cursor.copy_from(buffer, "next_aggregates")
            buffer.seek(0)
            buffer.truncate()
    if buffer.tell():
        buffer.seek(0)
        cursor.copy_from(buffer, "next_aggregates")
    if not total:
        raise ValueError("No usable reviews found. Previous warehouse results were preserved.")
    cursor.execute(sql.SQL("TRUNCATE {}").format(table))
    cursor.execute(sql.SQL("INSERT INTO {} SELECT * FROM next_aggregates").format(table))
    print(f"Loaded {total} aggregate rows into {schema}.review_aggregates", flush=True)


if __name__ == "__main__":
    args = cli()
    path = processed_path(args.source, args.snapshot, "aggregates")
    object_store().stat_object("steam-reviews", path.split("/", 3)[3] + "/_SUCCESS")
    spark = spark_session("Steam Reviews Parquet to PostgreSQL")
    try:
        totals = spark.read.parquet(path).select(*COLUMNS.split(", "))
        with psycopg2.connect(host=os.environ["POSTGRES_HOST"], dbname=os.environ["POSTGRES_DB"],
                              user=os.environ["POSTGRES_USER"], password=os.environ["POSTGRES_PASSWORD"]) as connection:
            with connection.cursor() as cursor:
                load(cursor, totals.toLocalIterator(), "raw_demo" if args.source == "demo" else "raw")
    finally:
        spark.stop()
