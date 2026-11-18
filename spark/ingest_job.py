import os
from functools import reduce

from pyspark.sql import functions as F

from pipeline import cli, processed_path, spark_session, upload

ALIASES = {
    "app_id": ("appid", "app_id"),
    "created": ("unix_timestamp_created", "timestamp_created"),
    "vote": ("voted_up", "recommended"),
    "playtime": ("playtime_at_review", "author_playtime_at_review"),
    "review_count": ("num_reviews", "author_num_reviews"),
}


def normalized_csv(spark, paths):
    frames = []
    for path in paths:
        raw = (spark.read.option("header", True).option("multiLine", True)
               .option("escape", '"').option("mode", "FAILFAST").csv(path))
        names = {column.strip().lower().lstrip("\ufeff"): column for column in raw.columns}
        selected = {}
        for field, aliases in ALIASES.items():
            name = next((names[alias] for alias in aliases if alias in names), None)
            if name is None:
                raise ValueError(f"Missing column {aliases} in {path}. Found: {raw.columns}")
            selected[field] = F.trim(F.col(f"`{name.replace('`', '``')}`")).alias(field)
        frames.append(raw.select(*selected.values()))
    rows = reduce(lambda left, right: left.unionByName(right), frames)
    epoch = F.col("created").cast("long")
    positive = F.lower("vote")
    playtime = F.col("playtime").cast("decimal(18,4)")
    unit = os.environ["PLAYTIME_UNIT"]
    if unit not in {"minutes", "hours"}:
        raise ValueError("PLAYTIME_UNIT must be minutes or hours")
    if unit == "hours":
        playtime = (playtime * 60).cast("decimal(18,4)")
    review_count = F.col("review_count").cast("long")
    return rows.select(
        F.col("app_id").cast("long").alias("app_id"),
        F.when(epoch.between(1, 4102444800),
               F.trunc(F.to_date(F.from_unixtime(epoch)), "month")).alias("review_month"),
        F.when(positive.isin("true", "1", "1.0"), True)
        .when(positive.isin("false", "0", "0.0"), False).alias("recommended"),
        F.when(playtime >= 0, playtime).alias("playtime_minutes"),
        F.when(review_count >= 0, review_count).alias("author_review_count"),
    )


def valid_reviews(rows):
    return rows.where((F.col("app_id") > 0) & F.col("review_month").isNotNull()
                      & F.col("recommended").isNotNull())


def aggregate(rows):
    time = F.col("playtime_minutes")
    activity = F.col("author_review_count")
    return (
        rows.withColumn("playtime_band", F.when(time.isNull(), "unknown")
                        .when(time < 60, "under_1h").when(time < 600, "1_to_10h")
                        .when(time < 3000, "10_to_50h").otherwise("50h_plus"))
        .withColumn("activity_band", F.when(activity.isNull(), "unknown")
                    .when(activity < 5, "under_5").when(activity < 20, "5_to_19").otherwise("20_plus"))
        .groupBy("review_month", "app_id", "playtime_band", "activity_band")
        .agg(F.count("*").alias("reviews"),
             F.sum(F.col("recommended").cast("long")).alias("recommendations"),
             F.count("playtime_minutes").alias("known_playtime"),
             F.coalesce(F.sum("playtime_minutes"), F.lit(0)).alias("total_playtime_minutes"))
    )


def process(spark, paths, clean_path, aggregate_path):
    valid = valid_reviews(normalized_csv(spark, paths))
    valid.repartition("review_month").write.mode("overwrite").partitionBy("review_month").parquet(clean_path)
    clean = spark.read.parquet(clean_path)
    aggregate(clean).write.mode("overwrite").parquet(aggregate_path)
    print(f"Parquet ready: {aggregate_path}", flush=True)


if __name__ == "__main__":
    args = cli()
    clean_path = processed_path(args.source, args.snapshot, "reviews")
    aggregate_path = processed_path(args.source, args.snapshot, "aggregates")
    paths = list(upload(args.source))
    spark = spark_session("Steam Reviews CSV to Parquet")
    try:
        process(spark, paths, clean_path, aggregate_path)
    finally:
        spark.stop()
