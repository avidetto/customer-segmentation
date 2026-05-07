from typing import Optional

from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType, StructField, StructType
from pyspark.sql.window import Window


def load_purchase_data(spark: SparkSession, input_table: str, max_rows: int = 1000000) -> DataFrame:
    df = spark.read.table(input_table)
    if max_rows is not None and max_rows > 0:
        df = df.limit(max_rows)
    return df


def select_geography_columns(df: DataFrame) -> DataFrame:
    geography_cols = [col for col in ["country", "region", "city"] if col in df.columns]
    if not geography_cols:
        return df.withColumn("country", F.lit("unknown")).withColumn("region", F.lit("unknown"))
    return df


def generate_customer_features(df: DataFrame) -> DataFrame:
    df = select_geography_columns(df)

    if "purchase_date" in df.columns:
        date_col = F.to_date("purchase_date")
        df = df.withColumn("purchase_date", date_col)
        latest = df.agg(F.max("purchase_date").alias("latest_date")).collect()[0]["latest_date"]
        if latest is None:
            latest = F.current_date()
        df = df.withColumn("days_from_latest", F.datediff(F.lit(latest), F.col("purchase_date")))
    else:
        df = df.withColumn("days_from_latest", F.lit(0))

    category_col = "product_category" if "product_category" in df.columns else None
    income_col = "income_bucket" if "income_bucket" in df.columns else None

    partitions = ["customer_id"]
    if "customer_id" not in df.columns:
        raise ValueError("Input Delta table must contain a customer_id column.")

    summary = (
        df.groupBy("customer_id")
        .agg(
            F.sum(F.coalesce(F.col("purchase_amount"), F.lit(0.0))).alias("total_spent"),
            F.count(F.lit(1)).alias("purchase_count"),
            F.avg(F.coalesce(F.col("purchase_amount"), F.lit(0.0))).alias("avg_amount"),
            F.max("days_from_latest").alias("recency_days"),
            F.countDistinct(category_col).alias("unique_categories") if category_col else F.lit(0).alias("unique_categories"),
            F.max("country").alias("country"),
            F.max("region").alias("region"),
            F.max("city").alias("city"),
            F.max(income_col).alias("income_bucket") if income_col else F.lit("unknown").alias("income_bucket"),
        )
    )

    if category_col:
        window = Window.partitionBy("customer_id").orderBy(F.desc(F.count("product_category")))
        category_top = (
            df.groupBy("customer_id", category_col)
            .count()
            .withColumn("rank", F.row_number().over(window))
            .filter(F.col("rank") == 1)
            .select("customer_id", F.col(category_col).alias("top_category"))
        )
        summary = summary.join(category_top, on="customer_id", how="left")
    else:
        summary = summary.withColumn("top_category", F.lit("unknown"))

    return summary.fillna({"country": "unknown", "region": "unknown", "city": "unknown", "income_bucket": "unknown", "top_category": "unknown"})


def build_feature_pipeline(df: DataFrame) -> DataFrame:
    categorical_cols = [col for col in ["country", "region", "top_category", "income_bucket"] if col in df.columns]
    indexers = [StringIndexer(inputCol=col, outputCol=f"{col}_idx", handleInvalid="keep") for col in categorical_cols]
    encoders = [OneHotEncoder(inputCol=f"{col}_idx", outputCol=f"{col}_ohe") for col in categorical_cols]
    numeric_cols = ["total_spent", "purchase_count", "avg_amount", "recency_days", "unique_categories"]
    feature_cols = [f"{col}_ohe" for col in categorical_cols] + [col for col in numeric_cols if col in df.columns]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="keep")
    pipeline = Pipeline(stages=indexers + encoders + [assembler])
    return pipeline.fit(df).transform(df)


def run_segmentation(
    spark: SparkSession,
    input_table: str,
    output_table: str,
    num_clusters: int = 5,
    max_rows: Optional[int] = 1000000,
) -> DataFrame:
    purchase_df = load_purchase_data(spark, input_table, max_rows)
    features_df = generate_customer_features(purchase_df)
    training_df = build_feature_pipeline(features_df)

    kmeans = KMeans(featuresCol="features", predictionCol="segment_id", k=num_clusters, seed=42)
    model = kmeans.fit(training_df)
    clustered = model.transform(training_df)

    output_df = clustered.select(
        "customer_id",
        "segment_id",
        "total_spent",
        "purchase_count",
        "avg_amount",
        "recency_days",
        "unique_categories",
        "country",
        "region",
        "city",
        "income_bucket",
        "top_category",
    ).withColumn("run_timestamp", F.current_timestamp())

    output_df.write.format("delta").mode("overwrite").saveAsTable(output_table)
    return output_df


def create_synthetic_purchase_table(
    spark: SparkSession,
    table_name: str,
    num_rows: int = 1000,
    purchases_per_customer: int = 5,
) -> DataFrame:
    if num_rows < 1:
        raise ValueError("num_rows must be a positive integer.")

    categories = ["electronics", "apparel", "home", "sports", "beauty"]
    countries = ["US", "CA", "GB", "DE", "FR"]
    regions = ["North", "South", "East", "West", "Central"]
    cities = ["Seattle", "Toronto", "London", "Berlin", "Paris"]
    incomes = ["low", "medium", "high", "premium"]

    raw_df = spark.range(0, num_rows)

    base_df = (
        raw_df
        .withColumn("purchase_id", (F.col("id") + 1).cast(IntegerType()))
        .withColumn(
            "customer_id",
            F.concat(F.lit("CUST_"), (F.floor((F.col("id") / purchases_per_customer)) + 1).cast(IntegerType()).cast(StringType())),
        )
        .withColumn("purchase_date", F.date_sub(F.current_date(), F.floor(F.rand(42) * 365).cast(IntegerType())).cast(DateType()))
        .withColumn("purchase_amount", F.round(F.rand(99) * 190 + 10, 2).cast(DoubleType()))
        .withColumn(
            "product_category",
            F.element_at(F.array(*[F.lit(v) for v in categories]), (F.col("id") % len(categories)) + 1).cast(StringType()),
        )
        .withColumn(
            "country",
            F.element_at(F.array(*[F.lit(v) for v in countries]), (F.col("id") % len(countries)) + 1).cast(StringType()),
        )
        .withColumn(
            "region",
            F.element_at(F.array(*[F.lit(v) for v in regions]), (F.col("id") % len(regions)) + 1).cast(StringType()),
        )
        .withColumn(
            "city",
            F.element_at(F.array(*[F.lit(v) for v in cities]), (F.col("id") % len(cities)) + 1).cast(StringType()),
        )
        .withColumn(
            "income_bucket",
            F.element_at(F.array(*[F.lit(v) for v in incomes]), (F.col("id") % len(incomes)) + 1).cast(StringType()),
        )
        .select(
            "purchase_id",
            "customer_id",
            "purchase_date",
            "purchase_amount",
            "product_category",
            "country",
            "region",
            "city",
            "income_bucket",
        )
    )

    base_df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    return base_df


def summarize_cluster_assignments(df: DataFrame):
    summary_df = (
        df.groupBy("segment_id")
        .agg(
            F.countDistinct("customer_id").alias("customer_count"),
            F.avg("total_spent").alias("avg_spent"),
            F.avg("avg_amount").alias("avg_order_amount"),
            F.avg("recency_days").alias("avg_recency_days"),
        )
        .orderBy("segment_id")
    )
    return [row.asDict() for row in summary_df.collect()]
