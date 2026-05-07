import argparse
import logging
from pyspark.sql import SparkSession
from app.segmentation import run_segmentation, summarize_cluster_assignments

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="Run customer segmentation from Delta purchase data.")
    parser.add_argument("--input-table", required=True, help="Source Delta table or table name")
    parser.add_argument("--output-table", required=True, help="Target Delta table to store segment results")
    parser.add_argument("--num-clusters", type=int, default=5, help="Number of clusters for segmentation")
    parser.add_argument("--session-name", default="customer-segmentation", help="Spark session name")
    parser.add_argument("--max-rows", type=int, default=1000000, help="Maximum number of rows to process")
    return parser.parse_args()


def create_spark_session(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def main():
    args = parse_args()
    spark = create_spark_session(args.session_name)
    logger.info("Starting customer segmentation: input=%s output=%s clusters=%s", args.input_table, args.output_table, args.num_clusters)

    result_df = run_segmentation(
        spark=spark,
        input_table=args.input_table,
        output_table=args.output_table,
        num_clusters=args.num_clusters,
        max_rows=args.max_rows,
    )

    segment_summary = summarize_cluster_assignments(result_df)
    logger.info("Segment assignment summary: %s", segment_summary)
    spark.stop()


if __name__ == "__main__":
    main()
