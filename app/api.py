from typing import List, Optional
import os

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

app = FastAPI(
    title="Customer Segmentation API",
    description="Backend API to run customer segmentation on Delta tables.",
    version="0.1.0",
)

spark: Optional[object] = None


def get_spark_session():
    global spark
    if spark is None:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("customer-segmentation-api").getOrCreate()
    return spark


class SegmentationRequest(BaseModel):
    input_table: str
    output_table: str
    num_clusters: int = Field(default=5, ge=2, description="Number of segments to compute")
    max_rows: int = Field(default=1000000, ge=1, description="Maximum number of rows to read from source table")


class SegmentSummaryItem(BaseModel):
    segment_id: int
    customer_count: int
    avg_spent: float
    avg_order_amount: float
    avg_recency_days: float


class RunSegmentationResponse(BaseModel):
    status: str
    message: str
    summary: List[SegmentSummaryItem]


@app.post("/api/run-segmentation", response_model=RunSegmentationResponse)
async def run_segmentation_endpoint(request: SegmentationRequest):
    from pyspark.sql import functions as F
    from app.segmentation import run_segmentation, summarize_cluster_assignments
    
    spark = get_spark_session()
    try:
        result_df = run_segmentation(
            spark=spark,
            input_table=request.input_table,
            output_table=request.output_table,
            num_clusters=request.num_clusters,
            max_rows=request.max_rows,
        )
        summary = summarize_cluster_assignments(result_df)
        return {
            "status": "success",
            "message": "Segmentation completed successfully.",
            "summary": summary,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/segment-summary", response_model=List[SegmentSummaryItem])
async def get_segment_summary(output_table: str):
    from pyspark.sql import functions as F
    
    spark = get_spark_session()
    try:
        summary_df = (
            spark.table(output_table)
            .groupBy("segment_id")
            .agg(
                F.countDistinct("customer_id").alias("customer_count"),
                F.avg("total_spent").alias("avg_spent"),
                F.avg("avg_amount").alias("avg_order_amount"),
                F.avg("recency_days").alias("avg_recency_days"),
            )
            .orderBy("segment_id")
        )
        return [row.asDict() for row in summary_df.collect()]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# Mount React frontend static files at the end after all API routes are defined
web_dist_path = os.path.join(os.path.dirname(__file__), "..", "web", "dist")
if os.path.exists(web_dist_path):
    app.mount("/", StaticFiles(directory=web_dist_path, html=True), name="static")

