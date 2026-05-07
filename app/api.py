from typing import List, Optional
import os

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
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


class PopulateSyntheticDataRequest(BaseModel):
    table_name: str
    num_rows: int = Field(default=1000, ge=100, le=200000, description="Number of synthetic purchase rows to generate")


# Define API routes first before the catch-all static routes
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


@app.post("/api/populate-synthetic-data")
async def populate_synthetic_data_endpoint(request: PopulateSyntheticDataRequest):
    from app.segmentation import create_synthetic_purchase_table

    spark = get_spark_session()
    try:
        create_synthetic_purchase_table(
            spark=spark,
            table_name=request.table_name,
            num_rows=request.num_rows,
        )
        return {
            "status": "success",
            "message": f"Synthetic data populated into table {request.table_name}.",
            "table_name": request.table_name,
            "rows": request.num_rows,
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


# Static file serving routes (defined after API routes)
@app.get("/")
async def root():
    """Serve the React app index.html at the root path."""
    # Try multiple possible paths for the built frontend
    possible_paths = [
        os.path.join(os.path.dirname(__file__), "static", "index.html"),  # In app/static/
        os.path.join(os.path.dirname(__file__), "..", "web", "dist", "index.html"),  # Relative to app/
        os.path.join(os.getcwd(), "web", "dist", "index.html"),  # Relative to cwd
        "/app/web/dist/index.html",  # Absolute path in Databricks
        "web/dist/index.html",  # Relative to app root
    ]

    for web_dist_path in possible_paths:
        if os.path.exists(web_dist_path):
            return FileResponse(web_dist_path)

    # If none found, return a helpful error with debugging info
    cwd = os.getcwd()
    app_dir = os.path.dirname(__file__)
    debug_info = {
        "message": "React frontend not found. Build with: cd web && npm install && npm run build",
        "current_working_directory": cwd,
        "app_directory": app_dir,
        "tried_paths": possible_paths,
        "files_in_app_static": list(os.listdir(os.path.join(app_dir, "static"))) if os.path.exists(os.path.join(app_dir, "static")) else "app/static does not exist",
        "files_in_web_dist": list(os.listdir(os.path.join(app_dir, "..", "web", "dist"))) if os.path.exists(os.path.join(app_dir, "..", "web", "dist")) else "web/dist does not exist"
    }
    return debug_info


@app.get("/{path_name:path}")
async def serve_static(path_name: str):
    """Serve static files and fallback to index.html for SPA routing."""
    # Don't serve API routes through this handler
    if path_name.startswith("api/"):
        raise HTTPException(status_code=404, detail="Not Found")

    app_dir = os.path.dirname(__file__)
    static_path = os.path.join(app_dir, "static")
    file_path = os.path.join(static_path, path_name)
    
    # Security check: ensure the file is within static_path
    if not os.path.abspath(file_path).startswith(os.path.abspath(static_path)):
        raise HTTPException(status_code=404, detail="File not found")
    
    # If file exists, serve it
    if os.path.isfile(file_path):
        return FileResponse(file_path)
    
    # Otherwise, fallback to index.html for SPA routing
    index_path = os.path.join(static_path, "index.html")
    return FileResponse(index_path)

    raise HTTPException(status_code=404, detail="File not found")

