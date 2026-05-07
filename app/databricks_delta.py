import os
import re
from contextlib import contextmanager
from typing import Any, Dict, List, Optional

from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config, oauth_service_principal


IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def quote_table_name(table_name: str) -> str:
    parts = [part.strip() for part in table_name.split(".")]
    if not parts or any(not part for part in parts):
        raise ValueError("Table name must be a valid Delta table name.")
    if len(parts) > 3:
        raise ValueError("Use table, schema.table, or catalog.schema.table.")
    invalid = [part for part in parts if not IDENTIFIER_RE.match(part)]
    if invalid:
        raise ValueError(
            "Table names can only contain letters, numbers, and underscores, "
            "and each part must start with a letter or underscore."
        )
    return ".".join(f"`{part}`" for part in parts)


def _server_hostname() -> str:
    host = os.getenv("DATABRICKS_SERVER_HOSTNAME") or os.getenv("DATABRICKS_HOST")
    if not host:
        raise RuntimeError("DATABRICKS_HOST is not set in the app environment.")
    return host.removeprefix("https://").removeprefix("http://").rstrip("/")


def _http_path() -> str:
    configured_path = os.getenv("DATABRICKS_HTTP_PATH")
    if configured_path:
        return configured_path

    warehouse_id = _warehouse_id()
    if not warehouse_id:
        raise RuntimeError(
            "No SQL warehouse configured. Add Starter Warehouse as a SQL warehouse app resource "
            "or grant the app service principal Can use permission on Starter Warehouse."
        )
    return f"/sql/1.0/warehouses/{warehouse_id}"


def _warehouse_id() -> Optional[str]:
    configured_id = (
        os.getenv("WAREHOUSE_ID")
        or os.getenv("DATABRICKS_WAREHOUSE_ID")
        or os.getenv("SQL_WAREHOUSE_ID")
    )
    if configured_id:
        return configured_id

    warehouse_name = os.getenv("SQL_WAREHOUSE_NAME", "Starter Warehouse")
    try:
        for warehouse in WorkspaceClient().warehouses.list():
            if warehouse.name == warehouse_name:
                return warehouse.id
    except Exception:
        return None

    return None


def _credentials_provider():
    host = os.getenv("DATABRICKS_HOST")
    if not host:
        host = f"https://{_server_hostname()}"
    config = Config(
        host=host,
        client_id=os.getenv("DATABRICKS_CLIENT_ID"),
        client_secret=os.getenv("DATABRICKS_CLIENT_SECRET"),
    )
    return oauth_service_principal(config)


@contextmanager
def sql_connection():
    token = os.getenv("DATABRICKS_TOKEN")
    connect_kwargs: Dict[str, Any] = {
        "server_hostname": _server_hostname(),
        "http_path": _http_path(),
    }
    if token:
        connect_kwargs["access_token"] = token
    else:
        connect_kwargs["credentials_provider"] = _credentials_provider

    with sql.connect(**connect_kwargs) as connection:
        yield connection


def _execute(statement: str, parameters: Optional[Dict[str, Any]] = None):
    with sql_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(statement, parameters or {})


def _fetch_all(statement: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    with sql_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(statement, parameters or {})
            columns = [column[0] for column in cursor.description or []]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]


def create_synthetic_purchase_table(table_name: str, num_rows: int = 1000) -> None:
    if num_rows < 1:
        raise ValueError("num_rows must be a positive integer.")

    table = quote_table_name(table_name)
    statement = f"""
    CREATE OR REPLACE TABLE {table}
    USING DELTA
    AS
    SELECT
      CAST(id + 1 AS INT) AS purchase_id,
      CONCAT('CUST_', CAST(FLOOR(id / 5) + 1 AS INT)) AS customer_id,
      DATE_SUB(CURRENT_DATE(), CAST(FLOOR(rand(42) * 365) AS INT)) AS purchase_date,
      CAST(ROUND(rand(99) * 190 + 10, 2) AS DOUBLE) AS purchase_amount,
      ELEMENT_AT(ARRAY('electronics', 'apparel', 'home', 'sports', 'beauty'), CAST(PMOD(id, 5) + 1 AS INT)) AS product_category,
      ELEMENT_AT(ARRAY('US', 'CA', 'GB', 'DE', 'FR'), CAST(PMOD(id, 5) + 1 AS INT)) AS country,
      ELEMENT_AT(ARRAY('North', 'South', 'East', 'West', 'Central'), CAST(PMOD(id, 5) + 1 AS INT)) AS region,
      ELEMENT_AT(ARRAY('Seattle', 'Toronto', 'London', 'Berlin', 'Paris'), CAST(PMOD(id, 5) + 1 AS INT)) AS city,
      ELEMENT_AT(ARRAY('low', 'medium', 'high', 'premium'), CAST(PMOD(id, 4) + 1 AS INT)) AS income_bucket
    FROM range(0, {int(num_rows)})
    """
    _execute(statement)


def run_segmentation(
    input_table: str,
    output_table: str,
    num_clusters: int = 5,
    max_rows: Optional[int] = 1000000,
) -> List[Dict[str, Any]]:
    if num_clusters < 2:
        raise ValueError("num_clusters must be at least 2.")

    source = quote_table_name(input_table)
    target = quote_table_name(output_table)
    row_limit = int(max_rows) if max_rows and max_rows > 0 else 1000000
    source_columns = _table_columns(input_table)
    _validate_purchase_columns(source_columns)

    purchase_date_expr = (
        "TO_DATE(purchase_date)"
        if "purchase_date" in source_columns
        else "CURRENT_DATE()"
    )
    product_category_expr = (
        "COALESCE(product_category, 'unknown')"
        if "product_category" in source_columns
        else "'unknown'"
    )
    geography_exprs = {
        "country": "MAX(COALESCE(country, 'unknown'))" if "country" in source_columns else "'unknown'",
        "region": "MAX(COALESCE(region, 'unknown'))" if "region" in source_columns else "'unknown'",
        "city": "MAX(COALESCE(city, 'unknown'))" if "city" in source_columns else "'unknown'",
        "income_bucket": (
            "MAX(COALESCE(income_bucket, 'unknown'))"
            if "income_bucket" in source_columns
            else "'unknown'"
        ),
    }

    statement = f"""
    CREATE OR REPLACE TABLE {target}
    USING DELTA
    AS
    WITH source_rows AS (
      SELECT *
      FROM {source}
      LIMIT {row_limit}
    ),
    latest_purchase AS (
      SELECT MAX({purchase_date_expr}) AS latest_date
      FROM source_rows
    ),
    purchase_features AS (
      SELECT
        customer_id,
        SUM(COALESCE(CAST(purchase_amount AS DOUBLE), 0.0)) AS total_spent,
        COUNT(*) AS purchase_count,
        AVG(COALESCE(CAST(purchase_amount AS DOUBLE), 0.0)) AS avg_amount,
        MAX(DATEDIFF((SELECT latest_date FROM latest_purchase), {purchase_date_expr})) AS recency_days,
        COUNT(DISTINCT {product_category_expr}) AS unique_categories,
        {geography_exprs["country"]} AS country,
        {geography_exprs["region"]} AS region,
        {geography_exprs["city"]} AS city,
        {geography_exprs["income_bucket"]} AS income_bucket
      FROM source_rows
      GROUP BY customer_id
    ),
    category_totals AS (
      SELECT
        customer_id,
        {product_category_expr} AS product_category,
        COUNT(*) AS category_count
      FROM source_rows
      GROUP BY customer_id, {product_category_expr}
    ),
    category_counts AS (
      SELECT
        customer_id,
        product_category,
        category_count,
        ROW_NUMBER() OVER (
          PARTITION BY customer_id
          ORDER BY category_count DESC, product_category
        ) AS category_rank
      FROM category_totals
    ),
    scored AS (
      SELECT
        pf.*,
        COALESCE(cc.product_category, 'unknown') AS top_category,
        PERCENT_RANK() OVER (ORDER BY total_spent) AS spend_rank,
        PERCENT_RANK() OVER (ORDER BY purchase_count) AS frequency_rank,
        PERCENT_RANK() OVER (ORDER BY recency_days DESC) AS recency_rank,
        PERCENT_RANK() OVER (ORDER BY unique_categories) AS breadth_rank
      FROM purchase_features pf
      LEFT JOIN category_counts cc
        ON pf.customer_id = cc.customer_id
       AND cc.category_rank = 1
    ),
    segmented AS (
      SELECT
        *,
        NTILE({int(num_clusters)}) OVER (
          ORDER BY spend_rank * 0.40 + frequency_rank * 0.30 + recency_rank * 0.20 + breadth_rank * 0.10
        ) - 1 AS segment_id
      FROM scored
    )
    SELECT
      customer_id,
      CAST(segment_id AS INT) AS segment_id,
      total_spent,
      purchase_count,
      avg_amount,
      COALESCE(recency_days, 0) AS recency_days,
      unique_categories,
      country,
      region,
      city,
      income_bucket,
      top_category,
      CURRENT_TIMESTAMP() AS run_timestamp
    FROM segmented
    """
    _execute(statement)
    return summarize_cluster_assignments(output_table)


def _table_columns(table_name: str) -> set[str]:
    table = quote_table_name(table_name)
    rows = _fetch_all(f"DESCRIBE TABLE {table}")
    columns = set()
    for row in rows:
        column_name = row.get("col_name") or row.get("col_name".upper()) or next(iter(row.values()), None)
        if not column_name or str(column_name).startswith("#"):
            continue
        columns.add(str(column_name).lower())
    return columns


def _validate_purchase_columns(columns: set[str]) -> None:
    required_columns = {"customer_id", "purchase_amount"}
    missing_columns = sorted(required_columns - columns)
    if missing_columns:
        raise ValueError(
            "Input Delta table must include these columns: "
            + ", ".join(sorted(required_columns))
            + f". Missing: {', '.join(missing_columns)}."
        )


def summarize_cluster_assignments(output_table: str) -> List[Dict[str, Any]]:
    table = quote_table_name(output_table)
    rows = _fetch_all(
        f"""
        SELECT
          CAST(segment_id AS INT) AS segment_id,
          CAST(COUNT(DISTINCT customer_id) AS INT) AS customer_count,
          CAST(AVG(total_spent) AS DOUBLE) AS avg_spent,
          CAST(AVG(avg_amount) AS DOUBLE) AS avg_order_amount,
          CAST(AVG(recency_days) AS DOUBLE) AS avg_recency_days
        FROM {table}
        GROUP BY segment_id
        ORDER BY segment_id
        """
    )
    return [_normalize_summary_row(row) for row in rows]


def _normalize_summary_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "segment_id": int(row["segment_id"]),
        "customer_count": int(row["customer_count"]),
        "avg_spent": float(row["avg_spent"] or 0.0),
        "avg_order_amount": float(row["avg_order_amount"] or 0.0),
        "avg_recency_days": float(row["avg_recency_days"] or 0.0),
    }
