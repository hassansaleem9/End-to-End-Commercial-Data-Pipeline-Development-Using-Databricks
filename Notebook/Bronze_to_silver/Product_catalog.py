from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="retail_q.retail_silver.product_catalog",
    comment="Standardized product catalog with data quality rules",
)
@dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dp.expect("valid_unit_price", "unit_price >= 0")
@dp.expect_or_drop("valid_product_name", "product_name IS NOT NULL AND TRIM(product_name) != ''"
)
@dp.expect("valid_category", "category IS NOT NULL")
@dp.expect_or_drop("valid_launch_date", "launch_date IS NOT NULL")
def product_catalog_silver():
    """
    Silver layer for product catalog with standardization:
    - Trimmed and cleaned text fields
    - Uppercased category fields for consistency
    - Standardized brand names
    - Null handling for optional fields
    - Active status validation
    """
    return spark.readStream.option("skipChangeCommits", "true").table("retail_q.postgres_bronze.product_catalog").select(
        # Core identifiers
        F.col("product_id"),

        # Standardized product information
        F.initcap(F.col("product_name")).alias("product_name"),
        F.upper(F.col("category")).alias("category"),
        F.col("subcategory").alias("subcategory"),

        # Standardize brand: trim and title case, handle nulls
        F.when(F.col("brand").isNotNull(), F.initcap(F.col("brand")))
        .otherwise(F.lit("Unknown"))
        .alias("brand"),

        # Pricing
        F.col("unit_price"),

        # Supplier information
        F.col("supplier_name").alias("supplier_name"),

        # Dates and status
        F.col("launch_date"),
        F.col("is_active"),
        F.col("updated_at"),

        # SCD Type 2 tracking columns
        F.col("__START_AT"),
        F.col("__END_AT"),

        # Derived fields
        F.when(F.col("__END_AT").isNull(), True).otherwise(False).alias("is_current"),

        # Add processing timestamp for audit trail
        F.current_timestamp().alias("processed_at"),

        # Keep dates and timestamps as-is
        F.when(F.col("unit_price") > 50000, "PREMIUM")
        .when(F.col("unit_price") > 10000, "MID_RANGE")
        .otherwise("BUDGET")
        .alias("product_segment")
    )
