from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="retail_q.retail_silver.inventory",
    comment="Silver layer inventory with standardization and data quality rules"
)
@dp.expect_or_drop("valid_inventory_id", "inventory_id IS NOT NULL")
@dp.expect("valid_product_id", "product_id IS NOT NULL")
@dp.expect("valid_store_id", "store_id IS NOT NULL")
@dp.expect("valid_stock_quantity", "stock_quantity >= 0")
@dp.expect("valid_reorder_level", "reorder_level >= 0")
def inventory():
    return (
        spark.readStream.option("skipChangeCommits", "true").table("retail_q.postgres_bronze.inventory")
        .select(
            F.col("inventory_id"),
            F.col("product_id"),
            F.col("store_id"),
            F.col("stock_quantity"),
            F.col("reorder_level"),

            F.when(
                F.col("stock_quantity") < F.col("reorder_level"),
                "LOW_STOCK"
            ).otherwise("HEALTHY").alias("inventory_status"),
                
            F.col("warehouse_location"),
            F.col("last_stock_update")
        )
    )
