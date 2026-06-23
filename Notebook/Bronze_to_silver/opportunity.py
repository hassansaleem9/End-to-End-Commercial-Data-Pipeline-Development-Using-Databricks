from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Define streaming table for cleaned opportunity data
# This table will be published to retail_q.retail_silver.opportunity in Unity Catalog
@dp.table(
    name="retail_q.retail_silver.opportunity",
    comment="Salesforce opportunity data with core sales fields and data quality checks"
)
# Data quality expectations - validation rules applied to incoming records
@dp.expect_or_drop("non-null id", "id IS NOT NULL")  # Drop records without an ID
@dp.expect("non-null name", "name IS NOT NULL")  # Warn if name is null but allow record
@dp.expect("valid amount", "amount IS NULL OR amount >= 0")  # Warn on negative amounts
@dp.expect("valid probability", "probability IS NULL OR (probability >= 0 AND probability <= 100)")  # Check probability range
@dp.expect("valid stage",
           "stage_name IN ('Prospecting','Closed Won','Closed Lost')")  # Verify stage is valid
def opportunity_clean():
    """
    Transforms bronze Salesforce opportunity data into silver layer.
    
    Applies standardization:
    - Converts column names from PascalCase to snake_case
    - Adds calculated field for deal size segmentation
    - Filters to core business-relevant columns
    
    Returns:
        Streaming DataFrame with cleaned opportunity records
    """
    # Read source streaming table from bronze layer
    source_df = spark.readStream.option("skipChangeCommits", "true").table("retail_q.salesforce_bronze.opportunity")
    
    # Select and transform core sales opportunity columns
    return source_df.select(
        # Identity and metadata fields
        F.col("Id").alias("id"),
        F.col("IsDeleted").alias("is_deleted"),
        F.col("AccountId").alias("account_id"),
        F.col("Name").alias("name"),

        # Opportunity details
        F.col("Description").alias("description"),
        F.col("StageName").alias("stage_name"),
        F.col("Amount").alias("amount"),
        
        # Calculated field: Segment deals by amount into size categories
        F.when(F.col("amount") > 100000, "ENTERPRISE")
         .when(F.col("amount") > 25000, "MID_MARKET")
         .otherwise("SMALL")
         .alias("deal_size"),

        # Sales metrics
        F.col("Probability").alias("probability"),
        F.col("CloseDate").alias("close_date"),

        # Sales process fields
        F.col("Type").alias("type"),
        F.col("NextStep").alias("next_step"),
        F.col("LeadSource").alias("lead_source"),
        
        # Status indicators
        F.col("IsClosed").alias("is_closed"),
        F.col("IsWon").alias("is_won"),
        F.col("ForecastCategory").alias("forecast_category"),
        
        # Ownership
        F.col("OwnerId").alias("owner_id"),
        
        # Audit timestamps
        F.col("CreatedDate").alias("created_date"),
        F.col("LastModifiedDate").alias("last_modified_date")
    )
