"""Environment configuration for multi-target pipeline deployments.

Reads catalog and schema names from spark.conf (set by bundle variables),
with fallback to dev defaults for local testing.
"""


def get_config():
    """Get environment configuration from spark.conf or use dev defaults.
    
    Returns:
        dict: Configuration with keys:
            - catalog: Unity Catalog name
            - bronze_schema: Bronze layer schema
            - silver_schema: Silver layer schema  
            - gold_schema: Gold layer schema
    """
    try:
        # In DLT pipeline context, spark is the implicit global
        return {
            "catalog": spark.conf.get("catalog", "workspace"),
            "bronze_schema": spark.conf.get("bronze_schema", "bronze_dev"),
            "silver_schema": spark.conf.get("silver_schema", "silver_dev"),
            "gold_schema": spark.conf.get("gold_schema", "gold_dev"),
        }
    except Exception:
        # Fallback for local testing or when spark is unavailable
        return {
            "catalog": "workspace",
            "bronze_schema": "bronze_dev",
            "silver_schema": "silver_dev",
            "gold_schema": "gold_dev",
        }
