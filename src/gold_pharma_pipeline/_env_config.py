"""Environment configuration for the gold layer DLT pipeline.

Reads catalog and schema names from spark.conf (set by bundle variables),
with fallback to dev defaults for local testing.
"""


def get_config():
    """Get environment configuration from spark.conf or use dev defaults.

    Returns:
        dict: Configuration with keys:
            - catalog: Unity Catalog name
            - silver_schema: Silver layer schema
            - gold_schema: Gold layer schema
    """
    try:
        return {
            "catalog": spark.conf.get("catalog", "workspace"),
            "silver_schema": spark.conf.get("silver_schema", "silver_dev"),
            "gold_schema": spark.conf.get("gold_schema", "gold_dev"),
        }
    except Exception:
        return {
            "catalog": "workspace",
            "silver_schema": "silver_dev",
            "gold_schema": "gold_dev",
        }
