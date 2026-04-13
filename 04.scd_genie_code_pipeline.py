"""SCD Type 2 Pipeline for tracking product changes in orders.

This pipeline implements Slowly Changing Dimension Type 2 for the fake_orders table,
tracking historical changes in the product_id column while maintaining SCD Type 1
semantics for all other columns.
"""

from pyspark import pipelines as dp

# Configuration
SOURCE_TABLE = "test_catalog.test_silver_schema.fake_orders_staging"
TARGET_CATALOG = "test_catalog"
GOLD_SCHEMA = "test_gold_schema"
TARGET_TABLE_NAME = "fake_orders_scd2"

# Primary key and sequencing columns
KEY_COLUMN = "id"
SEQUENCE_COLUMN = "timestamp"

# Column to track history for (SCD Type 2)
HISTORY_TRACKED_COLUMNS = ["productid"]


def create_orders_scd2_table() -> None:
    """
    Creates an SCD Type 2 table for order data tracking product changes.

    - Key: id
    - Sequence by: timestamp
    - History tracking: productid only (other columns use SCD Type 1)
    - Output includes __START_AT and __END_AT columns for temporal tracking
    """
    target_table_name = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.{TARGET_TABLE_NAME}"

    # Step 1: Create the target streaming table for SCD Type 2
    # This will automatically include __START_AT and __END_AT columns
    dp.create_streaming_table(
        name=target_table_name,
        comment="SCD Type 2 gold table tracking historical changes to the productid column in orders. Includes __START_AT and __END_AT columns for temporal tracking; all other columns use SCD Type 1 semantics.",
    )

    # Step 2: Define the Auto CDC flow with SCD Type 2 and selective history tracking
    dp.create_auto_cdc_flow(
        target=target_table_name,
        source=SOURCE_TABLE,
        keys=[KEY_COLUMN],
        sequence_by=SEQUENCE_COLUMN,
        stored_as_scd_type=2,
        track_history_column_list=HISTORY_TRACKED_COLUMNS,
        ignore_null_updates=True,
    )


if __name__ == "__main__":
    create_orders_scd2_table()
