"""Simple Lakeflow Declarative Pipeline that creates materialized views from CSV sources."""

from pyspark import pipelines as dp

SOURCE_FORMAT = "csv"
SOURCE_PATH_ROOT = "/Volumes/test_catalog/test_schema/test_volume/fake_source"

TARGET_CATALOG = "test_catalog"
TARGET_SCHEMA = "test_bronze_schema"

TABLES_LIST = ["fake_orders", "fake_products", "fake_users"]


def create_simple_materialized_view(
    table_name: str,
    source_path: str = SOURCE_PATH_ROOT,
    catalog: str = TARGET_CATALOG,
    schema: str = TARGET_SCHEMA,
    file_format: str = SOURCE_FORMAT,
    header: bool = True,
) -> None:
    """
    Creates a simple materialized view table using Lakeflow Declarative Pipelines.
    """
    table_path = f"{catalog}.{schema}.{table_name}_simple_table"
    full_source_path = f"{source_path}/{table_name}"

    @dp.materialized_view(name=table_path)
    def simple_table():
        source_frame = (
            spark.read.format(file_format).option("header", header).load(full_source_path)
        )
        return source_frame


def create_tables(tables_list: list[str]) -> None:
    """
    Create multiple materialized view tables.
    """
    for table in tables_list:
        create_simple_materialized_view(table)


if __name__ == "__main__":
    create_tables(TABLES_LIST)
