import dlt
import pyspark.sql.functions as F
from functools import reduce

from pydantic.dataclasses import dataclass
from pyspark.sql import DataFrame, Row

# configuration

SOURCE_FORMAT = "csv"
SOURCE_PATH_ROOT = "/Volumes/test_catalog/test_schema/test_volume/fake_source/"

TARGET_CATALOG = "test_catalog"
BRONZE_SCHEMA = "test_bronze_schema"
SILVER_SCHEMA = "test_silver_schema"
GOLD_SCHEMA = "test_gold_schema"

METADATA_COLUMN = "_metadata"
FILE_NAME_FIELD = "file_name"

TABLES_NAMES_LIST = ["fake_orders", "fake_products", "fake_users"]
NONSENSE_COLUMNS = ["nonsense_column"]

PRIME_KEY_COLUMNS = tuple(["id"])
TIMESTAMP_COLUMN = "timestamp"

SENSITIVE_COLUMNS = ["personal_email", "personal_address", "person_surname"]


# configuration object

@dataclass(frozen=True)
class TablePipelineConfig:
    """
    configuration for a single table pipeline
    """
    table_name: str
    root_source_path: str = SOURCE_PATH_ROOT
    target_catalog: str = TARGET_CATALOG
    bronze_schema: str = BRONZE_SCHEMA
    silver_schema: str = SILVER_SCHEMA
    gold_schema: str = GOLD_SCHEMA
    prime_key_columns: tuple[str, ...] = PRIME_KEY_COLUMNS
    timestamp_column: str = TIMESTAMP_COLUMN


# transformation functions, in a production code extract them to a module
# and add unit tests with pytest

def add_load_timestamp(
        input_frame: DataFrame,
        timestamp_col_name: str = "load_timestamp",
) -> DataFrame:
    """
    creates a new column with the current timestamp
    """

    # add a new column
    transformed_frame = (
        input_frame
        .withColumn(
            timestamp_col_name,
            F.current_timestamp()
        )
    )
    return transformed_frame


def extract_file_name_from_metadata(
        input_frame: DataFrame,
        metadata_column: str = METADATA_COLUMN,
        file_name_field: str = FILE_NAME_FIELD
) -> DataFrame:
    """
    extracts the file name from the metadata column and adds it as a new column
    """
    return input_frame.withColumn(
        "file_name", F.expr(f"{metadata_column}.{file_name_field}")
    )


def lower_all_column_names(input_frame: DataFrame) -> DataFrame:
    """
    lowers all column names in the input frame
    """
    lowered_columns = [
        F.col(column).alias(column.lower())
        for column in input_frame.columns
    ]
    return input_frame.select(lowered_columns)


def remove_nonsense_columns(
        input_frame: DataFrame,
        columns_to_drop: list[str] = NONSENSE_COLUMNS
) -> DataFrame:
    """
    removes all nonsense columns
    """
    return input_frame.drop(*columns_to_drop)


def anonymize_sensitive_data(
        input_frame: DataFrame,
        sensitive_cols: list[str] = SENSITIVE_COLUMNS,
        sha_hash_length: int = 256
) -> DataFrame:
    """
    anonymizes sensitive columns
    """
    input_cols = input_frame.columns
    # create transormation dict for columns if exist in the dataframe
    transformation_dict = {
        col: F.sha2(F.col(col), sha_hash_length).alias(col)
        for col in sensitive_cols
        if col in input_cols
    }
    # transform sensitive cols if needed and replace sensitive values
    if transformation_dict:
        return input_frame.withColumns(transformation_dict)
    # otherwise return source frame 
    return input_frame


# declarative stream tables, in a production code extract them to a module

def create_raw_bronze_table(
        bronze_table_name: str,
        table_path: str,
        source_format: str = SOURCE_FORMAT,
        header: bool = True,
        infer_schema: bool = True,
) -> None:
    """
    creates bronze table from a given source with basic transforms
    """

    @dlt.table(name=bronze_table_name)
    def raw_bronze_table():
        # raw streaming source
        raw_source = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", source_format)
            .option("header", header)
            .option("inferSchema", infer_schema)
            .load(table_path)
        )
        # run basic transforms
        transformed_source = (
            raw_source
            .transform(extract_file_name_from_metadata)
            .transform(add_load_timestamp)
        )
        return transformed_source


def create_silver_staging_table(
        silver_table_name: str,
        bronze_table_path: str
) -> None:
    """
    creates silver staging table from bronze table
    """

    @dlt.table(name=silver_table_name)
    def silver_staging_table():
        bronze_streaming_frame = dlt.readStream(bronze_table_path)
        silver_table = (
            bronze_streaming_frame
            .transform(remove_nonsense_columns)
            .transform(lower_all_column_names)
            .transform(anonymize_sensitive_data)
        )
        return silver_table


def create_gold_merged_table(
        silver_table_name: str,
        gold_table_name: str,
        prime_key_columns: tuple[str, ...] = PRIME_KEY_COLUMNS,
        timestamp_column: str = TIMESTAMP_COLUMN
) -> None:
    """
    creates gold table from silver staging table
    """

    # create target streaming frame
    dlt.create_streaming_table(
        name=gold_table_name,
        comment="gold table"
    )
    # merge into target using key cols and timestamp
    dlt.create_auto_cdc_flow(
        source=silver_table_name,
        target=gold_table_name,
        keys=list(prime_key_columns),
        sequence_by=timestamp_column
    )


def run_single_pipeline(
        tb_config: TablePipelineConfig,
) -> None:
    """
    create a single table pipeline by:
    1. creating a bronze table
    2. creating a silver staging table
    3. merging staging into gold table
    """

    # create name paths and prefixes
    source_table_path = f"{tb_config.root_source_path}/{tb_config.table_name}"
    bronze_schema_name_prefix = f"{tb_config.target_catalog}.{tb_config.bronze_schema}"
    silver_schema_name_prefix = f"{tb_config.target_catalog}.{tb_config.silver_schema}"
    gold_schema_name_prefix = f"{tb_config.target_catalog}.{tb_config.gold_schema}"

    # create names for tables in schemas and catalogs
    bronze_table_name = f"{bronze_schema_name_prefix}.{tb_config.table_name}_raw"
    silver_table_name = f"{silver_schema_name_prefix}.{tb_config.table_name}_staging"
    gold_table_name = f"{gold_schema_name_prefix}.{tb_config.table_name}_clean"

    # 1. create a bronze streaming table
    create_raw_bronze_table(bronze_table_name, source_table_path)

    # 2. create silver streaming staging table
    create_silver_staging_table(silver_table_name, bronze_table_name)

    # 3. stream merge staging into gold using keys and timestamp
    create_gold_merged_table(
        silver_table_name=silver_table_name,
        gold_table_name=gold_table_name,
        prime_key_columns=tb_config.prime_key_columns,
        timestamp_column=tb_config.timestamp_column,
    )


def run_all_pipelines(table_names_list: list = TABLES_NAMES_LIST) -> None:
    """
    runs all pipelines for a given list of tables
    """
    for table_name in table_names_list:
        # prepare table config
        tb_config = TablePipelineConfig(table_name=table_name)
        # run with the config
        run_single_pipeline(tb_config)


def aggregate_gold_tables(
        tables_names_list: list[str] = TABLES_NAMES_LIST,
        target_catalog: str = TARGET_CATALOG,
        gold_schema: str = GOLD_SCHEMA,
        aggregate_table_name: str = "summary_statistics_gold",
        gold_table_postfix: str = "_clean",
        id_column: str = "id",
) -> None:
    """
    aggregates gold tables stats into a single table
    """
    # we need table paths
    tables_paths = [
        f"{target_catalog}.{gold_schema}.{table_name}{gold_table_postfix}"
        for table_name in tables_names_list
    ]
    # and a gold table name
    gold_table_name = f"{target_catalog}.{gold_schema}.{aggregate_table_name}"

    # so we can have a summary table
    @dlt.table(name=gold_table_name)
    def summary_statistics_table():
        # create a list of frames
        frames_list = [
            dlt.read(table_path)
            .agg(
                F.count(F.lit(1)).alias("table_rows"),
                F.count(id_column).alias("unique_ids"),
            )
            .withColumn("table_name", F.lit(table_path))
            for table_path in tables_paths
        ]
        # reduce to a single frame using union
        reduced_frame = reduce(DataFrame.unionAll, frames_list)
        return reduced_frame


if __name__ == "__main__":
    run_all_pipelines()
    aggregate_gold_tables()
