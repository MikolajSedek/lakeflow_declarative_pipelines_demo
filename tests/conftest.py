import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Creates a local SparkSession shared across the entire test session."""
    return (
        SparkSession.builder.master("local[1]")
        .appName("lakeflow-pipeline-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


@pytest.fixture
def simple_df(spark: SparkSession):
    """Simple two-column DataFrame with id and value."""
    return spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])


@pytest.fixture
def mixed_case_df(spark: SparkSession):
    """DataFrame with mixed-case column names."""
    return spark.createDataFrame(
        [(1, "Alice", "Smith")],
        ["ID", "Person_Name", "SURNAME"],
    )


@pytest.fixture
def metadata_df(spark: SparkSession):
    """DataFrame with a nested struct column simulating _metadata."""
    schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("meta", StructType([StructField("file_name", StringType())])),
        ]
    )
    return spark.createDataFrame(
        [(1, {"file_name": "orders.csv"}), (2, {"file_name": "products.csv"})],
        schema,
    )


@pytest.fixture
def sensitive_df(spark: SparkSession):
    """DataFrame containing all three sensitive columns."""
    return spark.createDataFrame(
        [(1, "user@example.com", "Smith", "123 Main St")],
        ["id", "personal_email", "person_surname", "personal_address"],
    )


@pytest.fixture
def nonsense_df(spark: SparkSession):
    """DataFrame with a nonsense column alongside useful columns."""
    return spark.createDataFrame(
        [(1, 99, "keep"), (2, 42, "also_keep")],
        ["id", "nonsense_column", "important"],
    )
