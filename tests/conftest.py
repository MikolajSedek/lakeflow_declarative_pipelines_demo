import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    Creates a local SparkSession shared across the entire test session.
    """
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("lakeflow-pipeline-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
