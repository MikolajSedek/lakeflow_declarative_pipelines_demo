"""
Unit tests for the ThreadPoolExecutor-based parallel write logic introduced in
01.create_fake_data.py.

The notebook file cannot be imported directly (it relies on the Databricks
``spark`` global), so these tests re-create the same ``write_frame_config_to_path``
and ``write_all_configs_parallel`` functions and exercise them against a local
SparkSession.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import NamedTuple
from unittest.mock import patch

import pytest
from loguru import logger
from pyspark.sql import DataFrame, Row, SparkSession

pytestmark = pytest.mark.spark

# ---------------------------------------------------------------------------
# Local copies of the production helpers (mirrors 01.create_fake_data.py)
# ---------------------------------------------------------------------------


class _FrameConfig(NamedTuple):
    """Mirrors data_generation.FrameConfig for import-free testing."""

    name: str
    df: DataFrame


def _write_frame_config_to_path(root_path: str, config: _FrameConfig) -> None:
    config.df.write.mode("append").csv(f"{root_path}/{config.name}", header=True)


def _write_all_configs_parallel(
    root_path: str,
    configs: list[_FrameConfig],
    max_workers: int = 3,
) -> None:
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(_write_frame_config_to_path, root_path, cfg): cfg.name for cfg in configs
        }
        for future in as_completed(futures):
            table_name = futures[future]
            future.result()
            logger.info("Finished writing {}", table_name)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def tmp_root(tmp_path: Path) -> Path:
    """Provide a unique, auto-cleaned temp root directory for each parallel-write test."""
    return tmp_path / "parallel_writes"


@pytest.fixture
def users_df(spark: SparkSession) -> DataFrame:
    """Should provide a small users DataFrame for write tests."""
    return spark.createDataFrame([Row(id=i, name=f"user_{i}") for i in range(1, 11)])


@pytest.fixture
def products_df(spark: SparkSession) -> DataFrame:
    """Should provide a small products DataFrame for write tests."""
    return spark.createDataFrame([Row(id=i, product=f"prod_{i}") for i in range(1, 6)])


@pytest.fixture
def orders_df(spark: SparkSession) -> DataFrame:
    """Should provide a small orders DataFrame for write tests."""
    return spark.createDataFrame([Row(id=i, amount=float(i * 10)) for i in range(1, 8)])


@pytest.fixture
def three_configs(users_df, products_df, orders_df) -> list[_FrameConfig]:
    """Should provide three independent FrameConfigs for parallel write tests."""
    return [
        _FrameConfig("users", users_df),
        _FrameConfig("products", products_df),
        _FrameConfig("orders", orders_df),
    ]


# ---------------------------------------------------------------------------
# Tests: basic correctness
# ---------------------------------------------------------------------------


def test_parallel_write_creates_output_directories(
    tmp_root: Path, three_configs: list[_FrameConfig]
) -> None:
    """Should create one output directory per FrameConfig."""
    _write_all_configs_parallel(str(tmp_root), three_configs)
    for cfg in three_configs:
        assert (tmp_root / cfg.name).is_dir()


def test_parallel_write_row_counts_match(
    spark: SparkSession, tmp_root: Path, three_configs: list[_FrameConfig]
) -> None:
    """Should write the correct number of rows for each DataFrame."""
    _write_all_configs_parallel(str(tmp_root), three_configs)

    assert spark.read.csv(str(tmp_root / "users"), header=True).count() == 10
    assert spark.read.csv(str(tmp_root / "products"), header=True).count() == 5
    assert spark.read.csv(str(tmp_root / "orders"), header=True).count() == 7


def test_parallel_write_column_names_preserved(
    spark: SparkSession, tmp_root: Path, three_configs: list[_FrameConfig]
) -> None:
    """Should preserve column names through the write-read round trip."""
    _write_all_configs_parallel(str(tmp_root), three_configs)

    assert set(spark.read.csv(str(tmp_root / "users"), header=True).columns) == {"id", "name"}
    assert set(spark.read.csv(str(tmp_root / "products"), header=True).columns) == {
        "id",
        "product",
    }
    assert set(spark.read.csv(str(tmp_root / "orders"), header=True).columns) == {"id", "amount"}


def test_parallel_write_data_values_correct(
    spark: SparkSession, tmp_root: Path, three_configs: list[_FrameConfig]
) -> None:
    """Should preserve actual data values through the round trip."""
    _write_all_configs_parallel(str(tmp_root), three_configs)

    users_back = spark.read.csv(str(tmp_root / "users"), header=True)
    names = {row["name"] for row in users_back.collect()}
    assert "user_1" in names
    assert "user_10" in names


# ---------------------------------------------------------------------------
# Tests: edge cases
# ---------------------------------------------------------------------------


def test_parallel_write_empty_configs_is_noop(tmp_root: Path) -> None:
    """Should complete without error when given an empty config list."""
    _write_all_configs_parallel(str(tmp_root), [])
    assert not tmp_root.exists()


def test_parallel_write_single_config(spark: SparkSession, tmp_root: Path, users_df) -> None:
    """Should handle a single-element config list correctly."""
    configs = [_FrameConfig("only_users", users_df)]
    _write_all_configs_parallel(str(tmp_root), configs)

    assert spark.read.csv(str(tmp_root / "only_users"), header=True).count() == 10


def test_parallel_write_max_workers_one(
    spark: SparkSession, tmp_root: Path, three_configs: list[_FrameConfig]
) -> None:
    """Should still work correctly with max_workers=1 (sequential fallback)."""
    _write_all_configs_parallel(str(tmp_root), three_configs, max_workers=1)

    assert spark.read.csv(str(tmp_root / "users"), header=True).count() == 10
    assert spark.read.csv(str(tmp_root / "products"), header=True).count() == 5
    assert spark.read.csv(str(tmp_root / "orders"), header=True).count() == 7


def test_parallel_write_max_workers_exceeds_configs(
    spark: SparkSession, tmp_root: Path, three_configs: list[_FrameConfig]
) -> None:
    """Should work when max_workers exceeds the number of configs."""
    _write_all_configs_parallel(str(tmp_root), three_configs, max_workers=10)

    assert spark.read.csv(str(tmp_root / "users"), header=True).count() == 10


# ---------------------------------------------------------------------------
# Tests: exception handling
# ---------------------------------------------------------------------------


def test_parallel_write_propagates_write_exception(tmp_root: Path) -> None:
    """Should propagate exceptions raised during a Spark write action."""
    bad_config = _FrameConfig("broken", None)  # type: ignore[arg-type]
    with pytest.raises(AttributeError):
        _write_all_configs_parallel(str(tmp_root), [bad_config])


def test_parallel_write_propagates_first_exception_among_mixed(
    tmp_root: Path,
    users_df,
) -> None:
    """Should propagate the exception even when other configs succeed."""
    configs = [
        _FrameConfig("good", users_df),
        _FrameConfig("bad", None),  # type: ignore[arg-type]
    ]
    with pytest.raises(AttributeError):
        _write_all_configs_parallel(str(tmp_root), configs)


# ---------------------------------------------------------------------------
# Tests: concurrency behaviour
# ---------------------------------------------------------------------------


def test_parallel_write_submits_all_futures(
    tmp_root: Path, three_configs: list[_FrameConfig]
) -> None:
    """Should submit one future per config to the thread pool."""
    submitted_names: list[str] = []
    original_submit = ThreadPoolExecutor.submit

    def tracking_submit(self, fn, *args, **kwargs):
        cfg = args[1]  # second positional arg is the FrameConfig
        submitted_names.append(cfg.name)
        return original_submit(self, fn, *args, **kwargs)

    with patch.object(ThreadPoolExecutor, "submit", tracking_submit):
        _write_all_configs_parallel(str(tmp_root), three_configs)

    assert sorted(submitted_names) == ["orders", "products", "users"]


def test_parallel_write_all_configs_complete(
    tmp_root: Path, three_configs: list[_FrameConfig]
) -> None:
    """Should call future.result() for every submitted config (no silent drops)."""
    completed_names: list[str] = []

    # Save a direct reference to the real write function before patching
    real_write = _write_frame_config_to_path

    def tracking_write(root_path: str, config: _FrameConfig) -> None:
        real_write(root_path, config)
        completed_names.append(config.name)

    with patch(f"{__name__}._write_frame_config_to_path", side_effect=tracking_write):
        _write_all_configs_parallel(str(tmp_root), three_configs)

    assert sorted(completed_names) == ["orders", "products", "users"]


# ---------------------------------------------------------------------------
# Tests: append mode semantics
# ---------------------------------------------------------------------------


def test_parallel_write_append_mode_accumulates(
    spark: SparkSession, tmp_root: Path, users_df
) -> None:
    """Should append data on repeated writes (not overwrite)."""
    configs = [_FrameConfig("users", users_df)]

    _write_all_configs_parallel(str(tmp_root), configs)
    _write_all_configs_parallel(str(tmp_root), configs)

    assert spark.read.csv(str(tmp_root / "users"), header=True).count() == 20
