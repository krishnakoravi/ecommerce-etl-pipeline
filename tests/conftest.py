"""Shared pytest fixtures: a single local SparkSession reused across the test session
(creating a new SparkSession per test is slow and unnecessary)."""
import pytest

from src.transform.spark_utils import get_spark


@pytest.fixture(scope="session")
def spark():
    session = get_spark(app_name="pytest-ecommerce-etl", shuffle_partitions=2)
    yield session
    session.stop()
