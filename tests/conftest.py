import sys
import pytest

sys.path.insert(0, "/opt/pipeline")

from common.spark_session import get_spark


@pytest.fixture(scope="session")
def spark():
    s = get_spark("pytest-lakehouse")
    yield s
    s.stop()
