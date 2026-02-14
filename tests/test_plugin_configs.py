from strata.infra.backends.base import BaseSourceConfig
from strata.infra.backends.duckdb import DuckDBSourceConfig
from strata.infra.backends.local.storage import LocalSourceConfig


class TestDuckDBSourceConfig:
    def test_creates_with_path(self):
        config = DuckDBSourceConfig(path="./data/test.parquet")
        assert config.path == "./data/test.parquet"
        assert config.format == "parquet"

    def test_accepts_format_override(self):
        config = DuckDBSourceConfig(path="./data.csv", format="csv")
        assert config.format == "csv"

    def test_inherits_from_base(self):
        config = DuckDBSourceConfig(path="./test.parquet")
        assert isinstance(config, BaseSourceConfig)


class TestLocalSourceConfig:
    def test_creates_with_path(self):
        config = LocalSourceConfig(path="./data/events.parquet")
        assert config.path == "./data/events.parquet"

    def test_supports_delta_format(self):
        config = LocalSourceConfig(path="./data/", format="delta")
        assert config.format == "delta"
