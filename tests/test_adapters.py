"""Tests for Golden Suite adapters (mocked)."""
import pytest
from unittest.mock import MagicMock, patch

from goldenpipe.models.context import PipeContext, StageStatus


class TestScanStage:
    def test_info(self):
        from goldenpipe.adapters.check import ScanStage
        s = ScanStage()
        assert s.info.name == "goldencheck.scan"
        assert "findings" in s.info.produces
        assert "df" in s.info.consumes

    @patch("goldenpipe.adapters.check.HAS_CHECK", False)
    def test_validate_raises_without_tool(self):
        from goldenpipe.adapters.check import ScanStage
        s = ScanStage()
        with pytest.raises(RuntimeError, match="not installed"):
            s.validate(PipeContext())

    @patch("goldenpipe.adapters.check.HAS_CHECK", True)
    def test_run_success(self, sample_df):
        from goldenpipe.adapters import check
        mock_result = MagicMock()
        mock_result.findings = [{"severity": "warning", "check": "nulls"}]
        with patch.object(check, "_scan", return_value=mock_result):
            from goldenpipe.adapters.check import ScanStage
            s = ScanStage()
            ctx = PipeContext(df=sample_df, metadata={"source": "test.csv"})
            result = s.run(ctx)
            assert result.status == StageStatus.SUCCESS
            assert "findings" in ctx.artifacts


class TestTransformStage:
    def test_info(self):
        from goldenpipe.adapters.flow import TransformStage
        s = TransformStage()
        assert s.info.name == "goldenflow.transform"
        assert "df" in s.info.produces

    @patch("goldenpipe.adapters.flow.HAS_FLOW", False)
    def test_validate_raises_without_tool(self):
        from goldenpipe.adapters.flow import TransformStage
        s = TransformStage()
        with pytest.raises(RuntimeError, match="not installed"):
            s.validate(PipeContext())


class TestDedupeStage:
    def test_info(self):
        from goldenpipe.adapters.match import DedupeStage
        s = DedupeStage()
        assert s.info.name == "goldenmatch.dedupe"
        assert "clusters" in s.info.produces
        assert "golden" in s.info.produces
        assert "df" in s.info.consumes

    @patch("goldenpipe.adapters.match.HAS_MATCH", False)
    def test_validate_raises_without_tool(self):
        from goldenpipe.adapters.match import DedupeStage
        s = DedupeStage()
        with pytest.raises(RuntimeError, match="not installed"):
            s.validate(PipeContext())


class TestBuildConfigFromContexts:
    """Tests for _build_config_from_contexts geo-compound blocking."""

    def test_geo_columns_compound_with_name_blocking(self):
        """When geo columns exist with NAME-type contexts, blocking should compound geo + name."""
        from goldenpipe.adapters.match import _build_config_from_contexts
        from goldenpipe.models.column_context import ColumnType

        contexts = [
            MagicMock(name="last_name", inferred_type=ColumnType.NAME, is_identifier=True),
            MagicMock(name="state", inferred_type=ColumnType.GEO, is_identifier=False),
        ]
        contexts[0].name = "last_name"
        contexts[1].name = "state"

        import polars as pl
        states = ["AL", "CA", "NY", "TX", "FL"]
        names = ["Smith", "Jones", "Doe"]
        rows = [{"last_name": n, "state": s} for s in states for n in names]
        df = pl.DataFrame(rows)

        config = _build_config_from_contexts(contexts, df)
        assert config is not None
        primary_fields = config.blocking.keys[0].fields
        assert "state" in primary_fields, f"Expected 'state' in blocking keys, got {primary_fields}"

    def test_geo_compounds_with_string_fallback(self):
        """When no NAME columns but STRING + GEO exist, blocking should compound geo + string."""
        from goldenpipe.adapters.match import _build_config_from_contexts
        from goldenpipe.models.column_context import ColumnType

        # Simulate hospital data: facility_name is STRING (not NAME), state is GEO
        contexts = [
            MagicMock(name="facility_name", inferred_type=ColumnType.STRING, is_identifier=False),
            MagicMock(name="state", inferred_type=ColumnType.GEO, is_identifier=False),
            MagicMock(name="address", inferred_type=ColumnType.STRING, is_identifier=False),
        ]
        contexts[0].name = "facility_name"
        contexts[1].name = "state"
        contexts[2].name = "address"

        import polars as pl
        # Need enough unique facility names to pass cardinality filter (min 50)
        states = ["AL", "CA", "NY", "TX", "FL"]
        names = [f"HOSPITAL {i}" for i in range(60)]
        rows = [{"facility_name": n, "state": s, "address": f"123 {s} ST"}
                for s in states for n in names]
        df = pl.DataFrame(rows)

        config = _build_config_from_contexts(contexts, df)
        assert config is not None
        primary_fields = config.blocking.keys[0].fields
        assert "state" in primary_fields, f"Expected 'state' in blocking keys, got {primary_fields}"
        assert "facility_name" in primary_fields

    def test_no_geo_columns_stays_name_only(self):
        """Without geo columns, blocking should remain name-only."""
        from goldenpipe.adapters.match import _build_config_from_contexts
        from goldenpipe.models.column_context import ColumnType

        contexts = [
            MagicMock(name="last_name", inferred_type=ColumnType.NAME, is_identifier=True),
            MagicMock(name="email", inferred_type=ColumnType.EMAIL, is_identifier=True),
        ]
        contexts[0].name = "last_name"
        contexts[1].name = "email"

        import polars as pl
        df = pl.DataFrame({"last_name": ["Smith", "Jones", "Doe"], "email": ["a@b", "c@d", "e@f"]})

        config = _build_config_from_contexts(contexts, df)
        assert config is not None
        # Should use last_name soundex, no geo compound
        primary_fields = config.blocking.keys[0].fields
        assert primary_fields == ["last_name"]
