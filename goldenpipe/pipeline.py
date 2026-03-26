"""Pipeline orchestrator -- chains GoldenCheck, GoldenFlow, GoldenMatch."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl

from goldenpipe.decisions import severity_gate, pii_router, row_count_gate

# Lazy imports for graceful degradation
try:
    import goldencheck
    HAS_CHECK = True
except ImportError:
    HAS_CHECK = False

try:
    import goldenflow
    HAS_FLOW = True
except ImportError:
    HAS_FLOW = False

try:
    import goldenmatch
    HAS_MATCH = True
except ImportError:
    HAS_MATCH = False


@dataclass
class PipeResult:
    """Result of a full pipeline run."""
    status: str = "success"
    source: str = ""
    input_rows: int = 0
    check: object = None
    transform: object = None
    match: object = None
    skipped: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    reasoning: dict[str, str] = field(default_factory=dict)
    timing: dict[str, float] = field(default_factory=dict)

    def __repr__(self) -> str:
        stages = []
        if self.check is not None:
            stages.append("check")
        if self.transform is not None:
            stages.append("flow")
        if self.match is not None:
            stages.append("match")
        return (
            f"PipeResult(status={self.status!r}, source={self.source!r}, "
            f"input_rows={self.input_rows}, stages={stages}, "
            f"skipped={self.skipped})"
        )


class Pipeline:
    """Orchestrates GoldenCheck -> GoldenFlow -> GoldenMatch."""

    def __init__(self, source: str, strategy: str | None = None):
        self.source = source
        self.strategy_override = strategy
        self._df: pl.DataFrame | None = None
        self._check_result = None
        self._transform_result = None
        self._match_result = None
        self._skipped: list[str] = []
        self._errors: list[str] = []
        self._reasoning: dict[str, str] = {}
        self._timing: dict[str, float] = {}

    def _load_data(self) -> pl.DataFrame | None:
        """Load source file into DataFrame."""
        if self._df is not None:
            return self._df

        path = Path(self.source)
        if not path.exists():
            return None

        suffix = path.suffix.lower()
        if suffix == ".parquet":
            self._df = pl.read_parquet(path)
        elif suffix in (".xlsx", ".xls"):
            self._df = pl.read_excel(path, engine="openpyxl")
        else:
            self._df = pl.read_csv(path, ignore_errors=True, encoding="utf8-lossy")
        return self._df

    def check(self) -> None:
        """Run GoldenCheck on the source file."""
        if not HAS_CHECK:
            self._skipped.append("check: goldencheck not installed")
            self._reasoning["check"] = "Skipped: pip install goldencheck"
            return

        if not Path(self.source).exists():
            self._errors.append(f"File not found: {self.source}")
            self._reasoning["check"] = f"File not found: {self.source}"
            return

        t0 = time.time()
        try:
            self._check_result = goldencheck.scan_file(self.source)
            n_findings = len(getattr(self._check_result, "findings", []) or [])
            self._reasoning["check"] = f"Found {n_findings} quality issues"
        except Exception as e:
            self._errors.append(f"GoldenCheck: {e}")
            self._reasoning["check"] = f"Failed: {e}"
        self._timing["check"] = time.time() - t0

    def flow(self) -> None:
        """Run GoldenFlow to fix quality issues."""
        if not HAS_FLOW:
            self._skipped.append("flow: goldenflow not installed")
            self._reasoning["flow"] = "Skipped: pip install goldenflow"
            return

        # Check for critical findings that should abort
        from goldenpipe.models.context import PipeContext
        ctx = PipeContext()
        findings = getattr(self._check_result, "findings", []) or []
        ctx.artifacts["findings"] = [
            {"severity": getattr(f, "severity", "info"), "check": getattr(f, "check", "")}
            if not isinstance(f, dict) else f
            for f in findings
        ]
        gate = severity_gate(ctx)
        if gate and gate.abort:
            self._errors.append(f"Pipeline aborted: {gate.reason}")
            self._reasoning["flow"] = f"Aborted: {gate.reason}"
            return

        if not findings:
            self._skipped.append("flow")
            self._reasoning["flow"] = "No quality issues found"
            return

        t0 = time.time()
        try:
            df = self._load_data()
            self._transform_result = goldenflow.transform_df(df)
            if hasattr(self._transform_result, "df") and self._transform_result.df is not None:
                self._df = self._transform_result.df
            self._reasoning["flow"] = self._flow_decision.reason
        except Exception as e:
            self._errors.append(f"GoldenFlow: {e}")
            self._reasoning["flow"] = f"Failed: {e}"
        self._timing["flow"] = time.time() - t0

    def match(self) -> None:
        """Run GoldenMatch to deduplicate."""
        if not HAS_MATCH:
            self._skipped.append("match: goldenmatch not installed")
            self._reasoning["match"] = "Skipped: pip install goldenmatch"
            return

        df = self._load_data()

        # Use row_count_gate and pii_router for routing decisions
        from goldenpipe.models.context import PipeContext
        ctx = PipeContext(metadata={"input_rows": df.height if df is not None else 0})
        findings = getattr(self._check_result, "findings", []) or []
        ctx.artifacts["findings"] = [
            {"severity": getattr(f, "severity", "info"), "check": getattr(f, "check", "")}
            if not isinstance(f, dict) else f
            for f in findings
        ]

        gate = row_count_gate(ctx)
        if gate and "goldenmatch.dedupe" in gate.skip:
            self._skipped.append("match")
            self._reasoning["match"] = gate.reason
            return

        pii_decision = pii_router(ctx)
        use_pprl = pii_decision is not None or self.strategy_override == "pprl"
        strategy = self.strategy_override or ("pprl" if use_pprl else "auto")

        t0 = time.time()
        try:
            if strategy == "pprl":
                self._reasoning["match"] = pii_decision.reason if pii_decision else "PPRL strategy"
                self._match_result = goldenmatch.dedupe_df(df)
            elif strategy == "auto":
                session = goldenmatch.AgentSession()
                result = session.deduplicate(self.source)
                self._match_result = result.get("results") if isinstance(result, dict) else result
                self._reasoning["match"] = (
                    result.get("reasoning", {}).get("why", "Auto-detect strategy")
                    if isinstance(result, dict) else "Auto-detect strategy"
                )
            else:
                self._match_result = goldenmatch.dedupe_df(df)
                self._reasoning["match"] = f"Strategy: {strategy}"
        except Exception as e:
            self._errors.append(f"GoldenMatch: {e}")
            self._reasoning["match"] = f"Failed: {e}"
        self._timing["match"] = time.time() - t0

    @property
    def result(self) -> PipeResult:
        """Build and return the PipeResult."""
        df = self._load_data() if self._df is None else self._df

        if self._errors and not any([self._check_result, self._transform_result, self._match_result]):
            status = "failed"
        elif self._errors:
            status = "partial"
        else:
            status = "success"

        return PipeResult(
            status=status,
            source=self.source,
            input_rows=df.height if df is not None else 0,
            check=self._check_result,
            transform=self._transform_result,
            match=self._match_result,
            skipped=self._skipped,
            errors=self._errors,
            reasoning=self._reasoning,
            timing=self._timing,
        )


def run(source: str, strategy: str | None = None) -> PipeResult:
    """Run the full Golden Suite pipeline on a file.

    Args:
        source: Path to CSV, Excel, or Parquet file.
        strategy: Force matching strategy ("pprl", "auto", etc). None = auto-detect.

    Returns:
        PipeResult with check, transform, match results + reasoning.
    """
    pipe = Pipeline(source, strategy=strategy)
    try:
        pipe.check()
        pipe.flow()
        pipe.match()
    except FileNotFoundError as e:
        pipe._errors.append(f"File not found: {source}")
    except Exception as e:
        pipe._errors.append(f"Pipeline error: {e}")
    return pipe.result
