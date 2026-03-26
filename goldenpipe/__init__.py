"""GoldenPipe -- Golden Suite orchestrator.

Usage:
    import goldenpipe as gp
    result = gp.run("customers.csv")
"""
__version__ = "0.1.0"

from goldenpipe.pipeline import run, Pipeline, PipeResult
from goldenpipe.decisions import severity_gate, pii_router, row_count_gate

__all__ = [
    "run", "Pipeline", "PipeResult",
    "severity_gate", "pii_router", "row_count_gate",
]
