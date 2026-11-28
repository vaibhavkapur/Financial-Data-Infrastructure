"""Options Market Data Platform (OMDP).

An educational/research prototype for ingesting versioned option chains,
validating quotes, deriving implied-volatility features with full input-version
provenance, and serving/replaying historical snapshots.
"""

__version__ = "0.1.0"
SCHEMA_VERSION = "1.0.0"

__all__ = ["__version__", "SCHEMA_VERSION"]
