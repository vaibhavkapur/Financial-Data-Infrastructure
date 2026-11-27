"""Financial Data Lake — Versioned, point-in-time market data platform."""

from financial_data_lake.config import LakeConfig
from financial_data_lake.errors import (
    CorruptPartitionError,
    DataLakeError,
    EntitlementError,
    FixtureVerificationError,
    ImmutabilityViolationError,
    QualityCheckError,
    RecoveryError,
    SchemaEvolutionError,
    SchemaValidationError,
    SnapshotError,
)
from financial_data_lake.lake import FinancialDataLake
from financial_data_lake.models import (
    AccessContext,
    EntitlementPolicy,
    PartitionManifest,
    SnapshotManifest,
)
from financial_data_lake.schemas import FieldSpec, SchemaRegistry

__version__ = "0.1.0"

__all__ = [
    "AccessContext",
    "CorruptPartitionError",
    "DataLakeError",
    "EntitlementError",
    "EntitlementPolicy",
    "FieldSpec",
    "FinancialDataLake",
    "FixtureVerificationError",
    "ImmutabilityViolationError",
    "LakeConfig",
    "PartitionManifest",
    "QualityCheckError",
    "RecoveryError",
    "SchemaEvolutionError",
    "SchemaRegistry",
    "SchemaValidationError",
    "SnapshotError",
    "SnapshotManifest",
    "__version__",
]
