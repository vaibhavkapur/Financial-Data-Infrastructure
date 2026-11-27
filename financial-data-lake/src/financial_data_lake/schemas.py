"""Arrow schema definitions, schema registry, and backward-compatible evolution rules."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pyarrow as pa
from pydantic import BaseModel, ConfigDict, Field

from financial_data_lake.errors import SchemaEvolutionError, SchemaValidationError

ARROW_TYPE_MAP: dict[str, pa.DataType] = {
    "string": pa.string(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "float64": pa.float64(),
    "bool": pa.bool_(),
    "timestamp_us_utc": pa.timestamp("us", tz="UTC"),
}

ALLOWED_WIDENINGS: set[tuple[str, str]] = {
    ("int32", "int64"),
    ("int32", "float64"),
    ("int64", "float64"),
}


class FieldSpec(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str = Field(min_length=1)
    type: str
    nullable: bool = True
    default: Any = None


class SchemaVersionSpec(BaseModel):
    model_config = ConfigDict(frozen=True)

    version: int = Field(ge=1)
    fields: list[FieldSpec]

    def to_arrow_schema(self) -> pa.Schema:
        arrow_fields: list[pa.Field] = []
        for f in self.fields:
            if f.type not in ARROW_TYPE_MAP:
                raise SchemaValidationError(
                    f"Unsupported Arrow field type: {f.type}",
                    context={"field": f.name, "type": f.type},
                )
            arrow_fields.append(pa.field(f.name, ARROW_TYPE_MAP[f.type], nullable=f.nullable))
        return pa.schema(arrow_fields)

    def field_map(self) -> dict[str, FieldSpec]:
        return {f.name: f for f in self.fields}


class DatasetSchemaConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    dataset_id: str
    current_version: int = Field(ge=1)
    primary_key: list[str]
    dedupe_key: list[str]
    partition_columns: list[str]
    versions: dict[int, SchemaVersionSpec]


class SchemaRegistry:
    """Manages versioned dataset schemas and validates backward-compatible evolution."""

    def __init__(self, datasets: dict[str, DatasetSchemaConfig]) -> None:
        self._datasets: dict[str, DatasetSchemaConfig] = dict(datasets)

    @classmethod
    def load(cls, path: Path | str) -> SchemaRegistry:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        parsed: dict[str, DatasetSchemaConfig] = {}
        for dataset_id, entry in raw["datasets"].items():
            versions: dict[int, SchemaVersionSpec] = {}
            for ver_str, ver_payload in entry["versions"].items():
                ver_int = int(ver_str)
                versions[ver_int] = SchemaVersionSpec(
                    version=ver_int,
                    fields=[FieldSpec.model_validate(f) for f in ver_payload["fields"]],
                )
            parsed[dataset_id] = DatasetSchemaConfig(
                dataset_id=dataset_id,
                current_version=int(entry["current_version"]),
                primary_key=list(entry["primary_key"]),
                dedupe_key=list(entry["dedupe_key"]),
                partition_columns=list(entry["partition_columns"]),
                versions=versions,
            )
        return cls(parsed)

    def get_dataset_config(self, dataset_id: str) -> DatasetSchemaConfig:
        if dataset_id not in self._datasets:
            raise SchemaValidationError(
                f"Dataset '{dataset_id}' is not registered in schema registry.",
                context={"dataset_id": dataset_id},
            )
        return self._datasets[dataset_id]

    def get_schema(self, dataset_id: str, version: int | None = None) -> SchemaVersionSpec:
        ds = self.get_dataset_config(dataset_id)
        target_version = ds.current_version if version is None else version
        if target_version not in ds.versions:
            raise SchemaValidationError(
                f"Schema version {target_version} not found for dataset '{dataset_id}'.",
                context={"dataset_id": dataset_id, "version": target_version},
            )
        return ds.versions[target_version]

    def get_arrow_schema(self, dataset_id: str, version: int | None = None) -> pa.Schema:
        return self.get_schema(dataset_id, version=version).to_arrow_schema()

    def validate_evolution(
        self,
        dataset_id: str,
        proposed_fields: list[FieldSpec],
    ) -> SchemaVersionSpec:
        """Validate that proposed_fields is backward-compatible with the current schema.

        Rules enforced:
        - Existing fields cannot be removed.
        - Existing nullable fields cannot be tightened to non-nullable.
        - Existing field types cannot change except via safe numeric widening (int32->int64/float64, int64->float64).
        - New fields MUST be nullable or specify a non-null default so older partitions remain readable.
        """
        ds = self.get_dataset_config(dataset_id)
        current = ds.versions[ds.current_version]
        current_map = current.field_map()
        proposed_map = {f.name: f for f in proposed_fields}

        if len(proposed_map) != len(proposed_fields):
            raise SchemaEvolutionError(
                "Duplicate field names in proposed schema.",
                context={"dataset_id": dataset_id},
            )

        # 1. Check existing fields preserved & compatible
        for name, old_field in current_map.items():
            if name not in proposed_map:
                raise SchemaEvolutionError(
                    f"Breaking schema change: cannot drop existing field '{name}'.",
                    context={"dataset_id": dataset_id, "dropped_field": name},
                )
            new_field = proposed_map[name]
            if new_field.type not in ARROW_TYPE_MAP:
                raise SchemaEvolutionError(
                    f"Unsupported field type '{new_field.type}' for '{name}'.",
                    context={"dataset_id": dataset_id, "field": name, "type": new_field.type},
                )
            if old_field.type != new_field.type:
                if (old_field.type, new_field.type) not in ALLOWED_WIDENINGS:
                    raise SchemaEvolutionError(
                        f"Breaking schema change: incompatible type change for '{name}' "
                        f"({old_field.type} -> {new_field.type}).",
                        context={
                            "dataset_id": dataset_id,
                            "field": name,
                            "old_type": old_field.type,
                            "new_type": new_field.type,
                        },
                    )
            if old_field.nullable and not new_field.nullable:
                raise SchemaEvolutionError(
                    f"Breaking schema change: cannot tighten nullable field '{name}' to non-nullable.",
                    context={"dataset_id": dataset_id, "field": name},
                )

        # 2. Check newly added fields
        for name, new_field in proposed_map.items():
            if name in current_map:
                continue
            if new_field.type not in ARROW_TYPE_MAP:
                raise SchemaEvolutionError(
                    f"Unsupported field type '{new_field.type}' for new field '{name}'.",
                    context={"dataset_id": dataset_id, "field": name, "type": new_field.type},
                )
            if not new_field.nullable and new_field.default is None:
                raise SchemaEvolutionError(
                    f"Breaking schema change: new non-nullable field '{name}' must provide a default.",
                    context={"dataset_id": dataset_id, "field": name},
                )

        return SchemaVersionSpec(version=ds.current_version + 1, fields=proposed_fields)

    def register_evolution(
        self,
        dataset_id: str,
        proposed_fields: list[FieldSpec],
    ) -> SchemaVersionSpec:
        """Validate and register a new compatible schema version."""
        new_spec = self.validate_evolution(dataset_id, proposed_fields)
        ds = self.get_dataset_config(dataset_id)
        updated_versions = dict(ds.versions)
        updated_versions[new_spec.version] = new_spec
        self._datasets[dataset_id] = DatasetSchemaConfig(
            dataset_id=ds.dataset_id,
            current_version=new_spec.version,
            primary_key=ds.primary_key,
            dedupe_key=ds.dedupe_key,
            partition_columns=ds.partition_columns,
            versions=updated_versions,
        )
        return new_spec

    def align_table_to_schema(
        self,
        table: pa.Table,
        dataset_id: str,
        target_version: int | None = None,
    ) -> pa.Table:
        """Project/widen an Arrow table from an older or matching schema version to target_version."""
        spec = self.get_schema(dataset_id, version=target_version)
        target_schema = spec.to_arrow_schema()
        arrays: list[pa.Array] = []
        n_rows = table.num_rows
        for field_spec in spec.fields:
            arrow_type = ARROW_TYPE_MAP[field_spec.type]
            if field_spec.name in table.column_names:
                col = table[field_spec.name]
                if col.type != arrow_type:
                    col = col.cast(arrow_type)
                arrays.append(col)
            else:
                if not field_spec.nullable and field_spec.default is None:
                    raise SchemaValidationError(
                        f"Missing required column '{field_spec.name}' with no default.",
                        context={"dataset_id": dataset_id, "column": field_spec.name},
                    )
                fill_values = [field_spec.default] * n_rows
                arrays.append(pa.array(fill_values, type=arrow_type))
        return pa.Table.from_arrays(arrays, schema=target_schema)
