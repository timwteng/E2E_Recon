"""Typed, validated configuration for the end-to-end recon."""

import re
from dataclasses import dataclass, field
from decimal import Decimal
from importlib import resources
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml

STANDARD_FIELDS = ("txn_id", "business_date", "direction", "txn_type", "currency", "amount")
DIRECTIONS = ("IN", "OUT")
_SUPPORTED_FORMATS = ("csv", "json")
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class SystemSpec:
    """How one system's extract is laid out and how to map it to the standard schema."""

    name: str
    stage: int
    file_format: str
    date_format: str
    columns: dict[str, str]
    amount_scale: int = 1
    value_maps: dict[str, dict[str, str]] = field(default_factory=dict)
    description: str = ""

    def __post_init__(self) -> None:
        if not _IDENTIFIER.match(self.name):
            raise ValueError(f"System name '{self.name}' must be a valid identifier")
        missing = set(STANDARD_FIELDS) - set(self.columns)
        if missing:
            raise ValueError(f"System '{self.name}' is missing column mappings: {sorted(missing)}")
        if self.file_format not in _SUPPORTED_FORMATS:
            raise ValueError(f"System '{self.name}': file_format must be one of {_SUPPORTED_FORMATS}")
        if self.amount_scale <= 0:
            raise ValueError(f"System '{self.name}': amount_scale must be positive")
        unknown_maps = set(self.value_maps) - {"direction", "txn_type"}
        if unknown_maps:
            raise ValueError(f"System '{self.name}': value_maps only supports direction/txn_type, got {sorted(unknown_maps)}")

    def raw_column(self, standard_field: str) -> str:
        return self.columns[standard_field]


@dataclass(frozen=True)
class ReconRules:
    control_keys: tuple[str, ...]
    amount_tolerance: Decimal
    record_key: str = "txn_id"

    def __post_init__(self) -> None:
        invalid = set(self.control_keys) - (set(STANDARD_FIELDS) - {"amount", "txn_id"})
        if invalid or "business_date" not in self.control_keys:
            raise ValueError(f"control_keys must include business_date and only use {STANDARD_FIELDS[1:5]}")
        if self.amount_tolerance < 0:
            raise ValueError("amount_tolerance must be >= 0")


@dataclass(frozen=True)
class BusinessCalendar:
    """How a scheduled run picks its business date: `lag_days` before the run's local date."""

    timezone: str = "Asia/Singapore"
    lag_days: int = 1

    def __post_init__(self) -> None:
        ZoneInfo(self.timezone)  # raises for an unknown zone
        if self.lag_days < 0:
            raise ValueError("lag_days must be >= 0")


@dataclass(frozen=True)
class HopPair:
    """Two systems whose totals must agree. ADJACENT pairs localise breaks; END_TO_END proves the whole flow."""

    source: SystemSpec
    target: SystemSpec
    scope: str

    @property
    def label(self) -> str:
        return f"{self.source.name}->{self.target.name}"


@dataclass(frozen=True)
class ReconConfig:
    rules: ReconRules
    systems: tuple[SystemSpec, ...]
    calendar: BusinessCalendar = BusinessCalendar()

    def __post_init__(self) -> None:
        if len(self.systems) < 2:
            raise ValueError("At least two systems are required")
        stages = [s.stage for s in self.systems]
        names = [s.name for s in self.systems]
        if len(set(stages)) != len(stages) or len(set(names)) != len(names):
            raise ValueError("System names and stage numbers must be unique")
        if stages != sorted(stages):
            object.__setattr__(self, "systems", tuple(sorted(self.systems, key=lambda s: s.stage)))

    @property
    def hop_pairs(self) -> tuple[HopPair, ...]:
        adjacent = [HopPair(a, b, "ADJACENT") for a, b in zip(self.systems, self.systems[1:])]
        if len(self.systems) > 2:
            adjacent.append(HopPair(self.systems[0], self.systems[-1], "END_TO_END"))
        return tuple(adjacent)

    @property
    def adjacent_pairs(self) -> tuple[HopPair, ...]:
        return tuple(p for p in self.hop_pairs if p.scope == "ADJACENT")

    @classmethod
    def load(cls, path: str | Path | None = None) -> "ReconConfig":
        """Load from a YAML file, or the packaged default when no path is given."""
        if path is None:
            text = resources.files("e2e_recon").joinpath("conf/systems.yml").read_text(encoding="utf-8")
        else:
            text = Path(path).read_text(encoding="utf-8")
        return cls.from_dict(yaml.safe_load(text))

    @classmethod
    def from_dict(cls, raw: dict) -> "ReconConfig":
        recon = raw["recon"]
        rules = ReconRules(
            control_keys=tuple(recon["control_keys"]),
            amount_tolerance=Decimal(str(recon.get("amount_tolerance", "0"))),
        )
        systems = tuple(
            SystemSpec(
                name=s["name"],
                stage=int(s["stage"]),
                file_format=s["file_format"],
                date_format=s["date_format"],
                columns=dict(s["columns"]),
                amount_scale=int(s.get("amount_scale", 1)),
                value_maps={k: {str(rk): str(rv) for rk, rv in v.items()} for k, v in s.get("value_maps", {}).items()},
                description=s.get("description", ""),
            )
            for s in raw["systems"]
        )
        calendar = BusinessCalendar(**raw.get("calendar", {}))
        return cls(rules=rules, systems=systems, calendar=calendar)


@dataclass(frozen=True)
class TableNames:
    """Unity Catalog locations for every layer of the pipeline."""

    catalog: str
    schema: str

    def __post_init__(self) -> None:
        for part in (self.catalog, self.schema):
            if not _IDENTIFIER.match(part):
                raise ValueError(f"'{part}' is not a valid catalog/schema identifier")

    def _fq(self, table: str) -> str:
        return f"{self.catalog}.{self.schema}.{table}"

    @property
    def landing_root(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/landing"

    def bronze(self, system: str) -> str:
        return self._fq(f"bronze_{system}")

    @property
    def silver_txn(self) -> str:
        return self._fq("silver_txn")

    @property
    def silver_rejects(self) -> str:
        return self._fq("silver_rejects")

    @property
    def silver_extract_status(self) -> str:
        return self._fq("silver_extract_status")

    @property
    def gold_run_status(self) -> str:
        return self._fq("gold_run_status")

    @property
    def gold_control_totals(self) -> str:
        return self._fq("gold_control_totals")

    @property
    def gold_recon_results(self) -> str:
        return self._fq("gold_recon_results")

    @property
    def gold_break_details(self) -> str:
        return self._fq("gold_break_details")

    @property
    def synthetic_answer_key(self) -> str:
        return self._fq("synthetic_answer_key")
