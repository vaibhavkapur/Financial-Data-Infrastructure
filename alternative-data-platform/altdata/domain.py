"""Validated registry and normalized record contracts for schema v1."""
import json
import re
from datetime import datetime, timezone
from decimal import Decimal


class Problem(Exception):
    def __init__(self, code, detail):
        super().__init__(detail)
        self.code = code
        self.detail = detail


def require(condition, code, detail):
    if not condition:
        raise Problem(code, detail)


def pack(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False,
                      separators=(",", ":"))


def unpack(data):
    def unique(pairs):
        obj = {}
        for key, value in pairs:
            require(key not in obj, "invalid_json", "duplicate JSON key")
            obj[key] = value
        return obj

    def invalid(_):
        raise Problem("invalid_json", "non-finite JSON number")

    try:
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        value = json.loads(data, object_pairs_hook=unique, parse_constant=invalid)
        pack(value).encode("utf-8")
        return value
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise Problem("invalid_json", "expected finite UTF-8 JSON") from exc


def utc(value):
    require(isinstance(value, str) and re.fullmatch(
        r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
        r"(?:\.[0-9]{1,6})?(?:Z|[+-](?:[01][0-9]|2[0-3]):[0-5][0-9])", value),
        "invalid_time", "expected timestamp with explicit time zone")
    try:
        moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return moment.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
    except (ValueError, OverflowError) as exc:
        raise Problem("invalid_time", "invalid timestamp") from exc


def now():
    return utc(datetime.now(timezone.utc).isoformat())


def token(value):
    require(isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", value),
            "invalid_id", "expected stable identifier")
    return value


def field(obj, name):
    require(isinstance(obj, dict) and name in obj, "missing_field", name)
    return obj[name]


def positive(value):
    require(type(value) is int and 0 < value <= 2**31-1, "invalid_version", "expected positive integer")
    return value


def words(value):
    require(isinstance(value, str) and value.strip(), "invalid_text", "expected nonempty text")
    return value.strip()


def tokens(value):
    require(isinstance(value, list), "invalid_list", "expected list of identifiers")
    result = [token(item) for item in value]
    require(len(set(result)) == len(result), "invalid_list", "duplicate identifier")
    return result


def registry(doc):
    require(type(field(doc, "schema_version")) is int and doc["schema_version"] == 1,
            "unsupported_schema", "registry schema must be 1")
    positive(field(doc, "version"))
    sources = field(doc, "sources")
    require(isinstance(sources, dict), "invalid_registry", "expected source map")
    for key, spec in sources.items():
        token(key)
        require(field(spec, "connector") in ("macro", "news"), "unknown_connector", key)
        require(type(field(spec, "enabled")) is bool, "invalid_registry", "enabled must be boolean")
        token(field(spec, "license_id"))
        words(field(spec, "terms"))
        words(field(spec, "provenance"))
        tokens(field(spec, "export_purposes"))
        utc(field(spec, "valid_until"))
        aliases = field(spec, "entities")
        require(isinstance(aliases, dict), "invalid_registry", "expected alias map")
        for alias, candidates in aliases.items():
            require(words(alias).casefold() == alias, "invalid_registry", "use normalized aliases")
            require(tokens(candidates), "invalid_registry", "empty candidate list")
        if spec["connector"] == "macro":
            units = field(spec, "units")
            require(isinstance(units, dict) and units, "invalid_registry", "expected series unit map")
            for series, unit in units.items():
                token(series)
                token(unit)
    return doc


def authorize(spec, stamp, purpose=None):
    require(spec is not None and spec["enabled"] and stamp < utc(spec["valid_until"]),
            "entitlement_denied", "source removed, disabled or expired")
    if purpose is not None:
        require(purpose in spec["export_purposes"], "export_denied", "purpose outside source license")


def normalized(kind, row, spec):
    alias = words(field(row, "entity")).casefold()
    candidates = spec["entities"].get(alias, [])
    require(candidates, "unknown_entity", alias)
    require(len(candidates) == 1, "ambiguous_entity", alias)
    published = utc(field(row, "published_at"))
    result = dict(entity_id=candidates[0], event_at=utc(field(row, "event_at")),
                  published_at=published, revision=positive(field(row, "revision")),
                  embargo_until=utc(row["embargo_until"]) if "embargo_until" in row else published)
    if kind == "macro":
        series = token(field(row, "series"))
        unit = field(row, "unit")
        require(series in spec["units"] and spec["units"][series] == unit, "invalid_unit", series)
        value = field(row, "value")
        reason = row.get("missing_reason")
        if value is None:
            reason = words(reason)
        else:
            require(isinstance(value, str) and len(value) <= 128 and
                    re.fullmatch(r"-?[0-9]+(\.[0-9]+)?", value), "invalid_value", "expected decimal string")
            number = Decimal(value)
            value = format(number, "f") if number else "0"
            if "." in value:
                value = value.rstrip("0").rstrip(".")
            require(reason is None, "invalid_missingness", "value and missing reason both supplied")
        result.update(series=series, unit=unit, value=value, missing_reason=reason)
        key = pack([series, candidates[0], result["event_at"]])
    else:
        key = token(field(row, "article_id"))
        result.update(article_id=key, headline=words(field(row, "headline")))
    return key, result
