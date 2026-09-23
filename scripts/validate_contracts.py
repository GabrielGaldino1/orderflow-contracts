#!/usr/bin/env python3
"""Validate OrderFlow JSON Schemas and their executable examples."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource


ROOT = Path(__file__).resolve().parents[1]
SCHEMAS = ROOT / "schemas"
VALID = ROOT / "examples" / "valid"
INVALID = ROOT / "examples" / "invalid"


def load_json(path: Path) -> dict:
    with path.open(encoding="utf-8") as source:
        return json.load(source)


def semantic_errors(instance: dict, contract_name: str) -> list[str]:
    if contract_name == "payment-webhook.v1":
        return []

    payload = instance["payload"]
    errors: list[str] = []
    order_id = payload.get("orderId")

    if order_id and instance["correlationId"] != order_id:
        errors.append("correlationId must equal payload.orderId")

    reservation_events = {
        "inventory-reserved.v1",
        "inventory-release-requested.v1",
        "inventory-released.v1",
    }
    payment_results = {
        "payment-pending.v1",
        "payment-approved.v1",
        "payment-declined.v1",
    }

    if contract_name in reservation_events:
        if instance["aggregateId"] != payload["reservationId"]:
            errors.append("aggregateId must equal payload.reservationId")
    elif contract_name in payment_results:
        if instance["aggregateId"] != payload["paymentId"]:
            errors.append("aggregateId must equal payload.paymentId")
    elif order_id and instance["aggregateId"] != order_id:
        errors.append("aggregateId must equal payload.orderId")

    return errors


def main() -> int:
    schema_paths = sorted(SCHEMAS.rglob("*.schema.json"))
    schemas = {path: load_json(path) for path in schema_paths}

    registry = Registry().with_resources(
        (schema["$id"], Resource.from_contents(schema))
        for schema in schemas.values()
    )

    failures: list[str] = []

    for path, schema in schemas.items():
        try:
            Draft202012Validator.check_schema(schema)
        except Exception as exc:  # schema error details are needed in CLI output
            failures.append(f"Invalid schema {path.relative_to(ROOT)}: {exc}")

    contract_paths = [
        path for path in schema_paths if path.parent.name != "common"
    ]

    for schema_path in contract_paths:
        contract_name = schema_path.name.removesuffix(".schema.json")
        schema = schemas[schema_path]
        validator = Draft202012Validator(
            schema,
            registry=registry,
            format_checker=FormatChecker(),
        )

        valid_path = VALID / f"{contract_name}.json"
        invalid_path = INVALID / f"{contract_name}.json"

        if not valid_path.exists() or not invalid_path.exists():
            failures.append(f"Missing example pair for {contract_name}")
            continue

        valid_instance = load_json(valid_path)
        valid_errors = sorted(validator.iter_errors(valid_instance), key=str)
        valid_semantic_errors = semantic_errors(valid_instance, contract_name)

        if valid_errors:
            details = "; ".join(error.message for error in valid_errors)
            failures.append(f"Valid example rejected for {contract_name}: {details}")
        if valid_semantic_errors:
            failures.append(
                f"Semantic failure for {contract_name}: "
                + "; ".join(valid_semantic_errors)
            )

        invalid_instance = load_json(invalid_path)
        invalid_errors = list(validator.iter_errors(invalid_instance))
        if not invalid_errors:
            failures.append(f"Invalid example accepted for {contract_name}")

    if failures:
        print("Contract validation failed:")
        for failure in failures:
            print(f"- {failure}")
        return 1

    print(
        f"Validated {len(schema_paths)} schemas, "
        f"{len(contract_paths)} valid examples and "
        f"{len(contract_paths)} invalid examples."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
