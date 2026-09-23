# OrderFlow Contracts

Versioned contracts for the OrderFlow event-driven platform.

## Contents

- `docs/event-catalog.md`: semantic catalog, topology, ownership, idempotency and failure rules.
- `schemas/common`: shared Kafka message envelope.
- `schemas/inventory`: inventory commands and events.
- `schemas/payment`: payment commands and events.
- `schemas/webhook`: external payment webhook body.
- `examples/valid`: one valid example for every contract.
- `examples/invalid`: representative examples that must fail validation.
- `scripts/validate_contracts.py`: validates schemas and examples.

## Contract rules

- JSON Schema Draft 2020-12.
- Kafka delivery semantics are at least once.
- `orderId` is both the Kafka record key and the saga `correlationId`.
- Each message receives a new `eventId`; `causationId` points to the triggering message or request.
- Root objects and payloads are closed to unknown fields.
- Additive optional fields require an expand-first rollout within the current logical version.
- Removing, renaming, changing the type, or changing the meaning of a field requires a new logical version.
- Internal monetary values are decimal strings and map to `BigDecimal` in Java.

## Validation

```bash
python3 -m pip install -r requirements-dev.txt
python3 scripts/validate_contracts.py
```

The validator checks schema syntax, confirms that all valid examples pass, and confirms that all invalid examples fail.

## Status

This is the Phase 0 `v1` proposal. The contracts are ready for ADR review and implementation planning, but they have not yet been published as a repository release.
