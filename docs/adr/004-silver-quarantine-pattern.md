# ADR 4: Explicit Quarantine Routing for Invalid Records

## Status: Accepted

## Context

During Silver transformations, taxi records may violate business rules such as non-positive fares, non-positive trip distances, negative `extra` values, invalid pickup/drop-off ordering, or pickup timestamps outside the active pipeline window.

Simply removing these records would make it difficult to determine why they were excluded from the valid Silver dataset.

## Decision

The implementation uses an explicit quarantine pattern.

Business rules are evaluated sequentially using PySpark's `when().otherwise()` logic to assign a `rejection_reason`. Records with no rejection reason are written to `silver_trips`, while records that fail a business rule are written to `silver_quarantine_trips`.

The quarantine dataset therefore retains the rejected record together with the reason it was excluded from the valid dataset for the processed window.

## Consequences

* **Positive:** Invalid records are explicitly isolated instead of being silently discarded during transformation.

* **Positive:** Rejection reasons make it possible to inspect which business rule caused a record to be excluded.

* **Positive:** The approach provides a queryable record of rejected data for the current processed window.

* **Negative:** The `when().otherwise()` conditions are evaluated sequentially. If a record violates multiple rules, only the first matching rejection reason is retained.

* **Negative:** The quarantine table is overwritten with the active batch, so it should not be treated as a permanent historical audit log across pipeline runs.
