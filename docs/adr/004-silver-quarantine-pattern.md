# ADR 4: Explicit Quarantine Routing for Invalid Records

## Status: Accepted

## Context
During Silver layer transformations, records occasionally contain logical impossibilities, such as negative fares, missing primary keys, or drop-off times preceding pick-up times. Dropping these records implicitly removes invalid data but destroys the audit trail, making it difficult to distinguish between upstream data delivery issues and actual invalid metrics generated at the source.

## Decision
The implementation uses an explicit quarantine pattern. The pipeline evaluates business rules sequentially using PySpark's `when().otherwise()` to tag records with a specific `rejection_reason`. Valid records route to `silver_trips`, while invalid records route to `silver_quarantine_trips`.

## Consequences
* **Positive:** Preserves a strict, queryable audit trail in the Silver layer, enabling tracing of data loss back to specific business rule violations.
* **Positive:** Replaces implicit `AND` logic in chained filters with an explicit `OR` routing mechanism, ensuring records failing any single check are appropriately segregated.
* **Negative:** PySpark's `when().otherwise()` evaluates sequentially (top-to-bottom). If a record violates multiple rules (e.g., both an invalid date and a negative fare), it is only tagged with the first matched rule, masking secondary failures for that specific row.