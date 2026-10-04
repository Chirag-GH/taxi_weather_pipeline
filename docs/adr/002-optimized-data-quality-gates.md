# ADR 2: Data Quality Gates and Reference Join Optimization

## Status: Accepted

## Context

The pipeline validates Silver datasets before Gold materialization. Several checks verify that configured reference fields, such as `VendorID`, `RatecodeID`, payment types, location IDs, and weather codes, map to the corresponding reference data.

For these validation queries, the required result is the set of records with no corresponding reference value. `LEFT ANTI JOIN` is therefore used for referential-integrity checks.

Separately, the Silver and Gold transformations join large trip datasets with small reference and dimension tables. These smaller tables are suitable candidates for broadcast joins.

## Decision

The implementation uses two separate techniques:

1. **Data quality validation:** `LEFT ANTI JOIN` is used in the SQL-based DQ checks to identify records whose configured reference values do not have a corresponding entry in the reference tables.

2. **Transformation join optimization:** Small reference and dimension tables are explicitly broadcast during selected Silver and Gold transformations using PySpark's `broadcast()` function.

The broadcast optimization is therefore part of the transformation logic, not the DQ SQL checks themselves.

## Consequences

* **Positive:** Anti-joins directly identify unmatched records without producing a full joined result for referential-integrity checks.

* **Positive:** Broadcasting sufficiently small reference tables can reduce unnecessary shuffling when they are joined with larger trip datasets.

* **Negative:** Broadcast joins depend on the referenced tables remaining small enough to distribute safely. If their size or usage changes significantly, the join strategy should be reconsidered.

* **Negative:** The DQ framework validates and rejects invalid data; it does not automatically repair the underlying records.
