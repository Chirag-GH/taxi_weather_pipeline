# ADR 2: Data Quality Gates via Anti-Joins and Broadcasts

## Status: Accepted

## Context
Before Silver data is promoted to the Gold layer, referential integrity must be enforced. Every `VendorID`, `RatecodeID`, and `LocationID` in the fact table must exist in the respective Silver dimension tables. Performing standard full joins for these checks triggers extensive data shuffles across the cluster, leading to high latency and memory overhead on unpartitioned fact tables.

## Decision
The pipeline implements data quality checks using `LEFT ANTI JOIN` operations combined with PySpark `broadcast()` hints on all dimensional data.

## Consequences
* **Positive:** The Catalyst Optimizer natively evaluates `LEFT ANTI JOIN` efficiently for identifying missing references, stopping evaluation upon finding a non-match.
* **Positive:** Broadcasting serializes dimension tables to worker memory, keeping the fact table stationary and preventing cross-cluster shuffles.
* **Negative:** Broadcasting dictates that right-side tables must remain small to avoid driver node memory exhaustion. This constraint is acceptable in this architecture because the dimension tables (e.g., TLC zones, weather codes) represent fixed data with no growth risk.