# ADR 3: Dual Presentation Layer (Star Schema and OBT)

## Status: Accepted

## Context
The Gold layer's primary requirement is to serve data to Power BI dashboards. While a normalized schema is highly efficient for cloud storage, it forces BI engines to perform multi-table joins on the fly, which can degrade dashboard load performance. Conversely, a One Big Table (OBT) is structurally optimized for BI filtering speed but incurs storage redundancy.

## Decision
Materializes both a normalized Schema (`gold_facts`) and a fully denormalized One Big Table (`gold_obt_trips`).

## Consequences
* **Positive:** Power BI connects directly to `gold_obt_trips`, ensuring low-latency slicing and dicing without real-time join execution.
* **Positive:** Resolving self-referencing dimension conflicts (e.g., aliasing the Zone table into `pickup_zone` and `dropoff_zone`) occurs natively in Spark, removing ambiguity in the BI layer.
* **Positive:** The Star Schema (`gold_facts`) provides a compacted, columnar structure that could potentially support future ad-hoc analytical or machine learning workloads.
* **Negative:** Writing the data twice increases job runtime and doubles the Gold layer's storage footprint. This is mitigated by automated `OPTIMIZE` and `VACUUM` tasks that prune historical file versions.