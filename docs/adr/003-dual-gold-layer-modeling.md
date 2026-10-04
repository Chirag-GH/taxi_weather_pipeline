# ADR 3: Dual Gold Outputs for Analytical and BI Consumption

## Status: Accepted

## Context

The Gold layer serves two related purposes: providing a structured trip-level dataset for analytical use and providing a simplified dataset for Power BI consumption.

The trip-level `gold_facts` table retains the enriched trip data at one row per valid taxi trip. The denormalized `gold_obt_trips` table resolves coded attributes and geographic descriptions so that the BI layer can consume a largely single-table dataset without reproducing the same joins.

Maintaining both outputs introduces additional write and storage overhead, but keeps the BI-facing dataset simpler while retaining the structured Gold table.

## Decision

The pipeline materializes two Gold Delta tables:

* `gold_facts` — a trip-level fact table containing valid taxi trips enriched with geographic and hourly weather data.

* `gold_obt_trips` — a denormalized One Big Table that resolves descriptive attributes and geographic information for BI consumption.

The zone reference is joined separately for pickup and drop-off locations and aliased appropriately before the denormalized table is materialized.

## Consequences

* **Positive:** Power BI can use `gold_obt_trips` as a simplified reporting source without recreating the dimension-resolution joins in the BI layer.

* **Positive:** `gold_facts` remains available as a structured trip-level dataset for analytical queries and downstream use cases.

* **Positive:** Resolving coded attributes and pickup/drop-off geographic descriptions in Spark keeps the BI-facing dataset self-contained.

* **Negative:** Maintaining two Gold outputs increases write time and storage usage.

* **Negative:** The current pipeline uses overwrite-based materialization, so both Gold tables represent the active rolling-window dataset rather than an independently accumulated historical Gold store.
