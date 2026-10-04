# ADR 1: Filesystem-Driven Rolling Window Ingestion

## Status: Accepted

## Context

The pipeline maintains a three-month working window of NYC TLC Yellow Taxi trip data to bound compute and storage requirements.

The ingestion process needs to determine which month is currently available, which month should be downloaded next, and which month should be removed from the working window. Maintaining this state separately from the files themselves would introduce another source of truth that could become inconsistent with the actual data stored in the Databricks Volume.

## Decision

The implementation uses the files in `/Volumes/workspace/taxi_weather/datasets/` as the source of ingestion state.

Python's `glob` scans the Volume for files matching the expected TLC naming convention (`yellow_tripdata_YYYY-MM.parquet`). The pipeline uses the discovered months to determine the current temporal range, calculates the next target month, downloads it when available, and removes the oldest month to maintain the intended rolling window.

No separate state database or external ingestion-state tracker is used.

## Consequences

* **Positive:** Ingestion decisions are derived from the actual files present in storage rather than a separate state store.

* **Positive:** The approach keeps the ingestion mechanism simple and makes the current working window directly observable from the Volume contents.

* **Negative:** The implementation depends on the upstream TLC file naming convention (`yellow_tripdata_{yyyy}-{mm}.parquet`). Changes to that convention would require changes to the polling logic.

* **Negative:** File presence is used to determine the available months; the polling mechanism does not replace separate validation of file contents or completeness.
