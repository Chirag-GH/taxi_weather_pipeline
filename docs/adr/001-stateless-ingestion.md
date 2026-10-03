# ADR 1: Stateless Ingestion and Rolling Window Management

## Status: Accepted

## Context
The pipeline maintains a 3-month rolling window of NYC TLC trip data. This retention period bounds the dataset size (approximately 10+ million records) to operate efficiently within constrained compute environments. The system requires a mechanism to calculate the next target month for ingestion and identify the oldest month for eviction. Standard patterns often rely on external state tracking, which introduces split-brain risks if the physical file system and the state tracker fall out of sync.

## Decision
The implementation uses stateless file-system polling as the single source of truth. Python's `glob` scans the physical `/Volumes/workspace/taxi_weather/datasets/` path at runtime to parse existing `.parquet` files, determines the temporal boundaries of the current data, and calculates the next required month for ingestion.

## Consequences
* **Positive:** The ingestion operation remains idempotent based on actual file presence rather than an external record, allowing the system to naturally recover from interrupted downloads or manual file deletions.
* **Positive:** Reduces architectural complexity by removing the requirement for a persistent state database or configuration file.
* **Negative:** Couples the ingestion logic tightly to the upstream provider's file naming convention (`yellow_tripdata_{yyyy}-{mm}.parquet`). Upstream changes to this format will break the polling mechanism.