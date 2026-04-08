-- Run with DuckDB:
-- duckdb -c ".read sql/surface_analysis.sql"

SELECT COUNT(*) AS total_rows
FROM read_csv_auto('data/raw/synthetic_logs_batch_001.csv');

SELECT
    MIN(action_timestamp) AS min_action_ts,
    MAX(action_timestamp) AS max_action_ts,
    COUNT(DISTINCT user_id) AS unique_users
FROM read_csv_auto('data/raw/synthetic_logs_batch_001.csv');

SELECT event_type, COUNT(*) AS n
FROM read_csv_auto('data/raw/synthetic_logs_batch_001.csv')
GROUP BY event_type
ORDER BY n DESC;
