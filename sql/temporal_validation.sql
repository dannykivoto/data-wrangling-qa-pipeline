SELECT
    COUNT(*) AS timestamp_violations
FROM read_csv_auto('data/raw/synthetic_logs_batch_002.csv')
WHERE CAST(action_timestamp AS TIMESTAMP) < CAST(account_created AS TIMESTAMP);

SELECT
    batch_id,
    COUNT(*) AS bad_rows
FROM read_csv_auto('data/raw/synthetic_logs_batch_002.csv')
WHERE CAST(action_timestamp AS TIMESTAMP) < CAST(account_created AS TIMESTAMP)
GROUP BY batch_id;
