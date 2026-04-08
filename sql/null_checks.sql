SELECT
    SUM(CASE WHEN user_id IS NULL OR TRIM(user_id) = '' THEN 1 ELSE 0 END) AS missing_user_id,
    SUM(CASE WHEN bug_type IS NULL OR TRIM(bug_type) = '' THEN 1 ELSE 0 END) AS missing_bug_type,
    SUM(CASE WHEN severity IS NULL OR TRIM(severity) = '' THEN 1 ELSE 0 END) AS missing_severity
FROM read_csv_auto('data/raw/synthetic_logs_batch_001.csv');
