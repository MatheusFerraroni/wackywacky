-- Refresh stale tables
SELECT CONCAT(
    'ANALYZE TABLE ',
    GROUP_CONCAT(CONCAT('`', table_schema, '`.`', table_name, '`') SEPARATOR ', '),
    ';'
) AS analyze_sql
FROM information_schema.TABLES
WHERE table_schema = 'appdb';

SELECT
    table_schema AS database_name,
    table_name,
    table_rows,
    ROUND(data_length / 1024 / 1024, 2) AS data_mb,
    ROUND(index_length / 1024 / 1024, 2) AS index_mb,
    ROUND((data_length + index_length) / 1024 / 1024, 2) AS total_size_mb
FROM information_schema.TABLES
WHERE table_schema = 'appdb'
ORDER BY total_size_mb DESC;
