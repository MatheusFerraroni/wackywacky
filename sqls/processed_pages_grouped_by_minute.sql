SELECT
    DATE_FORMAT(updated_at, '%Y-%m-%d %H:%i:00') AS minute_window,
    COUNT(*) AS total_done,
    COUNT(*) / 60 AS processed_per_second
FROM pages
WHERE status = 'done'
GROUP BY minute_window
ORDER BY minute_window;
