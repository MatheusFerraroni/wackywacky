SELECT status, recursion_level, COUNT(1) AS total
FROM pages
where retry_count < 3
and recursion_level < 4
GROUP BY status, recursion_level
limit 100;