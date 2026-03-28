SELECT status, recursion_level, COUNT(1) AS total
FROM pages
GROUP BY status, recursion_level
limit 100;
