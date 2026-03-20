SELECT status, COUNT(1) AS total
FROM pages
GROUP BY status;
