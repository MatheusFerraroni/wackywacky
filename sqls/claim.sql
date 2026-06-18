# total to process
SELECT
    count(1)
FROM pages p
INNER JOIN domain d
    ON d.id = p.domain_id
WHERE
    p.recursion_level < 4
    AND d.recursion_level < 4
    AND p.retry_count < 3
    AND (p.status = 'todo' or p.status = 'failed')


# count domains to get data
SELECT
    COUNT(DISTINCT(d.id))
FROM pages p
INNER JOIN domain d
    ON d.id = p.domain_id
WHERE
    p.recursion_level < 3
    AND d.recursion_level < 3
    AND p.retry_count < 3
    AND p.status = 'todo';


# count domains already limited to claim
SELECT
    COUNT(DISTINCT(d.id))
FROM pages p
INNER JOIN domain d
    ON d.id = p.domain_id
WHERE
    p.recursion_level < 4
    AND d.recursion_level < 4
    AND p.retry_count < 3
    AND p.status = 'blocked_limit_recursion';
