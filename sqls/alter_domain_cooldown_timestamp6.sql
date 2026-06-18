ALTER TABLE domain
    MODIFY COLUMN last_request_at TIMESTAMP(6) NULL DEFAULT NULL,
    MODIFY COLUMN updated_at TIMESTAMP(6) NOT NULL
        DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6);

SHOW COLUMNS FROM domain WHERE Field IN ('last_request_at', 'updated_at');

SELECT id, `key`, value, description
FROM settings
WHERE `key` = 'domain_request_interval_ms';
