SET NAMES utf8mb4;

CREATE TABLE `domain` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `url` VARCHAR(2083) NOT NULL,
  `url_md5` BINARY(16) NOT NULL,

  `parent_domain_id` INT UNSIGNED NULL,

  `recursion_level` TINYINT UNSIGNED NOT NULL DEFAULT 0,

  `request_count` BIGINT UNSIGNED NOT NULL DEFAULT 0,
  `last_request_at` TIMESTAMP NULL DEFAULT NULL,

  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

  UNIQUE KEY `uq_domain_url_md5` (`url_md5`),
  KEY `idx_domain_last_request_at` (`last_request_at`),
  KEY `idx_domain_parent_domain_id` (`parent_domain_id`),

  CONSTRAINT `fk_domain_parent`
    FOREIGN KEY (`parent_domain_id`)
    REFERENCES `domain` (`id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE
) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `pages` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `domain_id` INT UNSIGNED NOT NULL,
  `parent_page_id` INT UNSIGNED NULL,
  `same_as` INT UNSIGNED NULL,

  `url` TEXT NOT NULL,
  `url_md5` BINARY(16) NOT NULL,

  `url_final` TEXT NULL,
  `url_final_md5` BINARY(16) NULL,

  `status_code` SMALLINT UNSIGNED NULL,
  `title` VARCHAR(512) NULL,

  `recursion_level` TINYINT UNSIGNED NOT NULL DEFAULT 0,

  `status` ENUM(
    'todo',
    'processing',
    'done',
    'failed',
    'blocked_domain',
    'blocked_limit_recursion',
    'blocked_language'
  ) NOT NULL DEFAULT 'todo',

  `retry_count` SMALLINT UNSIGNED NOT NULL DEFAULT 0,

  `text` BLOB NULL,
  `html` BLOB NULL,
  `text_md5` BINARY(16) NULL,
  `html_md5` BINARY(16) NULL,

  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

  UNIQUE KEY `uq_pages_url_md5` (`url_md5`),
  UNIQUE KEY `uq_pages_text_md5` (`text_md5`),
  UNIQUE KEY `uq_pages_html_md5` (`html_md5`),

  KEY `idx_pages_domain_id` (`domain_id`),
  KEY `idx_pages_parent_page_id` (`parent_page_id`),
  KEY `idx_pages_same_as` (`same_as`),
  KEY `idx_pages_recursion_level` (`recursion_level`),

  KEY `idx_pages_status` (`status`),
  KEY `idx_pages_status_created_at` (`status`, `created_at`),
  KEY `idx_pages_status_updated_at` (`status`, `updated_at`),

  KEY `idx_pages_url_final_md5` (`url_final_md5`),

  CONSTRAINT `fk_pages_domain`
    FOREIGN KEY (`domain_id`) REFERENCES `domain` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT `fk_pages_parent`
    FOREIGN KEY (`parent_page_id`) REFERENCES `pages` (`id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE,

  CONSTRAINT `fk_pages_same_as`
    FOREIGN KEY (`same_as`) REFERENCES `pages` (`id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE

) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `settings` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,

  `key` VARCHAR(191) NOT NULL,
  `value` JSON NULL,
  `description` VARCHAR(1024) NULL,

  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

  UNIQUE KEY `uq_settings_key` (`key`)
) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `blocked_domain` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `domain` VARCHAR(253) NOT NULL,
  `domain_md5` BINARY(16) NOT NULL,

  `reason` VARCHAR(512) NULL,

  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

  UNIQUE KEY `uq_blocked_domains_domain` (`domain`),
  UNIQUE KEY `uq_blocked_domains_domain_md5` (`domain_md5`)
) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO `settings` (`key`, `value`, `description`) VALUES
  (
    'request_timeout_ms',
    JSON_EXTRACT('10000', '$'),
    'Timeout in milliseconds to page load; select body/2; networkidle/3;'
  ),
  (
    'max_retry_attempts',
    JSON_EXTRACT('3', '$'),
    'Number of retry attempts for failed requests'
  ),
  (
    'retry_interval_ms',
    JSON_EXTRACT('60000', '$'),
    'Interval between retries in milliseconds'
  ),
  (
    'domain_request_interval_ms',
    JSON_EXTRACT('5000', '$'),
    'Interval between request to the same domain in milliseconds'
  ),
  (
    'max_recursion',
    JSON_EXTRACT('3', '$'),
    'Max recursion increasing to new domains'
  ),
  (
    'max_recursion_page',
    JSON_EXTRACT('4', '$'),
    'Max recursion increasing to new pages, reset every new domain.'
  ),
  (
    'system_status',
    JSON_EXTRACT('"starting"', '$'),
    'Current system status'
  );