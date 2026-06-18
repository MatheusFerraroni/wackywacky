SET NAMES utf8mb4;

INSERT INTO `settings` (`key`, `value`, `description`) VALUES
  (
    'search_engine',
    JSON_ARRAY('StarterWikipedia'),
    'List of enabled search engines'
  );

INSERT INTO `settings` (`key`, `value`, `description`) VALUES
  (
    'init_terms',
    JSON_ARRAY(
'runescape'
),
    'Initial search terms'
  );
