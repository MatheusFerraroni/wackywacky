INSERT IGNORE INTO cache_url (url_md5, url)
SELECT url_md5, url
FROM pages;
