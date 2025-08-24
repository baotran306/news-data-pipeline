
MERGE INTO table_target as t
USING source_table as s
ON t.id = s.id
WHEN MATCHED THEN
    UPDATE SET
        authors = s.authors,
        publish_time = s.publish_time,
        summary = s.summary,
        source_country = s.source_country,
        source_country_name = s.source_country_name,
        updated = CURRENT_TIMESTAMP(),
        title = s.title,
        url = s.url
WHEN NOT MATCHED THEN
    INSERT(id, title, summary, authors, url, source_country, source_country_name, authors, updated)
    VALUES(s.id, s.title, s.summary, s.authors, s.url, s.source_country, s.source_country_name, s.authors, CURRENT_TIMESTAMP())
