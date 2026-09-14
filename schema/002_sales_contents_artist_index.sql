-- 002_sales_contents_artist_index.sql: index for the search app's Getty lookups,
-- which filter sales_contents with art_authority_1 = ANY(...). Without it each
-- search reads the whole table.

CREATE INDEX sales_contents_art_authority_1_idx ON sales_contents (art_authority_1);
