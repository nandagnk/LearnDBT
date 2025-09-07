{{
    config(
        materialized='view'
    )
}}
WITH src_listings AS (
SELECT
*
FROM
{{ ref('src_listings') }}
)
SELECT
listing_id,
listing_name,
room_type,
minimum_nights,
host_id,
price,
created_at,
updated_at
FROM
src_listings