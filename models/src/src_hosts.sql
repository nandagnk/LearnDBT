{{
    config(
        materialized='ephemeral'
    )
}}
WITH raw_hosts AS (
SELECT
*
FROM {{ ref('raw_hosts') }}
)
SELECT
id AS host_id,
NAME AS host_name,
is_superhost,
created_at,
updated_at
FROM
raw_hosts