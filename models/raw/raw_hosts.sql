WITH raw_hosts AS (
SELECT
*
FROM
AIRBNB.RAW.RAW_HOSTS
)
SELECT
id ,
NAME,
is_superhost,
created_at,
updated_at
FROM
raw_hosts