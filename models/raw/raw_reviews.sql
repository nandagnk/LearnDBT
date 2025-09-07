WITH raw_reviews AS (
SELECT
*
FROM
AIRBNB.RAW.RAW_REVIEWS
)
SELECT
listing_id,
date,
reviewer_name,
comments,
sentiment
FROM
raw_reviews