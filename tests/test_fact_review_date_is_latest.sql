select *
from {{ ref('fact_reviews') }} fr join 
     {{ ref('src_listings') }} l on (fr.listing_id = l.listing_id)
where fr.review_date < l.created_at