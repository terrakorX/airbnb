-- KPI-1
select count(*) as nombre_proprietaire,  host_name from listings
where host_name is not null 
group by host_name 
order by nombre_proprietaire DESC



-- KPI 2
select
unnest(string_to_array(reviewers, ',')) as review_id
,count(*) as nombre_de_reviews
from reviews  as l
group by review_id 
order by nombre_de_reviews desc


-- KPI 3 
with all_host as (
select 
unnest(string_to_array(cohost_ids, ',')) AS all_host_id, 
unnest(string_to_array(cohost_names, ',')) AS host_name ,
listing_id
from listings
where cohost_ids is not null
union all 
select  
host_id as all_host_id,
host_name,
listing_id
from listings
where host_name is not null
)
select 
host_name as name ,
count (*) as nombre_annonce
from all_host 
group by all_host_id , host_name 
order by nombre_annonce desc


-- KPI 4
SELECT 
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ttm_avg_rate) AS median_price,
     Round(AVG(ttm_avg_rate)::numeric, 2) AS avg_price,
    split_part(
        split_part(source_file, 'Listings_Data_', 2), -- on enlève le préfixe
        '-france.csv',                              -- on enlève le suffixe
        1
    ) AS city
FROM listings
group by city
ORDER BY median_price DESC;

-- KPI 5

SELECT 
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ttm_avg_rate) AS median_price,
     Round(AVG(ttm_avg_rate)::numeric, 2) AS avg_price,
     CASE
    WHEN beds >= 1 AND beds < 3 THEN '1-3'
    WHEN beds >= 3 AND beds < 6 THEN '3-6'
    ELSE '6+'
  END AS available_beds,
    split_part(
        split_part(source_file, 'Listings_Data_', 2), -- on enlève le préfixe
        '-france.csv',                              -- on enlève le suffixe
        1
    ) AS city
FROM listings
group by city, available_beds
ORDER BY median_price DESC;

-- KPI 6
SELECT 
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ttm_avg_rate) AS median_price,
     Round(AVG(ttm_avg_rate)::numeric, 2) AS avg_price,
listing_type as room_type,
     split_part(
        split_part(source_file, 'Listings_Data_', 2), -- on enlève le préfixe
        '-france.csv',                              -- on enlève le suffixe
        1
    ) AS city
FROM listings
where listing_type is not null
group by city, listing_type
ORDER BY median_price DESC;

--KPI 7
select
 ROUND(SUM(pc.vacant_days)::NUMERIC / NULLIF(SUM(pc.vacant_days + pc.reserved_days), 0) * 100, 2) || '%' AS availability_days,
  split_part(
        split_part(l.source_file, 'Listings_Data_', 2), -- on enlève le préfixe
        '-france.csv',                              -- on enlève le suffixe
        1
    ) AS city
from listings l  
left join past_calendar as pc on pc.listing_id::numeric = l.listing_id::numeric 
group by  city

-- KPI 8
select r.listing_id, SUM(r.num_reviews) as reviews_nmb, l.listing_type as room_type
from listings l 
join reviews r on r.listing_id::numeric  = l.listing_id::numeric 
group by r.listing_id, l.listing_type 
order by  reviews_nmb desc
