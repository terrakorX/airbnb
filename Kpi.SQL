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