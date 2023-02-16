

select data_source , year, count(distinct uth_member_id) as distinct_member, sum(total_enrolled_months) as MYear
from data_warehouse.member_enrollment_yearly 
group by data_source , year 
order by data_source , year 
;



select year, gender_cd, count(distinct uth_member_id) as distinct_member, sum(total_enrolled_months) as MY
from data_warehouse.member_enrollment_yearly 
where data_source = 'truv'
group by year, gender_cd 
order by year, gender_cd 
;


select  year, case  when a.age_derived between 0  and 19 then 'Age 0 to 19' 
	         when a.age_derived between 20 and 34 then 'Age 20 to 34'
	         when a.age_derived between 35 and 44 then 'Age 35 to 44'
	         when a.age_derived between 45 and 54 then 'Age 45 to 54'
	         when a.age_derived between 55 and 64 then 'Age 55 to 64'
	         when a.age_derived between 65 and 74 then 'Age 65 to 74'
	         else 'Age 75+'
	   end as age_group, 
	   count(distinct uth_member_id) as distinct_member, sum(total_enrolled_months) as MY
from data_warehouse.member_enrollment_yearly a
where data_source = 'optz'
group by case  when a.age_derived between 0  and 19 then 'Age 0 to 19' 
	         when a.age_derived between 20 and 34 then 'Age 20 to 34'
	         when a.age_derived between 35 and 44 then 'Age 35 to 44'
	         when a.age_derived between 45 and 54 then 'Age 45 to 54'
	         when a.age_derived between 55 and 64 then 'Age 55 to 64'
	         when a.age_derived between 65 and 74 then 'Age 65 to 74'
	         else 'Age 75+'
	   end , year 
order by year, case  when a.age_derived between 0  and 19 then 'Age 0 to 19' 
	         when a.age_derived between 20 and 34 then 'Age 20 to 34'
	         when a.age_derived between 35 and 44 then 'Age 35 to 44'
	         when a.age_derived between 45 and 54 then 'Age 45 to 54'
	         when a.age_derived between 55 and 64 then 'Age 55 to 64'
	         when a.age_derived between 65 and 74 then 'Age 65 to 74'
	         else 'Age 75+'
	   end
;



select year, a.bus_cd ,   count(distinct uth_member_id) as distinct_member, sum(total_enrolled_months) as MY
from data_warehouse.member_enrollment_yearly a
where data_source = 'truv'
group by year, bus_cd 
order by year, bus_cd 


select data_source, year, 
       count(uth_claim_id ) as distinct_claim,
       sum(total_allowed_amount) as total_allowed, 
       avg(total_allowed_amount) as average_allowed,
       claim_type 
from data_warehouse.claim_header c 
group by data_source , year , claim_type 
order by data_source , year , claim_type ;



select distinct claim_Type, data_source from data_warehouse.claim_header


update data_warehouse.claim_header 
set claim_type = 'P' 
where data_source in ('mdcr','mcrn') and table_id_src in ('dme_claims_k','bcarrier_claims_k')

update data_warehouse.claim_header 
set claim_type = 'F' 
where data_source in ('mdcr','mcrn') and table_id_src not in ('dme_claims_k','bcarrier_claims_k')



alter table data_warehouse.claim_header drop column place_of_service;


