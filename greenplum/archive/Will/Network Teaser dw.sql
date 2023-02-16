--Member Counts Overall 
with cohort_cte as ( select data_source, uth_member_id, case when gender_cd = 'F' then 'AAF' else 'AAM' end as gender_cd, 
							case when age_derived between 0 and 19 then 'Age Group 1'
							    when age_derived between 20 and 34 then 'Age Group 2' 
								when age_derived between 35 and 44 then 'Age Group 3'
								when age_derived between 45 and 54 then 'Age Group 4'
								when age_derived between 55 and 64 then 'Age Group 5'
								when age_derived between 65 and 74 then 'Age Group 6'
								when age_derived >= 75 then 'Age Group 7' end as age_group, 
					      total_enrolled_months 
					      ,case when data_source in ('mdcd') then 'OTH' 
					            when data_source = 'optz' and plan_type in ('PPO','EPO','POS') then 'PPO' 
					            when data_source = 'optz' and plan_type in ('IND','OTH') then 'OTH' 
					            when data_source  = 'truv' and plan_type in ('CDHP','HDHP') then 'HDHP' 
					            when data_source = 'truv' and plan_type in ('PPO','EPO','POS') then 'PPO' 
					            when data_source = 'truv' and plan_type = 'HMO' then 'HMO'
					            when data_source = 'truv' and plan_type is null or plan_type in ('BMM','CMP') then 'OTH' 
					            else plan_type end as plan_type
                     from data_warehouse.member_enrollment_yearly 
                       where data_source in  ('mcrt','mdcd', 'optz','truv')
					    and "year" = 2019 
					    and state = 'TX'  
					 )
select * 
from 
( 
	select data_source, 'AAATotal' as measure, count(distinct uth_member_id), sum(total_enrolled_months) / 12 as my 
	from cohort_cte group by 1,2 
union 
	select data_source, gender_cd, count(distinct uth_member_id), sum(total_enrolled_months) / 12 as my 
	from cohort_cte group by 1,2 	
union 
	select data_source, age_group , count(distinct uth_member_id), sum(total_enrolled_months) / 12 as my 
	from cohort_cte group by 1,2 	
) inr 
order by 1,2 ;
					 
					 
--Member Counts by PlanType 
with cohort_cte as ( select data_source, uth_member_id, case when gender_cd = 'F' then 'AAF' else 'AAM' end as gender_cd, 
							case when age_derived between 0 and 19 then 'Age Group 1'
							    when age_derived between 20 and 34 then 'Age Group 2' 
								when age_derived between 35 and 44 then 'Age Group 3'
								when age_derived between 45 and 54 then 'Age Group 4'
								when age_derived between 55 and 64 then 'Age Group 5'
								when age_derived between 65 and 74 then 'Age Group 6'
								when age_derived >= 75 then 'Age Group 7' end as age_group, 
					      total_enrolled_months 
					      ,case when data_source in ('mdcd') then 'OTH' 
					            when data_source = 'optz' and plan_type in ('PPO','EPO','POS') then 'PPO' 
					            when data_source = 'optz' and plan_type in ('IND','OTH') then 'OTH' 
					            when data_source  = 'truv' and plan_type in ('CDHP','HDHP') then 'HDHP' 
					            when data_source = 'truv' and plan_type in ('PPO','EPO','POS') then 'PPO' 
					            when data_source = 'truv' and plan_type = 'HMO' then 'HMO'
					            when data_source = 'truv' and plan_type is null or plan_type in ('BMM','CMP') then 'OTH' 
					            else plan_type end as plan_type
                     from data_warehouse.member_enrollment_yearly 
                       where data_source in  ('mcrt','mdcd', 'optz','truv')
					    and "year" = 2019 
					    and state = 'TX'  
					 )
select * 
from 
( 
	select data_source, 'AAATotal' as measure, plan_type, count(distinct uth_member_id), sum(total_enrolled_months) / 12 as my 
	from cohort_cte group by 1,2,3 
union 
	select data_source, gender_cd, plan_type,count(distinct uth_member_id), sum(total_enrolled_months) / 12 as my 
	from cohort_cte group by 1,2,3 	
union 
	select data_source, age_group , plan_type,count(distinct uth_member_id), sum(total_enrolled_months) / 12 as my 
	from cohort_cte group by 1,2,3 	
) inr 
order by 1,2,3 ;



--Network / Non Network Claim Counts by PlanType 

---*****************
/*
drop table dev.wc_network_temp;

select uth_member_id , uth_claim_id, network_ind, data_source, bill_provider , ref_provider , other_provider , perf_op_provider  
into dev.wc_network_temp 
from data_warehouse.claim_detail 
where "year"  = 2019 
;

analyze dev.wc_network_temp ;

update dev.wc_network_temp set network_ind = false where data_source in ('truv','optz') and network_ind is null;


select count(*), network_ind, data_source 
from dev.wc_network_temp 
group by 2, 3
;

create table dev.wc_network_teaser_claims
with (appendonly=true, orientation=column) 
as 
select data_source, uth_member_id, uth_claim_id, bool_or(network_ind) as network_flag
from dev.wc_network_temp 
group by 1,2,3 
distributed by (uth_member_id)
;

 */

analyze dev.wc_network_teaser_claims;


--Network Claim Counts 
with cohort_cte as ( select data_source, uth_member_id, case when gender_cd = 'F' then 'AAF' else 'AAM' end as gender_cd, 
							case when age_derived between 0 and 19 then 'Age Group 1'
							    when age_derived between 20 and 34 then 'Age Group 2' 
								when age_derived between 35 and 44 then 'Age Group 3'
								when age_derived between 45 and 54 then 'Age Group 4'
								when age_derived between 55 and 64 then 'Age Group 5'
								when age_derived between 65 and 74 then 'Age Group 6'
								when age_derived >= 75 then 'Age Group 7' end as age_group, 
					      total_enrolled_months 
					      ,case when data_source in ('mdcd') then 'OTH' 
					            when data_source = 'optz' and plan_type in ('PPO','EPO','POS') then 'PPO' 
					            when data_source = 'optz' and plan_type in ('IND','OTH') then 'OTH' 
					            when data_source  = 'truv' and plan_type in ('CDHP','HDHP') then 'HDHP' 
					            when data_source = 'truv' and plan_type in ('PPO','EPO','POS') then 'PPO' 
					            when data_source = 'truv' and plan_type = 'HMO' then 'HMO'
					            when data_source = 'truv' and plan_type is null or plan_type in ('BMM','CMP') then 'OTH' 
					            else plan_type end as plan_type
                     from data_warehouse.member_enrollment_yearly 
                       where data_source in  ('mcrt','mdcd', 'optz','truv')
					    and "year" = 2019 
					    and state = 'TX'  
					 ),
	measure_cte as ( select data_source, uth_member_id, uth_claim_id, case when network_flag = true then 'Y' else 'N' end as network_flag 
					 from dev.wc_network_teaser_claims
					)					 
select * 
from 
( 
	select a.data_source, 'AAATotal' as measure, plan_type, network_flag, count(b.uth_claim_id) as clm_count 
	from cohort_cte a
     join measure_cte b 
       on b.uth_member_id = a.uth_member_id 
	group by 1,2,3, 4 
union 
	select a.data_source, gender_cd, plan_type, network_flag,  count(b.uth_claim_id) as clm_count
	from cohort_cte a
     join measure_cte b 
       on b.uth_member_id = a.uth_member_id 
	group by 1,2,3, 4
union 
	select a.data_source, age_group, plan_type, network_flag,  count(b.uth_claim_id) as clm_count
	from cohort_cte a
     join measure_cte b 
       on b.uth_member_id = a.uth_member_id 
	group by 1,2,3,4
) inr 
order by 1,2,3,4 ;



/*
drop table if exists dev.wc_network_teaser_facility;

select  data_source , uth_member_id , uth_claim_id,  facility_category , bool_or(network_ind) as network_flag 
into dev.wc_network_teaser_facility
from dev.ip_network_teaser
group by 1,2,3,4;

update dev.wc_network_teaser_facility set network_flag = false where network_flag is null;

 */

--Network Claim Counts by Category
with cohort_cte as ( select data_source, uth_member_id, case when gender_cd = 'F' then 'AAF' else 'AAM' end as gender_cd, 
							case when age_derived between 0 and 19 then 'Age Group 1'
							    when age_derived between 20 and 34 then 'Age Group 2' 
								when age_derived between 35 and 44 then 'Age Group 3'
								when age_derived between 45 and 54 then 'Age Group 4'
								when age_derived between 55 and 64 then 'Age Group 5'
								when age_derived between 65 and 74 then 'Age Group 6'
								when age_derived >= 75 then 'Age Group 7' end as age_group, 
					      total_enrolled_months 
					      ,case when data_source in ('mdcd') then 'OTH' 
					            when data_source = 'optz' and plan_type in ('PPO','EPO','POS') then 'PPO' 
					            when data_source = 'optz' and plan_type in ('IND','OTH') then 'OTH' 
					            when data_source  = 'truv' and plan_type in ('CDHP','HDHP') then 'HDHP' 
					            when data_source = 'truv' and plan_type in ('PPO','EPO','POS') then 'PPO' 
					            when data_source = 'truv' and plan_type = 'HMO' then 'HMO'
					            when data_source = 'truv' and plan_type is null or plan_type in ('BMM','CMP') then 'OTH' 
					            else plan_type end as plan_type
                     from data_warehouse.member_enrollment_yearly 
                       where data_source in  ('mcrt','mdcd', 'optz','truv')
					    and "year" = 2019 
					    and state = 'TX'  
					 ),
	measure_cte as ( select data_source , uth_member_id , uth_claim_id, network_flag 
	                  ,case when facility_category = 'SNF' then 1 else 0 end as SNF 
	                  ,case when facility_category = 'Rehabilitation' then 1 else 0 end as Rehabilitation
	                  ,case when facility_category = 'Psychiatric' then 1 else 0 end as Psychiatric
	                  ,case when facility_category = 'Hospital' then 1 else 0 end as Hospital
	 		          from dev.wc_network_teaser_facility
					)					 
select * 
from 
( 
	select a.data_source, 'AAATotal' as measure, plan_type, network_flag, 
	       sum(hospital) as hospital, sum(snf) as snf, sum(Psychiatric) as Psychiatric, sum(Rehabilitation) as rehab
	from cohort_cte a
     join measure_cte b 
       on b.uth_member_id = a.uth_member_id 
	group by 1,2,3, 4 
union 
	select a.data_source, gender_cd, plan_type, network_flag, 
	       sum(hospital) as hospital, sum(snf) as snf, sum(Psychiatric) as Psychiatric, sum(Rehabilitation) as rehab
	from cohort_cte a
     join measure_cte b 
       on b.uth_member_id = a.uth_member_id 
	group by 1,2,3, 4
union 
	select a.data_source, age_group, plan_type, network_flag, 
	       sum(hospital) as hospital, sum(snf) as snf, sum(Psychiatric) as Psychiatric, sum(Rehabilitation) as rehab
	from cohort_cte a
     join measure_cte b 
       on b.uth_member_id = a.uth_member_id 
	group by 1,2,3,4
) inr 
where network_flag is not null 
order by 1,2,3,4 ;



select *
from dev.ip_network_teaser
where cpt_hcpcs_cd between 'A0425' and 'A0436'