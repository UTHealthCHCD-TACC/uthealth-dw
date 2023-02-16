---- NDC for opioid script
drop table if exists dev.wc_tease_opioid_temp;

 select uth_member_id 
   into dev.wc_tease_opioid_temp
 from data_warehouse.pharmacy_claims p
   join dev.wc_tease_ndc n 
     on p.ndc like '%' || n.ndc || '%' 
 where data_source in ('optz','mcrt','mdcd','truv')
   and p."year"  = 2019 
 ;


drop table if exists dev.wc_tease_opioid_script;
select distinct uth_member_id 
into dev.wc_tease_opioid_script
from dev.wc_tease_opioid_details
;

select * 
from data_warehouse.member_enrollment_yearly mey 
where data_source = 'mcrt'
;

--cost and details
drop table if exists dev.wc_tease_opioid_details;
 select uth_member_id , p.ndc, p.total_charge_amount, p.total_allowed_amount, p.total_paid_amount , p.uth_script_id , p.script_id, p.days_supply 
   into dev.wc_tease_opioid_details
 from data_warehouse.pharmacy_claims p
   join dev.wc_tease_ndc n 
     on p.ndc like '%' || n.ndc || '%' 
 where data_source in ('optz','mcrt','mdcd','truv')
   and p."year"  = 2019 
 ;




alter table dev.wc_tease_opioid_details add column data_source char(4);

update  dev.wc_tease_opioid_details a set data_source = b.data_source 
from data_warehouse.dim_uth_member_id b 
   where b.uth_member_id = a.uth_member_id 
   ;


----

select * from dev.wc_tease_opioid_details;

--first and last scripts
drop table if exists dev.wc_tease_opioid_fill_temp;

 select uth_member_id , p.fill_date, uth_script_id
   into dev.wc_tease_opioid_fill_temp
 from data_warehouse.pharmacy_claims p
   join dev.wc_tease_ndc n 
     on p.ndc like '%' || n.ndc || '%' 
 where data_source in ('optz','mcrt','mdcd','truv')
   and p."year"  = 2019 
 ;

--Step Five
drop table if exists dev.wc_tease_opioid_fills;

select uth_member_id, min(fill_date) as first_fill, max(fill_date) as last_fill
into dev.wc_tease_opioid_fills
from dev.wc_tease_opioid_fill_temp
group by 1 
;

---Step Three
drop table if exists dev.wc_tease_multiple_scripts;

select *
into dev.wc_tease_multiple_scripts
from (
	select count(distinct uth_script_id) as scripts, sum(total_charge_amount) as charge, sum(total_allowed_amount) as allowed, sum(total_paid_amount) as paid, uth_member_id
	from dev.wc_tease_opioid_details
	group by uth_member_id 
	) x where scripts > 1
;


---Step Four 
drop table if exists dev.wc_tease_30days;

select uth_member_id , days, scripts
into dev.wc_tease_30days
from (
	select sum(days_supply) as days, count(distinct uth_script_id) as scripts, uth_member_id
	from dev.wc_tease_opioid_details
	group by uth_member_id 
	) x where days >= 30
;

--total
  select a.data_source , count(a.uth_member_id) as members, sum(total_enrolled_months) / 12 as MY, count(b.uth_member_id) as opioid_prescription
  from data_warehouse.member_enrollment_yearly a 
     left outer join dev.wc_tease_opioid_script b 
        on b.uth_member_id = a.uth_member_id 
  where a.data_source in ('mcrt','mdcd', 'optz','truv')
    and a."year" = 2019 
    and age_derived >= 18 
    and state = 'TX'
    and rx_coverage = 1
  group by a.data_source 
  ;
 
  
 
--gender 
  select a.data_source , gender_cd , count(a.uth_member_id) as members, sum(total_enrolled_months) / 12 as MY, count(b.uth_member_id) as opioid_prescription
  from data_warehouse.member_enrollment_yearly a 
     left outer join dev.wc_tease_opioid_script b 
        on b.uth_member_id = a.uth_member_id 
  where a.data_source in ('mcrt','mdcd', 'optz','truv')
    and a."year" = 2019 
    and age_derived >= 18 
    and state = 'TX' and rx_coverage = 1
  group by 1,2 
  order by 1,2 
  ;
 
 
--age group 
select	data_source , case when age_derived between 0 and 19 then '1'
		    when age_derived between 20 and 34 then '2' 
			when age_derived between 35 and 44 then '3'
			when age_derived between 45 and 54 then '4'
			when age_derived between 55 and 64 then '5'
			when age_derived between 65 and 74 then '6'
			when age_derived >= 75 then '7' end as age_group,
	 count(a.uth_member_id) as members, 
	 sum(total_enrolled_months) / 12 as MY,
	 count(b.uth_member_id) as opioid_prescription
  from data_warehouse.member_enrollment_yearly a
      left outer join dev.wc_tease_opioid_script b 
        on b.uth_member_id = a.uth_member_id 
  where data_source in ('mcrt','mdcd', 'optz','truv')
    and "year" = 2019 
    and age_derived >= 18 
    and state = 'TX'      and rx_coverage = 1    
 group by 1 ,2 
 order by 1,2 
;


---opioid use with no prescription 
drop table if exists dev.wc_tease_opioid_use;
select uth_member_id 
into dev.wc_tease_use_temp
from data_warehouse.claim_diag 
where diag_cd like 'F11%'
  and data_source in ('mcrt','mdcd', 'optz','truv')
  and extract(year from from_date_of_service) = 2019
  ;
 
select distinct uth_member_id into dev.wc_tease_opioid_use from dev.wc_tease_use_temp;


--total
  select a.data_source , count(a.uth_member_id) as members, sum(total_enrolled_months) / 12 as MY, count(c.uth_member_id) as opioid_use
  from data_warehouse.member_enrollment_yearly a 
     left outer join dev.wc_tease_opioid_script b 
        on b.uth_member_id = a.uth_member_id 
     join dev.wc_tease_opioid_use c 
        on a.uth_member_id = c.uth_member_id 
  where b.uth_member_id is null 
    and a.data_source in ('mcrt','mdcd', 'optz','truv')
    and a."year" = 2019 
    and age_derived >= 18 
    and state = 'TX' and rx_coverage = 1    
  group by 1
  order by 1 
  ;
 
 --gender
  select a.data_source , gender_cd ,  count(a.uth_member_id) as members, sum(total_enrolled_months) / 12 as MY, count(c.uth_member_id) as opioid_use
  from data_warehouse.member_enrollment_yearly a 
     left outer join dev.wc_tease_opioid_script b 
        on b.uth_member_id = a.uth_member_id 
     join dev.wc_tease_opioid_use c 
        on a.uth_member_id = c.uth_member_id 
  where b.uth_member_id is null 
    and a.data_source in ('mcrt','mdcd', 'optz','truv')
    and a."year" = 2019 
    and age_derived >= 18 
    and state = 'TX' and rx_coverage = 1 
  group by 1,2
  order by 1,2
  ;


--age group 
select	data_source , case when age_derived between 0 and 19 then '1'
		    when age_derived between 20 and 34 then '2' 
			when age_derived between 35 and 44 then '3'
			when age_derived between 45 and 54 then '4'
			when age_derived between 55 and 64 then '5'
			when age_derived between 65 and 74 then '6'
			when age_derived >= 75 then '7' end as age_group,
	 count(a.uth_member_id) as members, 
	 sum(total_enrolled_months) / 12 as MY,
	 count(c.uth_member_id) as opioid_user
  from data_warehouse.member_enrollment_yearly a
     left outer join dev.wc_tease_opioid_script b 
        on b.uth_member_id = a.uth_member_id 
     join dev.wc_tease_opioid_use c 
        on a.uth_member_id = c.uth_member_id 
  where b.uth_member_id is null 
    and data_source in ('mcrt','mdcd', 'optz','truv')
    and "year" = 2019 
    and age_derived >= 18 
    and state = 'TX'   and rx_coverage = 1       
 group by 1 ,2 
 order by 1,2 
;


---total multiple scripts
with cohort_cte as ( select data_source, uth_member_id, gender_cd, 
							case when age_derived between 0 and 19 then '1'
							    when age_derived between 20 and 34 then '2' 
								when age_derived between 35 and 44 then '3'
								when age_derived between 45 and 54 then '4'
								when age_derived between 55 and 64 then '5'
								when age_derived between 65 and 74 then '6'
								when age_derived >= 75 then '7' end as age_group, 
					      total_enrolled_months 
                     from data_warehouse.member_enrollment_yearly 
                       where data_source in ('mcrt','mdcd', 'optz','truv')
					    and "year" = 2019 
					    and age_derived >= 18 
					    and state = 'TX'  and rx_coverage = 1  
					 ),
	 measure_cte as ( select  * 
	 			      from dev.wc_tease_30days
	                 -- from dev.wc_tease_multiple_scripts  
	                 )
select * 
from ( 
	select a.data_source 
	       ,'atotal' as measure
	       ,count(a.uth_member_id) as members
	       ,sum(scripts) as unique_scripts
	       ,sum(scripts) / count(a.uth_member_id) as avg_scripts
	      -- ,sum(charge) as total_charge, sum(allowed) as total_allowed, sum(paid) as total_paid 
	  from cohort_cte a 
	     join measure_cte b
	        on b.uth_member_id = a.uth_member_id 
	  group by 1
	union 
	    select a.data_source 
	          ,gender_cd as measure
			  ,count(a.uth_member_id) as members
	          ,sum(scripts) as unique_scripts
	          ,sum(scripts) / count(a.uth_member_id) as avg_scripts
	         -- ,sum(charge) as total_charge, sum(allowed) as total_allowed, sum(paid) as total_paid 
	  from cohort_cte a 
	     join measure_cte b
	        on b.uth_member_id = a.uth_member_id 
	  group by 1, 2
	union 
	    select a.data_source 
	         ,'Z' || age_group::text as measure
	         ,count(a.uth_member_id) as members
	         ,sum(scripts) as unique_scripts
	         ,sum(scripts) / count(a.uth_member_id) as avg_scripts
	         --,sum(charge) as total_charge, sum(allowed) as total_allowed, sum(paid) as total_paid 
	  from cohort_cte a 
	     join measure_cte b
	        on b.uth_member_id = a.uth_member_id 
	  group by 1, 2
   ) inr 
 order by 1, 2 asc;



---Step Five
--dev.wc_tease_opioid_fills
drop table if exists dev.wc_tease_conditions;

--
select uth_member_id , a.from_date_of_service, a.data_source, 'chronic pain' as cond  
into dev.wc_tease_conditions
from data_warehouse.claim_diag a 
where data_source in ('mcrt','mdcd','optz','truv')
  and from_date_of_service between '2018-11-01' and '2020-02-01'
  and diag_cd in ('G8921','G894','G8929','G8928','G8922','G892','F4541','G894','G890')
  ;
 
insert into dev.wc_tease_conditions 
select uth_member_id , a.from_date_of_service, a.data_source, 'cancer' as cond  
from data_warehouse.claim_diag a 
where data_source in ('mcrt','mdcd','optz','truv')
  and from_date_of_service between '2018-11-01' and '2020-02-01'
  and  ( diag_cd in ('Z510','Z511','Z5111','Z5112','Z08')
        or   substring(diag_cd,1,3) between   'C00'  and    'C14'
		or substring(diag_cd,1,3) between  'C15'  and    'C26'
		or substring(diag_cd,1,3) between  'C30'  and    'C39'
		or substring(diag_cd,1,3) between  'C40'  and    'C41'
		or substring(diag_cd,1,3) between  'C43'  and    'C44'
		or substring(diag_cd,1,3) between  'C45'  and    'C49'
		or substring(diag_cd,1,3) = 'C50'
		or substring(diag_cd,1,3) between  'C51'  and    'C58'
		or substring(diag_cd,1,3) between  'C60'  and    'C63'
		or substring(diag_cd,1,3) between  'C64'  and    'C68'
		or substring(diag_cd,1,3) between  'C69'  and    'C72'
		or substring(diag_cd,1,3) between 'C73'  and    'C75'
		or substring(diag_cd,1,3) between 'C76'  and    'C80'
		or substring(diag_cd,1,3) =  'C7A'
		or substring(diag_cd,1,3) =  'C7B'
		or substring(diag_cd,1,3) between  'C81'  and    'C96'
		or substring(diag_cd,1,3) between 'D00'  and  'D09'
       );
 
insert into dev.wc_tease_conditions
select uth_member_id , a.from_date_of_service, a.data_source, 'paliative care' as cond  
from data_warehouse.claim_diag a 
where data_source in ('mcrt','mdcd','optz','truv')
  and from_date_of_service between '2018-11-01' and '2020-02-01'
  and diag_cd = 'Z515'
  ;
 
insert into dev.wc_tease_conditions
select uth_member_id , a.from_date_of_service, a.data_source, 'vascular pain' as cond  
from data_warehouse.claim_diag a 
where data_source in ('mcrt','mdcd','optz','truv')
  and from_date_of_service between '2018-11-01' and '2020-02-01'
  and diag_cd in ('D570','D571','D572')
  ; 
 
insert into dev.wc_tease_conditions
select uth_member_id , a.from_date_of_service, a.data_source, 'ms' as cond  
from data_warehouse.claim_diag a 
where data_source in ('mcrt','mdcd','optz','truv')
  and from_date_of_service between '2018-11-01' and '2020-02-01'
  and diag_cd in ( 'G35','M05','M06','M797')
  ; 
 
 
 select first_fill - 30, last_fill + 30, * 
 from dev.wc_tease_opioid_fills
 ;

 
 
 ---more than one script and condition within 30 days 
with cohort_cte as ( select data_source, uth_member_id, gender_cd, 
							case when age_derived between 0 and 19 then '1'
							    when age_derived between 20 and 34 then '2' 
								when age_derived between 35 and 44 then '3'
								when age_derived between 45 and 54 then '4'
								when age_derived between 55 and 64 then '5'
								when age_derived between 65 and 74 then '6'
								when age_derived >= 75 then '7' end as age_group, 
					      total_enrolled_months 
                     from data_warehouse.member_enrollment_yearly 
                       where data_source in ('mcrt','mdcd','optz','truv')
					    and "year" = 2019 
					    and age_derived >= 18 
					    and state = 'TX' and rx_coverage = 1
					 ),
	 measure_cte as ( select  * 
	                  from dev.wc_tease_multiple_scripts  
	                 )
select * 
from ( 
	select a.data_source 
	       ,'atotal' as measure
	       ,count(a.uth_member_id) as members
	       ,sum(scripts) as unique_scripts
	       ,sum(scripts) / count(a.uth_member_id) as avg_scripts	       
	  from cohort_cte a 
	     join measure_cte b
	        on b.uth_member_id = a.uth_member_id 
	     join dev.wc_tease_opioid_fills c 
	        on c.uth_member_id = a.uth_member_id 
	     where exists ( select 1 
	                    from dev.wc_tease_conditions d 
	                    where d.uth_member_id = a.uth_member_id 
	                    and d.from_date_of_service between c.first_fill - 30 and c.last_fill + 30 )
	  group by 1
	--gender
	union 
	    select a.data_source 
	          ,gender_cd as measure
			  ,count(a.uth_member_id) as members
	          ,sum(scripts) as unique_scripts
	          ,sum(scripts) / count(a.uth_member_id) as avg_scripts
	  from cohort_cte a 
	     join measure_cte b
	        on b.uth_member_id = a.uth_member_id 
	     join dev.wc_tease_opioid_fills c 
	        on c.uth_member_id = a.uth_member_id 
	     where exists ( select 1 
	                    from dev.wc_tease_conditions d 
	                    where d.uth_member_id = a.uth_member_id 
	                    and d.from_date_of_service between c.first_fill - 30 and c.last_fill + 30 )
	  group by 1, 2
	--age group 
	union 
	    select a.data_source 
	         ,'Z' || age_group::text as measure
	         ,count(a.uth_member_id) as members
	         ,sum(scripts) as unique_scripts
	         ,sum(scripts) / count(a.uth_member_id) as avg_scripts
	  from cohort_cte a 
	     join measure_cte b
	        on b.uth_member_id = a.uth_member_id 
	     join dev.wc_tease_opioid_fills c 
	        on c.uth_member_id = a.uth_member_id 
	     where exists ( select 1 
	                    from dev.wc_tease_conditions d 
	                    where d.uth_member_id = a.uth_member_id 
	                    and d.from_date_of_service between c.first_fill - 30 and c.last_fill + 30 )
	  group by 1, 2
   ) inr 
 order by 1, 2 asc;
 



--Step Six
drop table if exists dev.wc_tease_step6; 

analyze dev.wc_tease_step6;


create table dev.wc_tease_step6 
with (appendonly=true, orientation=column) 
as 
select a.*, c.first_fill 
from dev.wc_tease_multiple_scripts a 
 join dev.wc_tease_opioid_fills c 
	        on c.uth_member_id = a.uth_member_id 
	     where not exists ( select 1 
	                    from dev.wc_tease_conditions d 
	                    where d.uth_member_id = a.uth_member_id 
	                    and d.from_date_of_service between c.first_fill - 30 and c.last_fill + 30 )
distributed by (uth_member_id );



create table dev.wc_tease_claim_diag 
with (appendonly=true, orientation=column) 
as 
select uth_member_id, data_source, uth_claim_id, diag_cd, from_date_of_service
from data_warehouse.claim_diag 
where data_source in ('mcrt','mdcd','optz','truv')
  and  from_date_of_service between '2018-06-01' and '2020-03-01'
  and diag_position = 1 
distributed by (uth_member_id) 
;




--drop table if exists 
analyze 
dev.wc_tease_claim_diag 
;

drop table dev.wc_tease_step6_final;

select * 
into dev.wc_tease_step6_final
from ( 
		select d.uth_member_id, diag_cd, from_date_of_service
		       , row_number () over(partition by d.uth_member_id order by from_date_of_service desc) as d_grp
		from dev.wc_tease_claim_diag d 
		   join dev.wc_tease_step6 a 
		     on a.uth_member_id = d.uth_member_id 
   ) x where d_grp = 1 
;


select * from dev.wc_tease_step6_final
;


--Step 6 rank by diag 
select count(*), diag_cd , b.code_description 
from dev.wc_tease_step6_final a 
  left outer join reference_tables.ref_cms_codes b 
     on b.cd_value  = a.diag_cd
group by 2,3 order by 1 desc
;

---Step Six
with cohort_cte as ( select data_source, uth_member_id, gender_cd, 
							case when age_derived between 0 and 19 then '1'
							    when age_derived between 20 and 34 then '2' 
								when age_derived between 35 and 44 then '3'
								when age_derived between 45 and 54 then '4'
								when age_derived between 55 and 64 then '5'
								when age_derived between 65 and 74 then '6'
								when age_derived >= 75 then '7' end as age_group, 
					      total_enrolled_months 
                     from data_warehouse.member_enrollment_yearly 
                       where data_source in ('mcrt','mdcd', 'optz','truv')
					    and "year" = 2019 
					    and age_derived >= 18 
					    and state = 'TX' and rx_coverage = 1 
					 ),
	 measure_cte as ( select  * 
	                  from dev.wc_tease_step6_final 
	                 )
select * 
from ( 
	select a.data_source 
	       --,'a'  as measure
	       ,gender_cd 
	       --,age_group
	       ,diag_cd   
	       ,count(a.uth_member_id) as members
	  from cohort_cte a 
	     join measure_cte b
	        on b.uth_member_id = a.uth_member_id 
	    where data_source = 'mcrt'
	  group by 1,2,3
	  order by 4 desc 
	 limit 50
) inr	
order by 1, 2, 4 desc
;	  


---Step Six Costs 
with cohort_cte as ( select data_source, uth_member_id, gender_cd, 
							case when age_derived between 0 and 19 then '1'
							    when age_derived between 20 and 34 then '2' 
								when age_derived between 35 and 44 then '3'
								when age_derived between 45 and 54 then '4'
								when age_derived between 55 and 64 then '5'
								when age_derived between 65 and 74 then '6'
								when age_derived >= 75 then '7' end as age_group, 
					      total_enrolled_months 
                     from data_warehouse.member_enrollment_yearly 
                       where data_source in ('mcrt','mdcd', 'optz','truv')
					    and "year" = 2019 
					    and age_derived >= 18 
					    and state = 'TX' and rx_coverage = 1
					 ),
	 measure_cte as ( select  * 
	 			      --from dev.wc_tease_30days
	                  from dev.wc_tease_multiple_scripts  
	                  
	                 )
select * 
from ( 
	select a.data_source 
	       ,'atotal' as measure
	       ,count(a.uth_member_id) as members
	       --,sum(scripts) as unique_scripts
	       --,sum(scripts) / count(a.uth_member_id) as avg_scripts
	       ,sum(days_supply) / sum(scripts) as days_per_script
	       --,sum(charge) as total_charge, sum(allowed) as total_allowed, sum(paid) as total_paid 
	  from cohort_cte a 
	     join measure_cte b
	        on b.uth_member_id = a.uth_member_id 
	     join dev.wc_tease_step6_final c 
	        on c.uth_member_id = a.uth_member_id 
	  group by 1
	--gender
	union 
	    select a.data_source 
	          ,gender_cd as measure
			  ,count(a.uth_member_id) as members
	          --,sum(scripts) as unique_scripts
	          --,sum(scripts) / count(a.uth_member_id) as avg_scripts
	          ,sum(days_supply) / sum(scripts) as days_per_script
	          --,sum(charge) as total_charge, sum(allowed) as total_allowed, sum(paid) as total_paid 
	  from cohort_cte a 
	     join measure_cte b
	        on b.uth_member_id = a.uth_member_id 
	  	     join dev.wc_tease_step6_final c 
	        on c.uth_member_id = a.uth_member_id 
	        group by 1, 2
	--age group 
	union 
	    select a.data_source 
	         ,'Z' || age_group::text as measure
	         ,count(a.uth_member_id) as members
	        -- ,sum(scripts) as unique_scripts
	         --,sum(scripts) / count(a.uth_member_id) as avg_scripts
	         ,sum(days_supply) / sum(scripts) as days_per_script
	        -- ,sum(charge) as total_charge, sum(allowed) as total_allowed, sum(paid) as total_paid 
	  from cohort_cte a 
	     join measure_cte b
	        on b.uth_member_id = a.uth_member_id 
	 	     join dev.wc_tease_step6_final c 
	        on c.uth_member_id = a.uth_member_id 
	        group by 1, 2
   ) inr 
 order by 1, 2 asc;



---Step Seven 
drop table if exists dev.wc_tease_90days;

select uth_member_id , days, scripts
into dev.wc_tease_90days
from (
	select sum(days_supply) as days, count(distinct uth_script_id) as scripts, uth_member_id
	from dev.wc_tease_opioid_details
	group by uth_member_id 
	) x where days >= 90
;

drop table dev.wc_tease_opioid_use_temp
;

---opioid dependence
select distinct uth_member_id 
into dev.wc_tease_opioid_dependence_temp
from data_warehouse.claim_diag cd 
where data_source in ('mcrt','mdcd','optz','truv') 
  and substring(diag_cd ,1,4) between 'F111' and 'F112'
  and from_date_of_service between '2019-01-01' and '2019-12-31'
 ;
 
 
---opioid use
select distinct uth_member_id 
into dev.wc_tease_opioid_use_temp
from data_warehouse.claim_diag cd 
where data_source in ('mcrt','mdcd','optz','truv')  
  and diag_cd like 'F119%'
  and from_date_of_service between '2019-01-01' and '2019-12-31'
 ; 
 


select * from reference_tables.ref_cms_codes 
where cd_value like 'F119%'; 'F11%';
  
  
  ---Step Seven   long-term users 
  with cohort_cte as ( select data_source, uth_member_id, gender_cd, 
							case when age_derived between 0 and 19 then '1'
							    when age_derived between 20 and 34 then '2' 
								when age_derived between 35 and 44 then '3'
								when age_derived between 45 and 54 then '4'
								when age_derived between 55 and 64 then '5'
								when age_derived between 65 and 74 then '6'
								when age_derived >= 75 then '7' end as age_group, 
					      total_enrolled_months 
                     from data_warehouse.member_enrollment_yearly 
                       where data_source in ('mcrt','mdcd', 'optz','truv')
					    and "year" = 2019 
					    and age_derived >= 18 
					    and state = 'TX' and rx_coverage = 1 
					 ),
	 measure_cte as ( select  * 
	 				from dev.wc_tease_opioid_script
	 			      --from dev.wc_tease_90days                
	                 )
select * 
from ( 
	select a.data_source 
	       ,'atotal' as measure
	       ,count(a.uth_member_id) as members
	       ,count(c.uth_member_id) as dependence
	       ,count(d.uth_member_id) as use_disorder
	  from cohort_cte a 
	     join measure_cte b
	        on b.uth_member_id = a.uth_member_id 
	     left outer join dev.wc_tease_opioid_dependence_temp c 
	        on c.uth_member_id = a.uth_member_id
	     left outer join dev.wc_tease_opioid_use_temp d 
	        on d.uth_member_id = a.uth_member_id
	  group by 1
	--gender
	union 
	    select a.data_source 
	          ,gender_cd as measure
			  ,count(a.uth_member_id) as members  
			  ,count(c.uth_member_id) as dependence
	          ,count(d.uth_member_id) as use_disorder
	  from cohort_cte a 
	     join measure_cte b
	        on b.uth_member_id = a.uth_member_id 
	     left outer join dev.wc_tease_opioid_dependence_temp c 
	        on c.uth_member_id = a.uth_member_id
	     left outer join dev.wc_tease_opioid_use_temp d 
	        on d.uth_member_id = a.uth_member_id
	        group by 1, 2
	--age group 
	union 
	    select a.data_source 
	         ,'Z' || age_group::text as measure
	         ,count(a.uth_member_id) as members
	         ,count(c.uth_member_id) as dependence
	         ,count(d.uth_member_id) as use_disorder
	  from cohort_cte a 
	     join measure_cte b
	        on b.uth_member_id = a.uth_member_id 
	     left outer join dev.wc_tease_opioid_dependence_temp c 
	        on c.uth_member_id = a.uth_member_id
	     left outer join dev.wc_tease_opioid_use_temp d 
	        on d.uth_member_id = a.uth_member_id
	        group by 1, 2
   ) inr 
 order by 1, 2 asc;
 

--avg avg_scripts 

----average day supply count
drop table if exists dev.wc_tease_avg_days;

select uth_member_id, sum(days_supply) as days_supply  , count(distinct uth_script_id) as scripts
into dev.wc_tease_avg_days
from dev.wc_tease_opioid_details 
group by uth_member_id 
;

select count(*), count(distinct uth_member_id)
from dev.wc_tease_avg_days
;

select *
from dev.wc_tease_avg_days 
;

---avg 
with cohort_cte as ( select data_source, uth_member_id, gender_cd, 
							case when age_derived between 0 and 19 then '1'
							    when age_derived between 20 and 34 then '2' 
								when age_derived between 35 and 44 then '3'
								when age_derived between 45 and 54 then '4'
								when age_derived between 55 and 64 then '5'
								when age_derived between 65 and 74 then '6'
								when age_derived >= 75 then '7' end as age_group, 
					      total_enrolled_months 
                     from data_warehouse.member_enrollment_yearly 
                       where data_source in ('mcrt','mdcd', 'optz','truv')
					    and "year" = 2019 
					    and age_derived >= 18 
					    and state = 'TX' and rx_coverage = 1
					 ),
	 measure_cte as ( select  * 
	                  from dev.wc_tease_avg_days
	                 )
select * 
from ( 
	select a.data_source 
	       ,'atotal' as measure
	       ,count(a.uth_member_id) as members
	       --,sum(scripts) as unique_scripts
	       --,sum(scripts) / count(a.uth_member_id) as avg_scripts
	       ,sum(days_supply) / sum(scripts) as days_per_script
	       --,sum(charge) as total_charge, sum(allowed) as total_allowed, sum(paid) as total_paid 
	  from cohort_cte a 
	     join measure_cte b
	        on b.uth_member_id = a.uth_member_id 
	  group by 1
	--gender
	union 
	    select a.data_source 
	          ,gender_cd as measure
			  ,count(a.uth_member_id) as members
	          --,sum(scripts) as unique_scripts
	          --,sum(scripts) / count(a.uth_member_id) as avg_scripts
	          ,sum(days_supply) / sum(scripts) as days_per_script
	          --,sum(charge) as total_charge, sum(allowed) as total_allowed, sum(paid) as total_paid 
	  from cohort_cte a 
	     join measure_cte b
	        on b.uth_member_id = a.uth_member_id 
	        group by 1, 2
	union 
	    select a.data_source 
	         ,'Z' || age_group::text as measure
	         ,count(a.uth_member_id) as members
	        -- ,sum(scripts) as unique_scripts
	         --,sum(scripts) / count(a.uth_member_id) as avg_scripts
	         ,sum(days_supply) / sum(scripts) as days_per_script
	        -- ,sum(charge) as total_charge, sum(allowed) as total_allowed, sum(paid) as total_paid 
	  from cohort_cte a 
	     join measure_cte b
	        on b.uth_member_id = a.uth_member_id 
	        group by 1, 2
   ) inr 
 order by 1, 2 asc;

  