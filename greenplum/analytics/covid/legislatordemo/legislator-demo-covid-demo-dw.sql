
-----------------------------------
-----All COVID Claims DW
------------------------------------


-----------------------------------
------------LVL 1
-------------------------------------


drop table if exists dev.dw_covid_lvl_1;

select uth_member_id 
into   dev.dw_covid_lvl_1
from   data_warehouse.claim_diag
where  from_date_of_service between '2020-01-01' and '2020-12-31' 
      and ( diag_cd = 'Z8616' 
        or diag_cd = 'U08' 
        or diag_cd = 'U09' 
        or diag_cd = 'B948'); 

select count(distinct uth_member_id) from  dev.dw_covid_lvl_1;


-----------------------------------
---------All Confirmed Cases
-------------------------------------       
         

drop table if exists dev.dw_covid_all_confirmed_work1; 

select a.uth_member_id, 
       a.uth_claim_id, 
       a.diag_cd, 
       a.from_date_of_service 
into   dev.dw_covid_all_confirmed_work1
from   data_warehouse.claim_diag a
				 where from_date_of_service between '2020-01-01' and '2020-12-31' 
         and (a.diag_cd in ('U071','U072','U10','J1282','J208','J988','B9729')
              or a.diag_cd like 'J22%' 
              or a.diag_cd like 'J40%' 
              or a.diag_cd like 'J80%') 
              ;

drop table if exists dev.dw_covid_all_confirmed_work2; 


select a.uth_member_id, 
       a.uth_claim_id 
into   dev.dw_covid_all_confirmed_work2
from   dev.dw_covid_all_confirmed_work1 a
join   dev.dw_covid_all_confirmed_work1 b
         on a.uth_member_id = b.uth_member_id and 
         a.uth_claim_id = b.uth_claim_id  
				 where a.diag_cd = 'U071'
              or a.diag_cd = 'U072'
              or a.diag_cd = 'U10'
              or (((a.diag_cd in ('J1282','J208','J988') and b.diag_cd = 'B9729')
              or (a.diag_cd like 'J22%' and b.diag_cd = 'B9729')
              or (a.diag_cd like 'J40%' and b.diag_cd = 'B9729')
              or (a.diag_cd like 'J80%' and b.diag_cd = 'B9729')) 
              and a.from_date_of_service between '2020-01-01' and '2020-04-01')
						;


drop table if exists dev.dw_covid_all_confirmed;

select distinct uth_member_id, 
       uth_claim_id 
into   dev.dw_covid_all_confirmed
from   dev.dw_covid_all_confirmed_work2
;
-----------------------------------
-- all inpatient
-----------------------------------
					
drop table if exists dev.dw_covid_inpatient;

select distinct a.uth_member_id, 
       a.uth_claim_id, 
       b.admit_date,
       b.discharge_date
into   dev.dw_covid_inpatient 
from   dev.dw_covid_all_confirmed a 
       join data_warehouse.claim_detail b 
         on a.uth_member_id = b.uth_member_id 
            and a.uth_claim_id = b.uth_claim_id 
where  b.bill_type_inst in ('1','2')
       and b.bill_type_class in ('1','2','5','6','7','8')
      		and admit_date is not null and discharge_date is not null;  
      	
   	

-----------------------------------
---------Level 2 or 3 inclusion / exclusion
-------------------------------------  

drop table if exists dev.dw_covid_include_exclude;

select distinct uth_member_id, uth_claim_id
into   dev.dw_covid_include_exclude
from   data_warehouse.claim_diag
where   from_date_of_service between '2020-01-01' and '2020-12-31'  
			  and
        (diag_cd = 'J1289' 
        or diag_cd = 'J40' 
        or diag_cd = 'J22' 
        or diag_cd = 'J988' 
        or diag_cd = 'J80' 
        or diag_cd = 'R05' 
        or diag_cd = 'R0602' 
        or diag_cd = 'R0603' 
        or diag_cd = 'R509');  
      
-----------------------------------
---------Level 2 
-------------------------------------  

drop table if exists dev.dw_covid_lvl_2;

--level 2 without level 3 exclusions
select  a.uth_member_id
into    dev.dw_covid_lvl_2
from    dev.dw_covid_all_confirmed a
        left outer join dev.dw_covid_include_exclude b 
						 on a.uth_claim_id = b.uth_claim_id 
						 and a.uth_member_id = b.uth_member_id 
	where b.uth_claim_id is null;


-----------------------------------
------------LVL 3
-------------------------------------   


drop table if exists dev.dw_covid_lvl_3;
 
-- level 2 with level 3 dx
select  a.uth_member_id
into    dev.dw_covid_lvl_3
from    dev.dw_covid_all_confirmed a
				join dev.dw_covid_include_exclude b 
				on a.uth_claim_id = b.uth_claim_id 
				and a.uth_member_id = b.uth_member_id 
;
    
-----------------------------------
------------All hospitalizations (LVL 5)
-------------------------------------  

drop table if exists dev.dw_covid_lvl_5;
 
select distinct uth_member_id  
into dev.dw_covid_lvl_5
from dev.dw_covid_inpatient;   

      
-----------------------------------
------------All hospitalizations (LVL 4)
-------------------------------------        
      
drop table if exists dev.dw_covid_lvl_4;
  
select distinct a.uth_member_id
into   dev.dw_covid_lvl_4
from   dev.dw_covid_all_confirmed a 
       join data_warehouse.claim_detail b 
         on a.uth_member_id = b.uth_member_id 
            and a.uth_claim_id = b.uth_claim_id 
where  b.revenue_cd between '0450' and '0459' 
   and b.from_date_of_service between '2020-01-01' and '2020-12-31'
   and not exists (select 1 
                       from   data_warehouse.claim_detail d
                       where  a.uth_member_id = d.uth_member_id 
                              and d.admit_date = b.from_date_of_service) 
                          group by a.uth_member_id ; 


           
-----------------------------------
------------LVL 6
-------------------------------------                                                    
                                       

drop table if exists dev.dw_covid_lvl_6;
                                                                                                                                                                                                                                      
select distinct a.uth_member_id
  into dev.dw_covid_lvl_6 
   from data_warehouse.claim_detail a 
  join  dev.dw_covid_inpatient b
         on a.uth_member_id = b.uth_member_id 
         and a.uth_claim_id = b.uth_claim_id 
 where (( a.cpt_hcpcs_cd in ( '94660', '94662', '94779' ) 
        or a.revenue_cd in ('0270','0175','0998','0272')))
        and a.bill_type_inst in ('1','2')
       and a.bill_type_class in ('1','2','5','6','7','8')
      and a.admit_date is not null and a.discharge_date is not null
     ;  
        
 

-----------------------------------
------------LVL 7
-------------------------------------
        
drop table if exists dev.dw_covid_lvl_7;  


select distinct a.uth_member_id
  into dev.dw_covid_lvl_7
  from data_warehouse.claim_detail a 
  join  dev.dw_covid_inpatient b
         on a.uth_member_id = b.uth_member_id 
         and a.uth_claim_id = b.uth_claim_id 
where (a.cpt_hcpcs_cd in ( '94002', '94003', '94004', '94005','31500') or revenue_cd = '0410') 
        and a.bill_type_inst in ('1','2')
       and a.bill_type_class in ('1','2','5','6','7','8')
      and a.admit_date is not null and a.discharge_date is not null
     ;  

                 
insert into dev.dw_covid_lvl_7 (uth_member_id)
select a.uth_member_id 
  from data_warehouse.claim_icd_proc a 
  join  dev.dw_covid_inpatient b
         on a.uth_member_id = b.uth_member_id 
          and a.uth_claim_id = b.uth_claim_id 
 where ( a.proc_cd in ( '5A1955Z', '5A1935Z', '5A1945Z' ) 
              or a.proc_cd like '5A093%' 
              or a.proc_cd like '5A094%' 
              or a.proc_cd like '5A095%' ) 
       and a.from_date_of_service between b.admit_date and b.discharge_date
       and a.from_date_of_service between '2020-01-01' and '2020-12-31';  



-----------------------------------
------------LVL 8
-------------------------------------          


drop table if exists dev.dw_covid_lvl_8;             
--drop table if exists dev.dw_covid_lvl_8_renal;

-----------------------renal---------------------------------------

select a.uth_member_id 
into   dev.dw_covid_lvl_8 
from data_warehouse.claim_detail a
  join  dev.dw_covid_inpatient b
         on a.uth_member_id = b.uth_member_id 
        	 and a.uth_claim_id = b.uth_claim_id 
where  (( a.revenue_cd between '0800' and '0809' )) 
        and a.bill_type_inst in ('1','2')
       and a.bill_type_class in ('1','2','5','6','7','8')
      and a.admit_date is not null and a.discharge_date is not null
     ;  



insert into dev.dw_covid_lvl_8 (uth_member_id) 
select a.uth_member_id 
 from data_warehouse.claim_icd_proc a 
  join  dev.dw_covid_inpatient b
         on a.uth_member_id = b.uth_member_id 
          and a.uth_claim_id = b.uth_claim_id 
where  ( a.proc_cd = '5A1D00Z' 
          or a.proc_cd = '5A1D60Z' 
          or a.proc_cd like '3E1M39Z' ) 
       and a.from_date_of_service between b.admit_date and b.discharge_date
       and a.from_date_of_service between '2020-01-01' and '2020-12-31';    


  
-----------------------ecmo---------------------------------------  
 
insert  into dev.dw_covid_lvl_8 (uth_member_id)     
select a.uth_member_id
from data_warehouse.claim_detail a
  join  dev.dw_covid_inpatient b
         on a.uth_member_id = b.uth_member_id 
  join  dev.dw_covid_inpatient c
         on a.uth_member_id = c.uth_member_id 
where (a.cpt_hcpcs_cd between '33946' and '33959' 
                or ( a.cpt_hcpcs_cd between '33962' and '33966' ) 
                or ( a.cpt_hcpcs_cd between '33984' and '33989' ) 
                or ( a.cpt_hcpcs_cd = '33969' )) 
        and a.admit_date is not null and a.discharge_date is not null 
       and a.from_date_of_service between c.admit_date and c.discharge_date
      and a.data_source = 'optd'
     ;  --163           

insert  into dev.dw_covid_lvl_8 (uth_member_id)
select a.uth_member_id 
from data_warehouse.claim_icd_proc a 
  join  dev.dw_covid_inpatient b
         on a.uth_member_id = b.uth_member_id 
          and a.uth_claim_id = b.uth_claim_id 
 where ( a.proc_cd = '5A1522F' 
          or ( a.proc_cd = '5A1522G' ) 
          or ( a.proc_cd = '5A1522H' ) ) 
       and a.from_date_of_service between b.admit_date and b.discharge_date 
       and a.from_date_of_service between '2020-01-01' and '2020-12-31';                     
          


--------------------------------------------
--------Level 9
--------------------------------------------    


drop table if exists dev.dw_covid_lvl_9;

select distinct a.uth_member_id 
  into  dev.dw_covid_lvl_9
  from data_warehouse.claim_detail a
  join dev.dw_covid_inpatient b
				on a.uth_member_id = b.uth_member_id 
				and a.uth_claim_id = b.uth_claim_id 
 where a.discharge_status in ( '20', '40', '41', '42' )
			and a.admit_date between '2020-01-01' and '2020-12-31'; 

insert  into dev.dw_covid_lvl_9 (uth_member_id)
 select distinct a.uth_member_id 
  from data_warehouse.claim_detail a
  join dev.dw_covid_inpatient b
				on a.uth_member_id = b.uth_member_id 
				and a.uth_claim_id = b.uth_claim_id 
 join  data_warehouse.member_enrollment_yearly c
 				on a.uth_member_id = c.uth_member_id 
 					and a.year = c."year" 
 where a.discharge_status = '00'
      and Date_trunc('month', a.to_date_of_service) = Date_trunc('month', c.death_date)
			and a.admit_date between '2020-01-01' and '2020-12-31'; 
		
		
insert  into dev.dw_covid_lvl_9 (uth_member_id)
select distinct a.uth_member_id 
  from data_warehouse.claim_detail a
  join dev.dw_covid_inpatient b
				on a.uth_member_id = b.uth_member_id 
				and a.uth_claim_id = b.uth_claim_id 
 where a.data_source = 'truv' and a.discharge_status is null ;



--------------------------------------------
--------Severity
--------------------------------------------             
             
 
drop table if exists dev.dw_covid_severity;
   
select a.uth_member_id, Max(lvl) as severity
into dev.dw_covid_severity
from
(select distinct uth_member_id,1 as lvl 
        from   dev.dw_covid_lvl_1
        union 
        select distinct uth_member_id,2 as lvl 
        from   dev.dw_covid_lvl_2
        union 
        select distinct uth_member_id,3 as lvl 
        from   dev.dw_covid_lvl_3
        union 
        select distinct uth_member_id,4 as lvl 
        from   dev.dw_covid_lvl_4
        union 
        select distinct uth_member_id,5 as lvl 
        from   dev.dw_covid_lvl_5 
        union 
        select distinct uth_member_id,6 as lvl 
        from   dev.dw_covid_lvl_6
        union 
        select distinct uth_member_id,7 as lvl 
        from   dev.dw_covid_lvl_7
        union 
        select distinct uth_member_id,8 as lvl 
        from   dev.dw_covid_lvl_8
        union 
        select distinct uth_member_id,9 as lvl 
        from   dev.dw_covid_lvl_9) 
        a
join data_warehouse.dim_uth_member_id b
on a.uth_member_id = b.uth_member_id 
where data_source in ('mcrt','mdcd','optz')
group  by a.uth_member_id ;


-----------------------------------------

  drop table if exists dev.zip_county;
  
  select distinct zip, county 
  into dev.zip_county 
  from dev.zip_county_dis_tx;


-------------------
 
 drop table if exists dev.leg_covid_list;
 
 CREATE TABLE dev.leg_covid_list (
	data_source text NULL,
	id text NULL,
	severity int4 NULL,
	age_derived int4 NULL,
	gender_cd text NULL,
	zip5 text NULL,
	county text NULL,
	age_group int4 NULL
)
DISTRIBUTED RANDOMLY;

insert into dev.leg_covid_list
select b.data_source, a.uth_member_id::text as id, a.severity, 
	   b.age_derived, b.gender_cd, b.zip5, c.county,
	   case
			when b.age_derived between 0 and 19 then 1
			when b.age_derived between 20 and 34 then 2
			when b.age_derived between 35 and 44 then 3
			when b.age_derived between 45 and 54 then 4
			when b.age_derived between 55 and 64 then 5
			when b.age_derived between 65 and 74 then 6
			when b.age_derived >=75 then 7 
		end as age_group
  --into dev.leg_covid_list
  from dev.dw_covid_severity a
  join data_warehouse.member_enrollment_yearly b
    on a.uth_member_id = b.uth_member_id 
  join dev.zip_county c 
    on b.zip5 = c.zip
 where b."year" = 2020
   and b.state = 'TX'
  ;
  
  select *
  from dev.leg_covid_list; --311414
  
 
  --------------------import ppl from TRS ERS-------------------------------------------------

  ---- make erc/trs table
drop table if exists dev.leg_covid_list_ers ;
 
CREATE TABLE dev.leg_covid_list_ers (
	data_source text NULL,
	id text NULL,
	severity int4 NULL,
	age_derived int4 NULL,
	gender_cd text NULL,
	zip5 text NULL,
	county text NULL,
	age_group int4 NULL
)
DISTRIBUTED RANDOMLY;
  

  ----psql to copy in ERS from  MS SQL SERVER 
  /*
   * \copy  dev.leg_covid_list_ers from 'A:\jwozny\covid\legislatordemo\demo_leg_covid_ers_trs.csv' delimiter ',' csv header;
   */
  
------------- insert into covid list   
  insert into dev.leg_covid_list
  select * from dev.leg_covid_list_ers;
 
 ----------change ers optz and trs to commercial---------------
 update dev.leg_covid_list
 	set data_source =
        case 
        	when data_source in ('TRS','ERS','optz') then 'commercial'
        	else data_source 
        end
        
 select data_source from dev.leg_covid_list group by data_source ;


-------------------------------------------------------
------------- create county summaries median
-------------------------------------------------------
   
select * from dev.leg_covid_list;


drop table if exists dev.demo_leg_covid_county_severity;

select count(distinct county) from dev.leg_covid_list;

select data_source as insurance, county, count(*) as cases, 
		percentile_disc(0.5) within group (order by severity) as s_median,
		percentile_disc(0.75) within group (order by severity) as s_75_perc
   into dev.demo_leg_covid_county_severity
   from dev.leg_covid_list
  group by data_source, county
  order by cases;

update dev.demo_leg_covid_county_severity
set s_75_perc = null 
where cases <= 10;

update dev.demo_leg_covid_county_severity
set s_median = null 
where cases <= 10;
   
select * from dev.demo_leg_covid_county_severity order by insurance, county;

-------------------------------------------

drop table if exists dev.leg_cov_totals;

select * 
into dev.leg_cov_totals 
from (
		select data_source, 'State' as county, 'Total' as cat, count(*) as total_persons_dx,
		   count(*) filter (where severity = 1) as c_1,
		   count(*) filter (where severity = 2) as c_2,
		   count(*) filter (where severity = 3) as c_3,
		   count(*) filter (where severity = 4) as c_4,
		   count(*) filter (where severity = 5) as c_5,
		   count(*) filter (where severity = 6) as c_6,
		   count(*) filter (where severity = 7) as c_7,
		   count(*) filter (where severity = 8) as c_8,
		   count(*) filter (where severity = 9) as c_9
	  from dev.leg_covid_list
	 group by data_source
union all
	select data_source, 'State' as county, 'Total Gender ' || gender_cd::text as cat, count(*) as total_persons_dx,
		   count(*) filter (where severity = 1) as c_1,
		   count(*) filter (where severity = 2) as c_2,
		   count(*) filter (where severity = 3) as c_3,
		   count(*) filter (where severity = 4) as c_4,
		   count(*) filter (where severity = 5) as c_5,
		   count(*) filter (where severity = 6) as c_6,
		   count(*) filter (where severity = 7) as c_7,
		   count(*) filter (where severity = 8) as c_8,
		   count(*) filter (where severity = 9) as c_9
	  from dev.leg_covid_list
	 group by data_source, gender_cd
union all
	select data_source, 'State' as county, 'Total Age Group ' || age_group::text as cat, count(*) as total_persons_dx,
		   count(*) filter (where severity = 1) as c_1,
		   count(*) filter (where severity = 2) as c_2,
		   count(*) filter (where severity = 3) as c_3,
		   count(*) filter (where severity = 4) as c_4,
		   count(*) filter (where severity = 5) as c_5,
		   count(*) filter (where severity = 6) as c_6,
		   count(*) filter (where severity = 7) as c_7,
		   count(*) filter (where severity = 8) as c_8,
		   count(*) filter (where severity = 9) as c_9
	  from dev.leg_covid_list
	 group by data_source, age_group
) a
where cat <> 'Total Gender U';




insert into dev.leg_cov_totals
select * from (
select data_source, county, 'Total' as cat, count(*) as total_persons_dx,
		   count(*) filter (where severity = 1) as c_1,
		   count(*) filter (where severity = 2) as c_2,
		   count(*) filter (where severity = 3) as c_3,
		   count(*) filter (where severity = 4) as c_4,
		   count(*) filter (where severity = 5) as c_5,
		   count(*) filter (where severity = 6) as c_6,
		   count(*) filter (where severity = 7) as c_7,
		   count(*) filter (where severity = 8) as c_8,
		   count(*) filter (where severity = 9) as c_9
	  from dev.leg_covid_list
	 group by data_source, county
union all
	select data_source, county, 'Total Gender ' || gender_cd::text as cat, count(*) as total_persons_dx,
		   count(*) filter (where severity = 1) as c_1,
		   count(*) filter (where severity = 2) as c_2,
		   count(*) filter (where severity = 3) as c_3,
		   count(*) filter (where severity = 4) as c_4,
		   count(*) filter (where severity = 5) as c_5,
		   count(*) filter (where severity = 6) as c_6,
		   count(*) filter (where severity = 7) as c_7,
		   count(*) filter (where severity = 8) as c_8,
		   count(*) filter (where severity = 9) as c_9
	  from dev.leg_covid_list
	 group by data_source, county, gender_cd
union all
	select data_source, county, 'Total Age Group ' || age_group::text as cat, count(*) as total_persons_dx,
		   count(*) filter (where severity = 1) as c_1,
		   count(*) filter (where severity = 2) as c_2,
		   count(*) filter (where severity = 3) as c_3,
		   count(*) filter (where severity = 4) as c_4,
		   count(*) filter (where severity = 5) as c_5,
		   count(*) filter (where severity = 6) as c_6,
		   count(*) filter (where severity = 7) as c_7,
		   count(*) filter (where severity = 8) as c_8,
		   count(*) filter (where severity = 9) as c_9
	  from dev.leg_covid_list
	 group by data_source, county, age_group
) a
where cat <> 'Total Gender U';

select * from dev.leg_cov_totals order by 1,2,3;



drop table if exists dev.demo_leg_allcats;

with ds as (
	select distinct data_source 
	from dev.leg_cov_totals
),
county as (
	select distinct county 
	from dev.leg_cov_totals
), cat as (
	select distinct cat 
	from dev.leg_cov_totals
)
	select distinct data_source, county, cat 
	into dev.demo_leg_allcats
	from ds, county, cat
	order by 1,2,3


select * from dev.demo_leg_allcats;


drop table if exists dev.demo_leg_county_covid;

select a.data_source, a.county, a.cat, 
	   coalesce(b.total_persons_dx, 0) as total_persons_dx,
	   coalesce(b.c_1, 0) as c_1,
	   coalesce(b.c_2, 0) as c_2,
	   coalesce(b.c_3, 0) as c_3,
	   coalesce(b.c_4, 0) as c_4,
	   coalesce(b.c_5, 0) as c_5,
	   coalesce(b.c_6, 0) as c_6,
	   coalesce(b.c_7, 0) as c_7,
	   coalesce(b.c_8, 0) as c_8,
	   coalesce(b.c_9, 0) as c_9
  into dev.demo_leg_county_covid
  from dev.demo_leg_allcats a 
  left outer join dev.leg_cov_totals b  
    on a.data_source = b.data_source 
   and a.county = b.county
  and a.cat = b.cat
  order by a.data_source, a.county, a.cat;
 
 select * from dev.demo_leg_county_covid order by 1,2,3;
 








-----------------------------------------------------------


 /*
drop table if exists dev.leg_covid_e;

select data_source, zip5, gender_cd, age_derived, case
			when b.age_derived between 0 and 19 then 1
			when b.age_derived between 20 and 34 then 2
			when b.age_derived between 35 and 44 then 3
			when b.age_derived between 45 and 54 then 4
			when b.age_derived between 55 and 64 then 5
			when b.age_derived between 65 and 74 then 6
			when b.age_derived >=75 then 7 
		end as age_group, total_enrolled_months 
  into dev.leg_covid_e
  from data_warehouse.member_enrollment_yearly b
 where "year" = 2020
   and state = 'TX'
   and zip5 is not null
   and gender_cd <> 'U';


  ---------------------------------------
  
  
  select * from dev.leg_covid_e;
  
  
  
  

-------------------------------------------------------

drop table if exists dev.leg_e_totals;

select * 
into dev.leg_e_totals 
from (
	select data_source, 'Total' as cat, zip5, 
	sum(total_enrolled_months) / 12 as ppl_years
	  from dev.leg_covid_e
	 group by data_source, zip5
union all
	select data_source, 'Total Gender ' || gender_cd::text as cat, zip5, 
	sum(total_enrolled_months) / 12 as ppl_years
	  from dev.leg_covid_e
	 group by data_source, zip5, gender_cd
union all
	select data_source, 'Total Age Group ' || age_group::text as cat, zip5, 
	sum(total_enrolled_months) / 12 as ppl_years
	  from dev.leg_covid_e
	 group by data_source, zip5, age_group
) a;
--------


drop table if exists dev.leg_covid_zip_totals;
 
 
  select a.data_source, a.cat, a.zip5, a.ppl_years,
		 coalesce(b.total_persons_dx,0) as total_persons_dx,
		 coalesce(b.c_1,0) as c_1,
		 coalesce(b.c_2,0) as c_2,
		 coalesce(b.c_3,0) as c_3,
		 coalesce(b.c_4,0) as c_4,
		 coalesce(b.c_5,0) as c_5,
		 coalesce(b.c_6,0) as c_6,
		 coalesce(b.c_7,0) as c_7,
		 coalesce(b.c_8,0) as c_8,
		 coalesce(b.c_9,0) as c_9
    into dev.leg_covid_zip_totals
	from dev.leg_e_totals a
	left join dev.leg_cov_totals b 
	  on a.data_source = b.data_source
	 and a.cat = b.cat
	 and a.zip5 = b.zip5;
 
	drop table if exists dev.test_pplyr;
	
select z.data_source, d.district, z.cat, z.zip5, z.ppl_years ,  
	   z.total_persons_dx, d.weight, z.ppl_years * weight as wtd_ppl_years
  into dev.test_pplyr
  from dev.leg_covid_zip_totals z
  join dev.districtweightbyzip d	
   on z.zip5 = d.zipcode 
order by z.data_source, z.zip5, cat;
	
	
	
	select sum(wtd_ppl_years)   --1133561.6054490716
	from dev.test_pplyr;
	
	select sum()
	
	
	*/
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
--cleanup 
/*
drop table if exists dev.dw_covid_severity;
drop table if exists dev.dw_covid_conf_alldx;
drop table if exists dev.dw_covid_conf_alldx_clms;
drop table if exists dev.dw_covid_lvl_1;
drop table if exists dev.dw_covid_lvl_2;
drop table if exists dev.dw_covid_lvl_2_1;
drop table if exists dev.dw_covid_lvl_2_2;
drop table if exists dev.dw_covid_lvl_2_3;
drop table if exists dev.dw_covid_lvl_3;
drop table if exists dev.dw_covid_lvl_3_1;
drop table if exists dev.dw_covid_lvl_4;
drop table if exists dev.dw_covid_lvl_5;
drop table if exists dev.dw_covid_lvl_6;
drop table if exists dev.dw_covid_lvl_7;
drop table if exists dev.dw_covid_lvl_8;
drop table if exists dev.dw_covid_lvl_8_renal;
drop table if exists dev.dw_covid_lvl_8;

*/
