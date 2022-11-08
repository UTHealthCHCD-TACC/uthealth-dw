drop table if exists dw_staging.mdcd_plan_priority;

create table dw_staging.mdcd_plan_priority (
	plan_type text null, priority int null
	);

--add values to it
INSERT INTO dw_staging.mdcd_plan_priority(plan_type, priority)
  VALUES ('CHIP', 1 ),
	 ('STAR Kids', 2 ),
	 ('STAR+PLUS', 2 ),
	 ('STAR Health', 2 ),
	 ('STAR', 3 ),
	 ('MMP', 4 ),
	 ('FFS', 4 ),
	 ('PCCM', 4 );
                
select * from dw_staging.mdcd_plan_priority;

---------------------

drop table if exists dw_staging.mdcd_plan_count;

create table dw_staging.mdcd_plan_count 
with (appendonly=true, orientation=row) as (
select uth_member_id, "year", plan_type, 
       count(*) as "count", 
       max(month_year_id) as my
  from dw_staging.member_enrollment_monthly 
  group by 1,2,3
  )  distributed by(uth_member_id);
 analyze dw_staging.mdcd_plan_count ;

 
-----------------  row number by count, priority, then recent ------------------------------ 
drop table if exists dw_staging.mdcd_plan_rn;

create table dw_staging.mdcd_plan_rn 
with (appendonly=true, orientation=row) as (
select a.uth_member_id, year, b.plan_type, row_number ()  
 		over(partition by uth_member_id, year order by "count" desc, priority asc, my desc) as rn
  from dw_staging.mdcd_plan_count a 
  left outer join dw_staging.mdcd_plan_priority b 
  on a.plan_type = b.plan_type 
  )  distributed by(uth_member_id);
  ;
 
 analyze dw_staging.mdcd_plan_rn;

--------------  
select * from dw_staging.mdcd_plan_rn;

update dw_staging.member_enrollment_yearly a 
   set plan_type = b.plan_type 
  from dw_staging.mdcd_plan_rn  b 
 where a.uth_member_id = b.uth_member_id
   and a.year = b.year
   and b.rn = 1;	
  
/*
 * FIX RACE CD
 */
  
  drop table if exists dw_staging.mdcd_race_count;

create table dw_staging.mdcd_race_count 
with (appendonly=true, orientation=row) as (
select uth_member_id, "year", race_cd, 
       count(*) as "count", 
       max(month_year_id) as my
  from dw_staging.member_enrollment_monthly 
  group by 1,2,3
  )  distributed by(uth_member_id);
 analyze dw_staging.mdcd_plan_count ;
 
-----------------  row number by count, priority, then recent ------------------------------ 
drop table if exists dw_staging.mdcd_race_rn;

create table dw_staging.mdcd_race_rn 
with (appendonly=true, orientation=row) as (
select a.uth_member_id, year, race_cd , row_number ()  
 		over(partition by uth_member_id, year order by "count" desc, race_cd desc, my desc) as rn
  from dw_staging.mdcd_race_count a 
  )  distributed by(uth_member_id);
  ;
 
 analyze dw_staging.mdcd_race_rn;

--------------  
select * from dw_staging.mdcd_race_rn;

update dw_staging.member_enrollment_yearly a 
   set race_cd  = b.race_cd  
  from dw_staging.mdcd_race_rn  b 
 where a.uth_member_id = b.uth_member_id
   and a.year = b.year
   and b.rn = 1;	




/*
 * FINALIZE
 */

vacuum analyze dw_staging.member_enrollment_yearly;
alter table dw_staging.member_enrollment_yearly  owner to uthealth_dev;
grant select on dw_staging.member_enrollment_yearly to uthealth_analyst;