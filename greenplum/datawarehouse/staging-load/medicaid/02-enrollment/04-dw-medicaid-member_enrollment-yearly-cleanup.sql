/* ******************************************************************************************************
 * Assigns plan type on yearly level according to heirarchy
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 * JWozny wrote this code prior to versioning
 * ******************************************************************************************************
 * xrzhang || 03/30/202 || Modified for CHIP Perinatal + splitting chip peri/ htw to their own data_sources
 */

/*
* assigning a yearly plan type requires logic about the hierarchy of plan types
*/

drop table if exists dw_staging.mdcd_plan_priority;

create table dw_staging.mdcd_plan_priority (
	plan_type text null, priority int null
	);

--add values to it
INSERT INTO dw_staging.mdcd_plan_priority(plan_type, priority)
  VALUES ('CHIP PERI', 1 ),
  	 ('CHIP', 2),
	 ('STAR Kids', 3 ),
	 ('STAR+PLUS', 3 ),
	 ('STAR Health', 3 ),
	 ('STAR', 3 ),
	 ('MMP', 4 ),
	 ('FFS', 4 ),
	 ('PCCM', 4 );
                
---------------------

drop table if exists dw_staging.mdcd_plan_count;

create table dw_staging.mdcd_plan_count 
with (appendonly=true, orientation=row) as (
select data_source, uth_member_id, "year", plan_type, 
       count(*) as "count", 
       max(month_year_id) as my
  from dw_staging.mcd_member_enrollment_monthly 
  group by data_source, uth_member_id, "year", plan_type
  )  distributed by(uth_member_id);
 
 analyze dw_staging.mdcd_plan_count ;

 
-----------------  row number by count, priority, then recent ------------------------------ 
drop table if exists dw_staging.mdcd_plan_rn;

create table dw_staging.mdcd_plan_rn 
with (appendonly=true, orientation=row) as (
select a.data_source, a.uth_member_id, year, b.plan_type, row_number ()  
 		over(partition by data_source, uth_member_id, "year" order by "count" desc, priority asc, my desc) as rn
  from dw_staging.mdcd_plan_count a 
  left outer join dw_staging.mdcd_plan_priority b 
  on a.plan_type = b.plan_type 
  )  distributed by(uth_member_id);
  ;
 
 analyze dw_staging.mdcd_plan_rn;

--------------  
--select * from dw_staging.mdcd_plan_rn;

update dw_staging.mcd_member_enrollment_yearly a 
   set plan_type = b.plan_type 
  from dw_staging.mdcd_plan_rn  b 
 where a.data_source = b.data_source
 	and a.uth_member_id = b.uth_member_id
	and a."year" = b."year"
	and b.rn = 1;	
 

/*
 * FINALIZE
 */

vacuum analyze dw_staging.mcd_member_enrollment_yearly;
alter table dw_staging.mcd_member_enrollment_yearly owner to uthealth_dev;
grant select on dw_staging.mcd_member_enrollment_yearly to uthealth_analyst;


/*check
select * from dw_staging.mcd_member_enrollment_yearly;

If pass check then can drop tables

drop table if exists dw_staging.mdcd_plan_priority;
drop table if exists dw_staging.mdcd_plan_count;
drop table if exists dw_staging.mdcd_plan_rn;
*/






