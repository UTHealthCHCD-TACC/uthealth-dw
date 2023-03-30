/* ******************************************************************************************************
 * Cleans up Medicaid fiscal year enrollment table
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 * xrzhang || 03/21/202 || Created
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
  VALUES ('CHIP', 1 ),
  	 ('CHIP Perinatal', 2),
	 ('STAR Kids', 3 ),
	 ('STAR+PLUS', 3 ),
	 ('STAR Health', 3 ),
	 ('STAR', 3 ),
	 ('MMP', 4 ),
	 ('FFS', 4 ),
	 ('PCCM', 4 );
                
---------------------
select distinct plan_type from dw_staging.mcd_member_enrollment_monthly;

drop table if exists dw_staging.mdcd_plan_count;

create table dw_staging.mdcd_plan_count 
with (appendonly=true, orientation=row) as (
select uth_member_id, fiscal_year, plan_type, 
       count(*) as "count", 
       max(month_year_id) as my
  from dw_staging.mcd_member_enrollment_monthly 
  group by 1,2,3
  )  distributed by(uth_member_id);
 analyze dw_staging.mdcd_plan_count ;

 
-----------------  row number by count, priority, then recent ------------------------------ 
drop table if exists dw_staging.mdcd_plan_rn;

create table dw_staging.mdcd_plan_rn 
with (appendonly=true, orientation=row) as (
select a.uth_member_id, fiscal_year, b.plan_type, row_number ()  
 		over(partition by uth_member_id, fiscal_year order by "count" desc, priority asc, my desc) as rn
  from dw_staging.mdcd_plan_count a 
  left outer join dw_staging.mdcd_plan_priority b 
  on a.plan_type = b.plan_type 
  )  distributed by(uth_member_id);
  ;
 
 analyze dw_staging.mdcd_plan_rn;

--------------  
--select * from dw_staging.mdcd_plan_rn;

update dw_staging.member_enrollment_fiscal_yearly a 
   set plan_type = b.plan_type 
  from dw_staging.mdcd_plan_rn  b 
 where a.uth_member_id = b.uth_member_id
   and a.fiscal_year = b.fiscal_year
   and b.rn = 1;	
 

/*
 * FINALIZE
 */

vacuum analyze dw_staging.member_enrollment_fiscal_yearly;
alter table dw_staging.member_enrollment_fiscal_yearly owner to uthealth_dev;
grant select on dw_staging.member_enrollment_fiscal_yearly to uthealth_analyst;




