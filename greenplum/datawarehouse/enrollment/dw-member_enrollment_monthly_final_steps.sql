/* ******************************************************************************************************
 *  The member_enrollment_monthly table creates one record for each month/year that a member was enrolled in coverage
 *  This file runs the cleanup for duplicate rows and gets the value for consecutive enrolled months
 *  Run the relevant code section for the dataset in (---------------- data loads --------------------)
 * 
 *  !!!!!!!!!  data_warehouse.dim_member_id_src table must be populated first !!!!!!!!!  
 * !!!!!!!!!    dw_staging.member_enrollment_monthly must be populated first  !!!!!!!!!   
 *   	             Use dw-create-load-dim_member_id_src.sql in Git    
 * 	
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wc001  || 1/01/2021 || script created 
 * ******************************************************************************************************
 *  wc002  || 6/28/21 || added logic to exclude enrollment records after death optum dod
 * ******************************************************************************************************
 *  wallingTACC  || 8/23/2021 || Cleaning up comments
 * ******************************************************************************************************
 *  wc003  || 9/02/2021 || Changing process to load dw_staging. Add mapping for null race to assign 0 (Unknown).
 * ******************************************************************************************************
 *  jw001  || 9/20/2021 || Cut to its own script file from longer file
 *  ******************************************************************************************************
*/



----  // BEGIN SCRIPT 

-------(^---------------- data loads --------------------^)



------*****run below code for data sources***** vvvvvvvvvvvvvvvvvvvvvvvvvvvvvv

---redistribute table on row id so below scripts run quicker
create table dw_staging.temp_member_enrollment_monthly
with (appendonly=true, orientation=column) as 
select * from dw_staging.member_enrollment_monthly 
distributed by (row_id);

drop table if exists dw_staging.member_enrollment_monthly; 

alter table dw_staging.temp_member_enrollment_monthly rename to dw_staging.member_enrollment_monthly;

vacuum analyze dw_staging.member_enrollment_monthly;



---- get row id of duplicate rows
drop table if exists  dev.temp_dupe_enrollment_rows;

create table dev.temp_dupe_enrollment_rows
with (appendonly=true, orientation=column) as 
select row_id::bigint as row_id 
from
(		
select row_number() over(partition by uth_member_id, month_year_id order by month_year_id) as rn
	      ,*
	from dw_staging.member_enrollment_monthly 		
) sub
where rn > 1
distributed by (row_id);	

--remote dupe records so only one per member per month - runtime: 12min
delete from dw_staging.member_enrollment_monthly a 
  using dev.temp_dupe_enrollment_rows b 
   where a.row_id = b.row_id 
; 
 	
		
---**script to build consecutive enrolled months	  16min	
drop table if exists dev.temp_consec_enrollment;

create table dev.temp_consec_enrollment 
with (appendonly=true, orientation=column) as 
select row_id::bigint as row_id
      ,row_number() over(partition by uth_member_id, my_grp order by  month_year_id) as in_streak
from ( 
	   select a.row_id
	         ,a.month_year_id
	         ,a.uth_member_id
	         ,b.my_row_counter - row_number() over(partition by a.uth_member_id order by a.month_year_id) as my_grp
	   from dw_staging.member_enrollment_monthly 	 a 
	     join reference_tables.ref_month_year b 
	       on a.month_year_id = b.month_year_id 	   		    
	 ) inr    
distributed by (row_id);



--update consec enrolled months  9m 
update dw_staging.member_enrollment_monthly a set consecutive_enrolled_months = b.in_streak 
from dev.temp_consec_enrollment b 
where a.row_id = b.row_id
;

-----*****run above code for all data sets*****              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

--final
vacuum analyze dw_staging.member_enrollment_monthly;

----/END SCRIPT


select data_source, year, count(*)
from dw_staging.member_enrollment_monthly
group by data_source, year
order by data_source, year
;
