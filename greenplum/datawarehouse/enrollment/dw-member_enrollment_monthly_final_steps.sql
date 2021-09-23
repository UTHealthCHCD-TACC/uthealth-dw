/* ******************************************************************************************************
 *  The script should be run as the final step once dw_staging.member_enrollment_monthly has been updated
 * 	
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wc001  || 1/01/2021 || script created 
 * ******************************************************************************************************
 * ******************************************************************************************************
 *  jw001  || 9/20/2021 || Cut to its own script file from longer file
 *  ******************************************************************************************************
*/


---- BEGIN SCRIPT -----


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

------------------------------------------------------------
--**cleanup
drop table if exists dev.temp_consec_enrollment;
drop table if exists dev.temp_dupe_enrollment_rows;

---/drop sequence, rebuild table distributed on uth_member_id 
alter table dw_staging.member_enrollment_monthly drop column row_id;

create table dw_staging.member_enrollment_monthly_new 
with (appendonly=true, orientation=column, compresstype=zlib) as 
select * 
from dw_staging.member_enrollment_monthly 
distributed by (uth_member_id)
;

drop table if exists dw_staging.member_enrollment_monthly;

alter table dw_staging.member_enrollment_monthly_new rename to member_enrollment_monthly;
--------/

--finalize
vacuum analyze dw_staging.member_enrollment_monthly;



---validate
select data_source, year, count(*)
from dw_staging.member_enrollment_monthly
group by data_source, year
order by data_source, year
;



----END SCRIPT-------------------------------------------------------