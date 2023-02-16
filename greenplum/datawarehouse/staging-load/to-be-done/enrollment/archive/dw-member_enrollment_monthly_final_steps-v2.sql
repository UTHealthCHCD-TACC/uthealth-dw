/* ******************************************************************************************************
 *  The script should be run as the final step once dw_staging.member_enrollment_monthly has been updated
 * 	
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wc001  || 1/01/2021 || script created 
 * ******************************************************************************************************
 * ******************************************************************************************************
 *  wc002  || 9/30/2021 || add compression level to final table
 *  ******************************************************************************************************
 *  wc003  || 11/11/2021 || write as one script
 *  ******************************************************************************************************
 *  wc004  || 01/26/2022 || add partitions
 *  ******************************************************************************************************
*/

--runtime 1/31/2022 38minutes



---- get row id of duplicate rows
--drop table if exists  dev.temp_dupe_enrollment_rows;


---**script to build consecutive enrolled months	  10min	
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

analyze dev.temp_consec_enrollment;



--update consec enrolled months  9m 
update dw_staging.member_enrollment_monthly a 
   set consecutive_enrolled_months = b.in_streak 
  from dev.temp_consec_enrollment b 
 where a.row_id = b.row_id
;

------------------------fix birthdays ----------------------
/*
 drop table if exists dev.birth_dupes1 ;
 
  select uth_member_id, dob_derived, count(*) as d_count, max(month_year_id) as recent 
    into dev.birth_dupes1 
    from dw_staging.member_enrollment_monthly 
   group by uth_member_id, dob_derived ;
  
  
  drop table if exists dev.birth_dupes2;
    
   select *,
  	     row_number() over (partition by uth_member_id order by d_count desc, recent desc) as dob_row
    into dev.birth_dupes2
  	from dev.birth_dupes1;
  
  drop table if exists dev.birth_dupes3;
  
  select uth_member_id, dob_derived 
    into dev.birth_dupes3
  	from dev.birth_dupes2
   where dob_row = 1;
  
  update dw_staging.member_enrollment_monthly a
    set dob_derived = b.dob_derived 
   from dev.birth_dupes3 b 
  where a.uth_member_id = b.uth_member_id ;
  
 vacuum analyze dw_staging.member_enrollment_monthly;
 drop table if exists dev.birth_dupes3;
 drop table if exists dev.birth_dupes2;
 drop table if exists dev.birth_dupes1 ;
*/
------------------------------------------------------------
--**cleanup
drop table if exists dev.temp_consec_enrollment;
---/drop sequence, rebuild table distributed on uth_member_id 
alter table dw_staging.member_enrollment_monthly drop column row_id;
vacuum full analyze dw_staging.member_enrollment_monthly;
alter table dw_staging.member_enrollment_monthly owner to uthealth_dev;



