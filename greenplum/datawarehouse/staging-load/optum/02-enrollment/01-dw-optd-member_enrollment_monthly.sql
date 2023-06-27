/* ******************************************************************************************************
 *  The member_enrollment_monthly table creates one record for each month/year that a member was enrolled in coverage
 *  Run the relevant code section for the dataset in (---------------- data loads --------------------)
 * 
 *  !!!!!!!!!  data_warehouse.dim_member_id_src table must be populated first !!!!!!!!!    
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
 *  wc004  || 11/06/2021 || moved table creation to new script. formatting. changed bus_cd mapping
 *  ******************************************************************************************************
 *  iperez  || 6/26/2023 || split optum_zip and optum_dod into seperate sql scripts.
 *  ******************************************************************************************************
*/



----  // BEGIN SCRIPT 


drop table if exists dw_staging.optd_member_enrollment_monthly;

create table dw_staging.optd_member_enrollment_monthly  
(like data_warehouse.member_enrollment_monthly including defaults) 
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
distributed by (uth_member_id)
;

alter table dw_staging.optd_member_enrollment_monthly add column row_id bigserial;
alter sequence dw_staging.optd_member_enrollment_monthly_row_id_seq cache 200;

-- ***** Optum DOD ***** --------------------------------------------------------------------------------------
insert into dw_staging.optd_member_enrollment_monthly  (
	data_source, 
	year, 
	month_year_id, 
	uth_member_id,
	gender_cd, 
	state, 
	zip5, 
	zip3,
	age_cy, 
	dob_derived, 
	death_date,
	plan_type, 
	bus_cd, 
	rx_coverage, 
	fiscal_year, 
	race_cd,
	family_id,
	load_date,
	table_id_src,
	member_id_src
	)
select 'optd', 
	   b.year_int, 
	   b.month_year_id, 
	   a.uth_member_id,
       c.gender_cd, 
       m.state, 
       null, 
       null, 
       b.year_int - yrdob, 
       case when yrdob = 0 then null else (yrdob::varchar || '-12-31')::date end as birth_dt, 
       case when death_ym is null then null else to_date(death_ym::text,'YYYYMM')  end as death_dt,  
       d.plan_type, 
       case when bus = 'MCR' then 'MA' when bus = 'COM' then 'COM' else 'UNK' end as bus_cd, 
       1 as rx, 
       b.fy_ut, 
       r.race_cd,
       m.family_id::text,
       current_date,
       'mbr_enroll',
       m.patid
from optum_dod.mbr_enroll_r m
  join data_warehouse.dim_uth_member_id a
    on a.member_id_src = m.member_id_src 
   and a.data_source = 'optd'
  left outer join optum_dod.mbrwdeath dth 
    on dth.patid = m.patid 
  join reference_tables.ref_month_year b
    on b.start_of_month between date_trunc('month', m.eligeff) 
                        and case when dth.death_ym is not null then to_date(death_ym::text,'YYYYMM') 
                            else m.eligend 
                            end   ---wcc002
  left outer join reference_tables.ref_gender c
    on c.data_source = 'opt'
   and c.gender_cd_src = m.gdr_cd 
  left outer join reference_tables.ref_plan_type d
    on d.data_source = 'opt'
   and d.plan_type_src = m.product
  left outer join reference_tables.ref_race r 
    on r.race_cd_src = m.race 
   and r.data_source = 'optd'
;

---- get row id of duplicate rows
drop table if exists dw_staging.temp_optd_dupe_enrollment_rows;

create table dw_staging.temp_optd_dupe_enrollment_rows
with (appendonly=true, orientation=column) as 
select row_id::bigint as row_id 
from
(		
select row_number() over(partition by uth_member_id, month_year_id order by month_year_id) as rn
	      ,*
	from dw_staging.optd_member_enrollment_monthly
) sub
where rn > 1
distributed by (row_id);	

--remove dupe records so only one per member per month - runtime: 12min
delete from dw_staging.optd_member_enrollment_monthly a 
  using dw_staging.temp_optd_dupe_enrollment_rows b 
   where a.row_id = b.row_id 
; 

---**script to build consecutive enrolled months	
drop table if exists dw_staging.temp_optd_consec_enrollment;

create table dw_staging.temp_optd_consec_enrollment 
with (appendonly=true, orientation=column) as 
select month_year_id, uth_member_id
      ,row_number() over(partition by uth_member_id, my_grp order by  month_year_id) as in_streak
from ( 
	   select a.month_year_id,
	        a.uth_member_id,
	        b.my_row_counter - row_number() over(partition by a.uth_member_id
	        	order by a.month_year_id) as my_grp
	   from dw_staging.optd_member_enrollment_monthly a 
	     join reference_tables.ref_month_year b 
	       on a.month_year_id = b.month_year_id 	   		    
	 ) t    
distributed by (month_year_id);

analyze dw_staging.temp_optd_consec_enrollment;

--update consec enrolled months
update dw_staging.optd_member_enrollment_monthly a 
   set consecutive_enrolled_months = b.in_streak 
  from dw_staging.temp_optd_consec_enrollment b 
 where a.month_year_id = b.month_year_id and a.uth_member_id = b.uth_member_id;

--**cleanup
drop table if exists dw_staging.temp_optd_consec_enrollment;

alter table dw_staging.optd_member_enrollment_monthly drop column row_id;

--vacuum analyze
vacuum analyze dw_staging.optd_member_enrollment_monthly;

-- updating age in months and fy age

update dw_staging.optd_member_enrollment_monthly
   set age_months = extract(years from age(to_date(month_year_id::text, 'YYYYMM'), dob_derived)) * 12 + 
	   				extract(months from age(to_date(month_year_id::text, 'YYYYMM'), dob_derived))
;

update dw_staging.optd_member_enrollment_monthly a
   set age_fy = extract(years from age(c.fy_end, dob_derived))
 from reference_tables.ref_month_year c
 where a.month_year_id = c.month_year_id
;

--vacuum analyze
vacuum analyze dw_staging.optd_member_enrollment_monthly;