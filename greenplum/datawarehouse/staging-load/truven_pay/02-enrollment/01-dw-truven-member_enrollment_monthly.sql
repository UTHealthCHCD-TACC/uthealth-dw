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
 * ******************************************************************************************************
 *  wc004  || 11/05/2021 || updates for bus cd based on new medadv column
 * ******************************************************************************************************
 *  wc005  || 11/06/2021 || moved table creation to new script. formatting. changed bus_cd mapping
 *  ******************************************************************************************************
 *  jwozny || 01/15/2023 || Split Truven-specific script off + updated
 *  ******************************************************************************************************
 *  xrzhang || 03/20/2023 || Edited script to include MSA column
 * 
 * 
*/


/*
 * Because there are several billion rows, we need to redistribute the enrollment table on a key with the right data type to speed up the join with dim in the DW
 */

drop table if exists staging_clean.ccaet;

--note that there are no NULL enrolids in ccaet
create table staging_clean.ccaet with (
	appendonly=true,
	orientation=column,
	compresstype=zlib
) 
as select m.enrolid::text,
       m.medadv,
       m.rx,
       m.efamid,
       m.mhsacovg,
       m.egeoloc,
       m.dtstart,
       m.sex,
       m.dtend,
       m.plantyp,
       m.empzip,
       m.dobyr,
       m.eestatu ,
       m.year, 
       m.msa
  from truven.ccaet m
  distributed by (enrolid)
  ;
 
 analyze staging_clean.ccaet;

-- Truven Commercial ----------------------------------------------------------------------------
insert into dw_staging.member_enrollment_monthly  (
	data_source, 
	year, 
	month_year_id, 
	uth_member_id,
	gender_cd, 
	state, 
	zip5, 
	zip3,
	msa,
	age_cy, 
	dob_derived, 
	death_date,
	plan_type, 
	bus_cd, 
	employee_status, 
	rx_coverage, 
	fiscal_year, 
	race_cd,
	family_id,
	behavioral_coverage,
	load_date,
	table_id_src,
	member_id_src
	)		
select 'truv', 
       m.year,
       (extract (year from m.dtstart)::text || lpad(extract (month from m.dtstart)::text , 2, '0'))::int, 
       a.uth_member_id, 
       c.gender_cd, 
       case when length(s.abbr) > 2 then '' else s.abbr end, null, rpad((trunc(m.empzip,0)::text),3,'0'), 
       m.msa,
       m.year - dobyr as age_cy, 
       (trunc(dobyr,0)::varchar || '-12-31')::date as dob_derived, 
       null, 
       d.plan_type, 
       case when m.medadv = '1' then 'MA' else 'COM' end as bus_cd, 
       eestatu, 
       m.rx, 
       null, 
       '0' as race, 
       m.efamid::text, 
       coalesce(m.mhsacovg,0),
       current_date,
       'ccaet',
       enrolid 
  from staging_clean.ccaet m
  join data_warehouse.dim_uth_member_id  a 
    on a.member_id_src = m.enrolid
 and a.data_source = 'truv'
  left outer join reference_tables.ref_truven_state_codes s 
    on m.egeoloc=s.truven_code
  left outer join reference_tables.ref_gender c
    on c.data_source = 'trv' 
   and c.gender_cd_src = m.sex::text 
  left outer join reference_tables.ref_plan_type d 
    on d.data_source = 'trv' 
  and d.plan_type_src::int = m.plantyp  
;

 analyze dw_staging.member_enrollment_monthly_1_prt_truv;
---------------------------------------------------------------------------------------------------

/*
 * Create re-distributed source table 
 */
drop table if exists staging_clean.mdcrt;

--note that there are no NULL enrolids in mdcrt
create table staging_clean.mdcrt with (
	appendonly=true,
	orientation=column,
	compresstype=zlib
) 
as select m.enrolid::text,
       m.medadv,
       m.rx,
       m.efamid,
       m.mhsacovg,
       m.egeoloc,
       m.dtstart,
       m.sex,
       m.dtend,
       m.plantyp,
       m.empzip,
       m.dobyr,
       m.eestatu ,
       m.year,
       m.msa
  from truven.mdcrt m
  distributed by (enrolid)
  ;
 
analyze staging_clean.mdcrt;

-- Truven Medicare Advantage ----------------------------------------------------------------------
insert into dw_staging.member_enrollment_monthly  (
	data_source, 
	year, 
	month_year_id, 
	uth_member_id,
	gender_cd, 
	state, 
	zip5, 
	zip3,
	msa,
	age_cy, 
	dob_derived, 
	death_date,
	plan_type, 
	bus_cd, 
	employee_status, 
	rx_coverage, 
	fiscal_year, 
	race_cd,
	family_id,
	behavioral_coverage,
	load_date,
	table_id_src,
	member_id_src
	)	
select 
       'truv', 
       m.year,
       (extract (year from m.dtstart)::text || lpad(extract (month from m.dtstart)::text , 2, '0'))::int, 
       a.uth_member_id,
       c.gender_cd, 
       case when length(s.abbr) > 2 then '' else s.abbr end, null, rpad((trunc(m.empzip,0)::text),3,'0'),
       m.msa,
       m.year - dobyr as age_cy, 
       (trunc(dobyr,0)::varchar || '-12-31')::date as dob_derived, 
       null,
       d.plan_type, 
       case when m.medadv = '1' then 'MA' else 'MS' end as bus_cd, 
       eestatu, 
       m.rx, 
       null,
       '0' as race,
       m.efamid::text, 
       coalesce(m.mhsacovg,0),
       current_date,
       'mdcrt',
       enrolid 
  from staging_clean.mdcrt m
  join data_warehouse.dim_uth_member_id a 
    on a.member_id_src = m.enrolid
   and a.data_source = 'truv'
  left outer join reference_tables.ref_truven_state_codes s 
	on m.egeoloc=s.truven_code
  left outer join reference_tables.ref_gender c
    on c.data_source = 'trv'
   and c.gender_cd_src = m.sex::text
  left outer join reference_tables.ref_plan_type d
    on d.data_source = 'trv'
  and d.plan_type_src::int = m.plantyp
;

/*
 * Get consecutive enrollment 
 */

analyze dw_staging.member_enrollment_monthly_1_prt_truv;

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
	   from dw_staging.member_enrollment_monthly_1_prt_truv 	 a 
	     join reference_tables.ref_month_year b 
	       on a.month_year_id = b.month_year_id 	   		    
	 ) inr    
distributed by (row_id);

analyze dev.temp_consec_enrollment;

--update consec enrolled months  9m 
update dw_staging.member_enrollment_monthly_1_prt_truv a 
   set consecutive_enrolled_months = b.in_streak 
  from dev.temp_consec_enrollment b 
 where a.row_id = b.row_id
;

--**cleanup
drop table if exists dev.temp_consec_enrollment;
drop table if exists staging_clean.mdcrt;
drop table if exists staging_clean.ccaet;

alter table dw_staging.member_enrollment_monthly drop column row_id;

vacuum analyze dw_staging.member_enrollment_monthly;





