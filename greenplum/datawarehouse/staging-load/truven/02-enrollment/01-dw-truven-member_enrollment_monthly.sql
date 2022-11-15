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
*/


----  // BEGIN SCRIPT 
analyze dw_staging.member_enrollment_monthly;

select count(*), data_source, year from dw_staging.member_enrollment_monthly group by 2,3 order by 2,3;

-- Truven Commercial ----------------------------------------------------------------------------
-- 11/7/21 runtime 53min
insert into dw_staging.member_enrollment_monthly  (
	data_source, 
	year, 
	month_year_id, 
	uth_member_id,
	gender_cd, 
	state, 
	zip5, 
	zip3,
	age_derived, 
	dob_derived, 
	death_date,
	plan_type, 
	bus_cd, 
	employee_status, 
	rx_coverage, 
	fiscal_year, 
	race_cd,
	family_id,
	behavioral_coverage
	)		
select 'truv', 
       b.year_int,
       b.month_year_id, 
       a.uth_member_id,
       c.gender_cd, 
       case when length(s.abbr) > 2 then '' else s.abbr end, null, rpad((trunc(m.empzip,0)::text),3,'0'),
       b.year_int - dobyr as age_derived, 
       (trunc(dobyr,0)::varchar || '-12-31')::date as dob_derived, 
       null,
       d.plan_type, 
       case when m.medadv = '1' then 'MA' else 'COM' end as bus_cd, 
       eestatu, 
       m.rx, 
       b.fy_ut,
       '0' as race,
       m.efamid::text, 
       m.mhsacovg
from truven.ccaet m
  join data_warehouse.dim_uth_member_id a 
    on a.member_id_src = m.enrolid::text
   and a.data_source = 'truv'
  join reference_tables.ref_truven_state_codes s 
    on m.egeoloc=s.truven_code
  join reference_tables.ref_month_year b 
    on b.start_of_month between date_trunc('month', m.dtstart) and m.dtend
  left outer join reference_tables.ref_gender c
    on c.data_source = 'trv'
   and c.gender_cd_src = m.sex::text
  left outer join reference_tables.ref_plan_type d
    on d.data_source = 'trv'
  and d.plan_type_src::int = m.plantyp  
;
---------------------------------------------------------------------------------------------------



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
	age_derived, 
	dob_derived, 
	death_date,
	plan_type, 
	bus_cd, 
	employee_status, 
	rx_coverage, 
	fiscal_year, 
	race_cd,
	family_id,
	behavioral_coverage
	)	
select 
       'truv', 
       b.year_int,
       b.month_year_id, 
       a.uth_member_id,
       c.gender_cd, 
       case when length(s.abbr) > 2 then '' else s.abbr end, null, rpad((trunc(m.empzip,0)::text),3,'0'),
       b.year_int - dobyr as age_derived, 
       (trunc(dobyr,0)::varchar || '-12-31')::date as dob_derived, 
       null,
       d.plan_type, 
       case when m.medadv = '1' then 'MA' else 'MS' end as bus_cd, 
       eestatu, 
       m.rx, 
       b.fy_ut,
       '0' as race,
       m.efamid::text, 
       m.mhsacovg
from truven.mdcrt m
  join data_warehouse.dim_uth_member_id a 
    on a.member_id_src = m.enrolid::text
   and a.data_source = 'truv'
  join reference_tables.ref_truven_state_codes s 
	on m.egeoloc=s.truven_code
  join reference_tables.ref_month_year b
    on b.start_of_month between date_trunc('month', m.dtstart) and m.dtend
  left outer join reference_tables.ref_gender c
    on c.data_source = 'trv'
   and c.gender_cd_src = m.sex::text
  left outer join reference_tables.ref_plan_type d
    on d.data_source = 'trv'
  and d.plan_type_src::int = m.plantyp
;
---------------------------------------------------------------------------------------------------


--validate
select count(*), data_source, year 
from dw_staging.member_enrollment_monthly 
group by data_source, year  
order by data_source, year 
;


----/END SCRIPT