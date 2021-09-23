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
*/


----  // BEGIN SCRIPT 


---create working table in dw_staging 
drop table if exists dw_staging.member_enrollment_monthly ;

create table dw_staging.member_enrollment_monthly  (
	data_source char(4),
	year int2, 
	uth_member_id bigint,
	month_year_id int4, 
	consecutive_enrolled_months int4, 
	gender_cd char(1), 
	race_cd char(1),
	age_derived int4, 
	dob_derived date, 
	state text, 
	zip5 char(5), 
	zip3 char(3), 
	death_date date, 
	plan_type text, 
	bus_cd char(4), 
	employee_status text, 
	claim_created_flag boolean default false,
	rx_coverage int2, 
	fiscal_year int2,
	row_id bigserial
) distributed by (row_id);

                                                                        
alter sequence dw_staging.member_enrollment_monthly_row_id_seq cache 200;


-------------insert existing records from data warehouse. except for this data source
insert into dw_staging.member_enrollment_monthly 
select * 
from data_warehouse.member_enrollment_monthly 
where data_source not in ('truv')
;

vacuum analyze dw_staging.member_enrollment_monthly;

------ **** Truven *******

--copy of uth id table distributed on member id src 
create table dw_staging.truven_uth_member_id
with(appendonly=true,orientation=column)
as select *
from data_warehouse.dim_uth_member_id where data_source = 'truv'
distributed by(member_id_src);

vacuum analyze dw_staging.truven_uth_member_id;

--(---------------- data loads --------------------)

-- Truven Commercial ----------------------------------------------------------------------------
-- 9/2/21 runtime 58min
insert into dw_staging.member_enrollment_monthly  (
	data_source, year, month_year_id, uth_member_id,
	gender_cd, state, zip5, zip3,
	age_derived, dob_derived, death_date,
	plan_type, bus_cd, employee_status, rx_coverage, fiscal_year , race_cd        
	)		
select 
	   'truv', b.year_int, b.month_year_id, a.uth_member_id, 
       c.gender_cd, case when length(s.abbr) > 2 then '' else s.abbr end, null, rpad((trunc(m.empzip,0)::text),3,'0'),
       b.year_int - dobyr, (trunc(dobyr,0)::varchar || '-12-31')::date, null, 
       d.plan_type, 'COM', eestatu, m.rx, m.year , '0' as race
from truven.ccaet m
  join dw_staging.truven_uth_member_id a
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
	data_source, year, month_year_id, uth_member_id,
	gender_cd, state, zip5, zip3,
	age_derived, dob_derived, death_date,
	plan_type, bus_cd, employee_status, rx_coverage , fiscal_year, race_cd     
	)		
select 
       'truv', b.year_int,b.month_year_id, a.uth_member_id,
       c.gender_cd, case when length(s.abbr) > 2 then '' else s.abbr end, null, rpad((trunc(m.empzip,0)::text),3,'0'),
       b.year_int - dobyr, (trunc(dobyr,0)::varchar || '-12-31')::date, null,
       d.plan_type, 'MCR', eestatu, m.rx, m.year, '0' as race 
from truven.mdcrt m
  join dw_staging.truven_uth_member_id a
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

---cleanup
drop table dw_staging.truven_uth_member_id;

--vacuum
vacuum analyze dw_staging.member_enrollment_monthly;

--validate
select count(*), data_source, year 
from dw_staging.member_enrollment_monthly mem 
group by data_source, year  
order by data_source, year 



----/END SCRIPT