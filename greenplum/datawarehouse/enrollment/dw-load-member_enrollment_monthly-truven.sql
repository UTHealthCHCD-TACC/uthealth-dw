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

------ **** Truven *******
delete from dw_staging.member_enrollment_monthly where data_source = 'truv';

create table dev.truven_uth_mem
with(appendonly=true,orientation=column)
as select *
from data_warehouse.dim_uth_member_id where data_source = 'truv'
distributed by(member_id_src);

vacuum analyze dev.truven_uth_mem;


create table truven.ccaet_temp 
with (appendonly=true, orientation=column) as 
select * 
from truven.ccaet
distributed by (enrolid)
;

vacuum analyze truven.ccaet_temp;


select count(*), year 
from optum_zip.medical 
group by year 
order by year 
;

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
from truven.ccaet_temp m
  join dev.truven_uth_mem a --join data_warehouse.dim_uth_member_id a
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
  join dev.truven_uth_mem a  --join data_warehouse.dim_uth_member_id a
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

drop table dev.truven_uth_mem;


vacuum analyze dw_staging.member_enrollment_monthly;

select count(*), data_source, year 
from dw_staging.member_enrollment_monthly mem 
group by data_source, year  
order by data_source, year 


-------(^---------------- data loads --------------------^)

----- *** End Truven **** -----

----/END SCRIPT