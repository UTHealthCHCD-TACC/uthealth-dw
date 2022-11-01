



select * from dw_staging.member_enrollment_monthly limit 5;

--select random rows
select a.member_id_src, a.year, a.total_enrolled_months, a.gender_cd, a.race_cd, b.race_cd_src,
a.age_derived, a.dob_derived, a.zip5, a.plan_type, a.dual, a.htw
from dw_staging.member_enrollment_yearly a left join reference_tables.ref_race b
	on a.gender_cd = b.race_cd
--where b.data_source = 'mdcd'
--order by random()
limit 5;

with b as (select race_cd_src, race_cd from reference_tables.ref_race
	where data_source = 'mdcd' and race_cd_src is not null)

select a.data_source, a.member_id_src, a.year, a.total_enrolled_months, a.gender_cd, a.race_cd, 
b.race_cd_src,
a.age_derived, a.dob_derived, a.zip5, a.plan_type, a.dual, a.htw
from dw_staging.member_enrollment_yearly a left join b
	on a.race_cd = b.race_cd
limit 30;

select * from reference_tables.ref_race where data_source = 'mdcd';
select * from dw_staging.member_enrollment_yearly limit 5;

select * from dw_staging.member_enrollment_monthly where member_id_src = '700024111' and year = 2018
order by month_year_id ;

select * from dw_staging.member_enrollment_yearly where member_id_src = '526423364' and fiscal_year = 2014;

with b as (select race_cd_src, race_cd from reference_tables.ref_race
where data_source = 'mdcd' and race_cd_src is not null)

select a.data_source, a.member_id_src, a.year, a.gender_cd, b.race_cd_src,
a.zip5, a.plan_type
from dw_staging.member_enrollment_yearly a left join b
on a.race_cd = b.race_cd
where member_id_src = '501173880' and year = '2014'



select *
from dw_staging.member_enrollment_monthly
where member_id_src = '720920939' and year = '2020'

select *
from dw_staging.member_enrollment_yearly
where member_id_src = '720920939' and year = '2020'

/*********************************************
 *  Test code for starting on SPC side going to TACC side
 * *******************************************/

drop table if exists dev.xz_mcd_enrl_mismatches;

with c as (select race_cd_src, race_cd from reference_tables.ref_race
	where data_source = 'mdcd' and race_cd_src is not null)
select b.mem_id as member_id, b.cy as year,
	case when a.member_id_src is null then 1 else 0 end as member_id_mismatch,
	case when a.total_enrolled_months != b.enrolled_months then 1 else 0 end as em_mismatch,
	case when a.gender_cd != b.sex then 1 else 0 end as sex_mismatch,
	case when c.race_cd_src != b.race and b.race is not null then 1 else 0 end as race_mismatch,
	case when a.dob_derived != b.dob::date then 1 else 0 end as dob_mismatch,
	case when a.zip5 != b.zip then 1 else 0 end as zip_mismatch,
	case when trim(a.plan_type) != trim(b.mco) then 1 else 0 end as plan_mismatch,
	case when a.dual::text !=  b.smib then 1 else 0 end as dual_mismatch,
	case when a.htw != b.htw then 1 else 0 end as htw_mismatch,
	a.gender_cd as tacc_sex, b.sex as spc_sex,
	c.race_cd_src as tacc_race, b.race as spc_race,
	a.zip5 as tacc_zip, b.zip as spc_zip,
	a.plan_type as tacc_plan, b.mco as spc_plan, 
	a.dob_derived as tacc_dob, b.dob as spc_dob
into dev.xz_mcd_enrl_mismatches
from dev.xz_dwqa_temp1 b
	left join dw_staging.member_enrollment_yearly a
	on a.member_id_src = b.mem_id and a.year::text = b.cy
	left join c
	on a.race_cd = c.race_cd;

select * from dev.xz_mcd_enrl_mismatches where dob_mismatch = 1;

select * from dw_staging.member_enrollment_monthly where member_id_src = '701644512';


