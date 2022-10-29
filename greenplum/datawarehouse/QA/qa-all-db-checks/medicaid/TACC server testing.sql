



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