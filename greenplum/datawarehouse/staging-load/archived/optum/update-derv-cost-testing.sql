/*******************************************************************
 * Update derived cost for Optum
 * 
 * Optum tables come with standardized cost (std_cost) and year of standardized cost (std_cost_yr)
 * std_cost is an estimate of the allowed amount and depends on the year of standardization.
 * 
 * To obtain the derived cost, we multiply the standardized cost by its cost_factor,
 * which is obtained from reference_tables.ref_optum_cost_factor and depends on
 * year and service type.
 * 
 * Medical tables contain a mix of service types, whereas Confinement tables are strictly
 * inpatient facility (FAC_IP) charges and rx tables are strictly pharmacy charges.
 * 
 *******************************************************************/

--we have some missing tos_cds in medical. How many?

select year, count(case when tos_cd is null then 1 end) as null_tos_cd,
	count(*) as count,
	count(case when tos_cd is null then 1 end)/ count(*)::float as null_tos_pct
from optum_zip.medical
group by year
order by year;

--check to see if zeroes and nulls evaluaate properly
--answer: yes they do
select * from optum_zip.medical where std_cost is null; --nulls exist
select * from optum_zip.medical where std_cost = 0; --zeroes exist

select round((null * 1.13)::numeric, 2); --evaluates to null
select round((0 * 1.13)::numeric, 2); --evaluates to 0

--Make testing table
drop table if exists dev.xz_optum_derv_cost_test;

create table dev.xz_optum_derv_cost_test as
select a.year, a.patid, a.clmid, a.tos_cd, a.std_cost, a.std_cost_yr
from optum_zip.medical a left join optum_zip.mbr_enroll b
on a.year = extract(year from b.eligeff) and a.patid = b.patid
where b.zipcode_5 like '7%'
order by random()
limit 100;

select * from dev.xz_optum_derv_cost_test order by year desc;

--make second testing table
drop table if exists dev.xz_optum_derv_cost_test2;

create table dev.xz_optum_derv_cost_test2 as
select * from dev.xz_optum_derv_cost_test;

alter table dev.xz_optum_derv_cost_test2
add column service_type text,
add column derv_cost numeric,
add column derv_cost_yr int2;

update dev.xz_optum_derv_cost_test2
	set service_type = case
	when tos_cd like 'ANC%' then 'ANC'
	when tos_cd like 'FAC_IP%' then 'FAC_IP'
	when tos_cd like 'FAC_OP%' then 'FAC_OP'
	when tos_cd like 'PROF%' then 'PROF'
else null end;

select * from dev.xz_optum_derv_cost_test2 order by year desc;

update dev.xz_optum_derv_cost_test2 a
	set derv_cost = round((a.std_cost * b.cost_factor)::numeric, 2),
	derv_cost_yr = 2021
from reference_tables.ref_optum_cost_factor b
where a.std_cost_yr::int = b.standard_price_year and a.service_type = b.service_type;

--Final table
select * from dev.xz_optum_derv_cost_test2 order by year desc;

2021	560499411677847	ONORRJ3J3L         	FAC_OP.FO_LAB	60.94	2021	FAC_OP	60.94	current year
2016	560499806018381	NO3JOF8LNN         	PROF.PHYMED  	78.88	2020	PROF	80.93	current year
2015	560499278726951	3V8N98NN9O         	FAC_OP.FO_OTH	5.43	2019	FAC_OP	5.77	current year

/********************
 * SPC-side code
 * 
 * Check to see if the derv_cost lines up
 *******************/
select PATID, CLMID, TOS_CD, STD_COST, STD_COST_YR, DERV_COST, DERV_YR from OPT_ZIP_TX.dbo.Zip_Medical_2021
where PATID = '560499411677847' and CLMID = 'ONORRJ3J3L' and TOS_CD = 'FAC_OP.FO_LAB';
560499411677847	ONORRJ3J3L	FAC_OP.FO_LAB	60.94	2021	60.9400000000000	2021

select PATID, CLMID, TOS_CD, STD_COST, STD_COST_YR, DERV_COST, DERV_YR from OPT_ZIP_TX.dbo.Zip_Medical_2016
where PATID = '560499806018381' and CLMID = 'NO3JOF8LNN' and TOS_CD = 'PROF.PHYMED';
560499806018381	NO3JOF8LNN	PROF.PHYMED	78.88	2020	80.9308800000000	2021

select PATID, CLMID, TOS_CD, STD_COST, STD_COST_YR, DERV_COST, DERV_YR from OPT_ZIP_TX.dbo.Zip_Medical_2015
where PATID = '560499278726951' and CLMID = '3V8N98NN9O' and TOS_CD = 'FAC_OP.FO_OTH';
560499278726951	3V8N98NN9O	FAC_OP.FO_OTH	5.43	2019	5.7717424800000	2021


--PASS