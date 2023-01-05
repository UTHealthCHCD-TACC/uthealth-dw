
----------------- GET ERS TRS COMMERICAL COVID SEVERITY TO INSERT INTO DW ---------------

drop table if exists test.dbo.zip_county_distinct;

select distinct [ZIP Code], County 
into test.dbo.zip_county_distinct
from [REF].dbo.zip_county_dis_tx
;

----------------------------------------------------

drop table if exists test.dbo.demo_leg_covid_ers_trs;

select * 
into test.dbo.demo_leg_covid_ers_trs
from (
select 'ERS' as data_source, a.ID, a.severity, 
		b.AGE as age_derived, GEN as gender_cd, b.ZIP5 as zip5, c.county,
		case
			when b.AGE between 0 and 19 then 1
			when b.AGE between 20 and 34 then 2
			when b.AGE between 35 and 44 then 3
			when b.AGE between 45 and 54 then 4
			when b.AGE between 55 and 64 then 5
			when b.AGE between 65 and 74 then 6
			when b.AGE >=75 then 7 
		end as age_group
from TRSERS.dbo.ERS_COVID_SEVERITY_new a 
join TRSERS.dbo.ERS_AGG_CY b 
  on a.ID = b.ID 
join test.dbo.zip_county_distinct c
  on b.ZIP5 = c.[ZIP Code]
where b.CY = 2020
union all 
select 'TRS' as data_source, a.combo_id , a.severity, 
		b.AGE as age_derived, GEN as gender_cd, b.ZIP5 as zip5, c.county,
		case
			when b.AGE between 0 and 19 then 1
			when b.AGE between 20 and 34 then 2
			when b.AGE between 35 and 44 then 3
			when b.AGE between 45 and 54 then 4
			when b.AGE between 55 and 64 then 5
			when b.AGE between 65 and 74 then 6
			when b.AGE >=75 then 7 
		end as age_group
from TRSERS.dbo.TRS_COVID_SEVERITY_new a 
join TRSERS.dbo.TRS_AGG_CY b 
  on a.combo_id  = b.combo_id  
join test.dbo.zip_county_distinct c
  on b.ZIP5 = c.[ZIP Code]
where b.CY = 2020
) a
;
 --------------------



select * from test.dbo.demo_leg_covid_ers_trs;














