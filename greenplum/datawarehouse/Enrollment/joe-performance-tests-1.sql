select * from pg_catalog.pg_stat_activity

select * from gp_toolkit.gp_size_of_table_disk where sotdschemaname='data_warehouse'

analyze data_warehouse.member_enrollment_monthly_2
explain analyze 
select data_source, month_year_id,gender_cd, count(distinct uth_member_id) 
from data_warehouse.member_enrollment_monthly_2
where month_year_id='2015-06' and not data_source='optd'
group by data_source, month_year_id, gender_cd
order by data_source, month_year_id, gender_cd

create index gender_bmp_idx on data_warehouse.member_enrollment_monthly_2 using bitmap (gender_cd)
create index age_idx on data_warehouse.member_enrollment_monthly_2 (age_derived)


explain analyze select count(id) from data_warehouse.member_enrollment_monthly

select * from gp_toolkit.gp_size_of_table_and_indexes_disk
where sotaidschemaname='data_warehouse'

select gp_segment_id, count(*) from data_warehouse.member_enrollment_monthly group by gp_segment_id

select data_source, month_year_id, gender_code,
case
	when age<17 then 1
	when age between 18 and 29 then 2
	when age between 30 and 39 then 3
	when age between 40 and 49 then 4
	when age between 50 and 59 then 5
	when age between 60 and 65 then 6
	else 7
end as AGEGRP, 

