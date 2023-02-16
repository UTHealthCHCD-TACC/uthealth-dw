show all

select * from pg_catalog.pg_stat_operations where actionname='ANALYZE' order by statime desc

select count(distinct uth_member_id) as total_members,count(id) as total_rows
from data_warehouse.member_enrollment_monthly_2

select month_year_id, count(id)
from data_warehouse.member_enrollment_monthly_2
where data_source in('optz','optd')
group by month_year_id
order by month_year_id

select data_source, count(distinct uth_member_id) as total_members, count(id) as total_rows
from data_warehouse.member_enrollment_monthly_2
group by data_source
order by data_source

create index bus_cd_bmp_idx on data_warehouse.member_enrollment_monthly_2 using bitmap(bus_cd)

analyze data_warehouse.member_enrollment_monthly_2

select sum(sotaidtablesize)/1024/1024/1024 as TOTAL_TABLE_GB,  sum(sotaididxsize)/1024/1024/1024 as TOTAL_INDEX_GB 
from gp_toolkit.gp_size_of_table_and_indexes_disk 
where sotaidschemaname='data_warehouse'
and sotaidtablename = 'member_enrollment_monthly'

select count(id) from data_warehouse.member_enrollment_monthly

select month_year_id from data_warehouse.member_enrollment_monthly
where month_year_id>0
limit 100


select * from gp_toolkit.gp_table_indexes where titableschemaname='data_warehouse' and titablename='member_enrollment_monthly'

select * from gp_toolkit.gp_size_of_all_table_indexes

select sum(soisize)/1024/1024/1024 as SIZE_GB
from gp_toolkit.gp_size_of_index 
--where soiindexname like 'state_bmp_idx%'
where soitablename like 'member_enrollment_monthly%'
order by soisize desc

select * from gp_toolkit.gp_disk_free order by dfsegment

select * from gp_toolkit.gp_size_of_database





