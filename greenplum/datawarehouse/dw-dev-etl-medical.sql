select *
from data_warehouse.medical
limit 1;

create table dev.medical_2010
as
select *
from data_warehouse.medical
where adm_dt between '2010-01-01' and '2010-12-31';

alter table dev.medical_2010 rename to medical_2007;

select sum(tot_chgs), avg(tot_chgs)
from dev.medical_2007;

select extract(year from adjud_date)as year, sum(tot_chgs), avg(tot_chgs)
from data_warehouse.medical
group by 1
order by 1;
