/*
 * The enrollment_date table maps patient enrollments in a particular dataset to every month/year covered in the data
 */

-- Date Table
drop table data_warehouse.dim_date;
create table data_warehouse.dim_date as		
select TO_CHAR(datum,'yyyymmdd')::INT AS id,
       datum AS date,
       EXTRACT(MONTH FROM datum) AS month,
       TO_CHAR(datum,'Month') AS month_name
FROM (	  select datum
      FROM GENERATE_SERIES ('2007-01-01'::DATE, '2020-12-31'::DATE, '1 month') AS datum
) DQ
ORDER BY 1;

-- Add Columens
alter table data_warehouse.dim_date add column year smallint;
update data_warehouse.dim_date set year = extract(year from date);

--Map Enrollment to dim_date

create table data_warehouse.dim_enrollment_date (
id bigserial, date_id bigint, enrollment_id bigint
)
WITH (appendonly=true, orientation=column)
distributed randomly;

--Greenplum performance optimization for serial/sequence
alter sequence data_warehouse.dim_enrollment_date_id_seq cache 200;

--Insert
alter table data_warehouse.dim_enrollment_date rename to dim_enrollment_date_backup;


insert into data_warehouse.dim_enrollment_date(date_id, enrollment_id)
select d.id, e.id
from data_warehouse.dim_date d
join data_warehouse.enrollment e on d.date between e.cov_eff_dt and e.cov_term_dt
where e.source='t'
and d.year >= 2016;

and 
limit 10;

--Verify
select e.source, extract(year from e.cov_eff_dt) as year, count(*)
from data_warehouse.enrollment e
group by 1, 2; 

select e.source, count(*), min(d.year), max(d.year)
from data_warehouse.dim_enrollment_date ed
join data_warehouse.enrollment e on e.id=ed.enrollment_id
join data_warehouse.dim_date d on d.id=ed.date_id
group by 1;

-- Test Queries
select d.month, d.year, e.source, count(*) as mbr_cnt, 
sum(case when e.gndr_cd='F' then 1 else 0 end) as num_females, 
sum(case when e.gndr_cd='M' then 1 else 0 end) as num_males, 
avg(2013-e.mbr_dob) as avg_age
from data_warehouse.enrollment e	
join data_warehouse.dim_enrollment_date ed on e.id=ed.enrollment_id
join data_warehouse.dim_date d on d.id=ed.date_id
where d.year=2013
group by 1, 2, 3
order by d.year, d.month, e.source;


drop table dev.enrollment__date_2013;
create table dev.enrollment__date_2013 
as (
select ed.enrollment_id, ed.date_id, d.*
from data_warehouse.dim_enrollment_date ed
join data_warehouse.dim_date d on d.id=ed.date_id
where d.year=2013)
distributed randomly;

select ed.month, ed.year, e.source, count(*) as mbr_cnt, count(distinct e.mbr_id) as cnt2
from data_warehouse.enrollment e	
join dev.enrollment__date_2013 ed on e.id=ed.enrollment_id
group by 1, 2, 3
order by ed.year, ed.month, e.source;

select distinct plan_ty_cd
from data_warehouse.enrollment;


select *
from data_warehouse.enrollment
where source = 't'
limit 10;