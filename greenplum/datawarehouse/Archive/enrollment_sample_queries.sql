--Audit Queries
select e.source, count(e.id) as cnt, min(d.date) as min_date, max(d.date) as max_date 
from data_warehouse.enrollment e
join data_warehouse.dim_enrollment_date ed on e.id=ed.enrollment_id
join data_warehouse.dim_date d on d.id=ed.date_id
group by 1;


select e.source, count(e.id) as cnt, min(e.cov_eff_dt) as min_cov_eff_dt, max(e.cov_term_dt) as max_cov_term_dt
from data_warehouse.enrollment e
group by 1;


--#1: all 25 year old women with minimum 10 month enrollment in 2017 by source and state
drop table dev.enrollment_date_2017_female_25yoa;
create table dev.enrollment_date_2017_female_25yoa
as select distinct e.*, d.year, d.month
from data_warehouse.enrollment e
join data_warehouse.dim_enrollment_date ed on e.id=ed.enrollment_id
join data_warehouse.dim_date d on d.id=ed.date_id
where d.year = 2017
and e.gndr_cd='F'
and (e.mbr_dob + 25) = 2017;

select source, mbr_id, count(month) as num_months_enrolled, count(distinct id) as num_enrollments
from dev.enrollment_date_2017_female_25yoa
where source='od'
and mbr_id=33039469695
group by source, mbr_id 
having count(month) >= 10 and count(distinct id) != 1 
order by 1, 4 desc, 3 desc;

--Sanity check
select *
from data_warehouse.enrollment
where mbr_id=33039469695
order by cov_eff_dt, cov_term_dt;

-- Final query
select source, state, count(distinct mbr_id) as num_mbrs
from dev.enrollment_date_2017_female_25yoa
group by 1, 2
having count(month) >= 10
order by 1, 2;


--#2: Avg of 'member months' cnt for each age group and source
drop table dev.enrollment_months_query2;
create table dev.enrollment_months_query2
as
select e.mbr_id, 2019-e.mbr_dob as age, e.source, count(month) as num_months
from data_warehouse.enrollment e
join data_warehouse.dim_enrollment_date ed on e.id=ed.enrollment_id
join data_warehouse.dim_date d on d.id=ed.date_id
group by 1, 2, 3;

select age, source, count(*) as cnt, avg(num_months) as average
from dev.enrollment_months_query2
group by 1, 2
order by 1, 2;


--#3: plantype by relcode in truven avg member count per year
--Notes: Need mappings
create table dev.enrollment_query3
as
select source, plan_ty_cd, relcode, d.year, count(distinct mbr_id) as mbr_cnt 
from data_warehouse.enrollment
join data_warehouse.dim_enrollment_date ed on e.id=ed.enrollment_id
join data_warehouse.dim_date d on d.id=ed.date_id
group by 1, 2, 3, 4;

select source, plan_ty_cd, relcode, avg(mbr_cnt) as avg_mbr_cnt
from dev.enrollment_query3
group by 1, 2, 3;

--#4: Number of members with enrollment gap > 2 months in a given calendar year

--REF: https://stackoverflow.com/questions/9604400/sql-query-to-show-gaps-between-multiple-date-ranges
SELECT EXTRACT(YEAR FROM age), EXTRACT(MONTH FROM age), EXTRACT(YEAR FROM age) * 12 + EXTRACT(MONTH FROM age) + 1 AS months_between
FROM age(TIMESTAMP '2012-06-30 10:38:40', TIMESTAMP '2011-06-01 14:38:40') AS t(age);

SELECT e.cov_eff_dt, e.cov_term_dt, EXTRACT(YEAR FROM age(e.cov_term_dt, e.cov_eff_dt)) * 12 + EXTRACT(MONTH FROM age(e.cov_term_dt, e.cov_eff_dt)) + 1 AS months_covered
FROM dev.enrollment_2017 e
limit 100;

SELECT distinct
    e1.mbr_id,
    e1.cov_term_dt as LastEndTime,
    e3.cov_eff_dt as NextStartTime
FROM dev.enrollment_2017 e1 
join dev.enrollment_2017 e3 on e1.cov_term_dt < e3.cov_eff_dt
     and e3.mbr_id = e1.mbr_id
     and e3.cov_eff_dt = (select min(cov_eff_dt) from dev.enrollment_2017 e5
                     where e5.cov_eff_dt > e1.cov_term_dt
                    and e5.mbr_id = e3.mbr_id)
where not exists (select * from dev.enrollment_2017 e7 
                    where e7.cov_eff_dt < e1.cov_term_dt
                      and e7.cov_term_dt > e1.cov_term_dt
                      and e7.mbr_id = e1.mbr_id)
order by e1.mbr_id, e1.cov_term_dt



--#5: How many families added a new member and avg age of new member for a given UT fiscal (temp calendar) year by source
--Notes: https://gpdb.docs.pivotal.io/510/best_practices/summary.html

drop table dev.enrollment_family_by_year_2013;
create table dev.enrollment_family_by_year_2013
WITH (appendonly=true, orientation=column)
as (
select distinct source, fam_id, mbr_id, d.year
from data_warehouse.enrollment e
join data_warehouse.dim_enrollment_date ed on e.id=ed.enrollment_id
join data_warehouse.dim_date d on d.id=ed.date_id
where d.year=2013 )
distributed by (fam_id);

analyse dev.enrollment_family_by_year_2013;

drop table dev.enrollment_family_by_year_2014;
create table dev.enrollment_family_by_year_2014
WITH (appendonly=true, orientation=column)
as (
select distinct source, fam_id, mbr_id, 2014-e.mbr_dob as age, d.year
from data_warehouse.enrollment e
join data_warehouse.dim_enrollment_date ed on e.id=ed.enrollment_id
join data_warehouse.dim_date d on d.id=ed.date_id
where d.year=2014 )
distributed by (fam_id);

analyse dev.enrollment_family_by_year_2014;

select n.source, count(distinct n.fam_id) as num_families, avg(n.age) as avg_age
from dev.enrollment_family_by_year_2014 n
left join dev.enrollment_family_by_year_2013 o on n.fam_id=o.fam_id and n.mbr_id=o.mbr_id
where o.mbr_id is null -- Wasn't a member of family in 2014
and n.fam_id in (select distinct fam_id from dev.enrollment_family_by_year_2013) --Was existing fam_id 2013
group by 1;


--#6: Like above ut count of new members by age
select n.source, n.age, count(*) as new_member_count
from dev.enrollment_family_by_year_2014 n
left join dev.enrollment_family_by_year_2013 o on n.fam_id=o.fam_id and n.mbr_id=o.mbr_id
where o.mbr_id is null -- Wasn't a member of family in 2014
and n.fam_id in (select distinct fam_id from dev.enrollment_family_by_year_2013) --Was existing fam_id 2013
group by 1, 2
order by 1, 2;

