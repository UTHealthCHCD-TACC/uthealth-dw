--Audit Queries
select e.source, count(e.id) as cnt, min(d.date) as min_date, max(d.date) as max_date 
from data_warehouse.enrollment e
join data_warehouse.dim_enrollment_date ed on e.id=ed.enrollment_id
join data_warehouse.dim_date d on d.id=ed.date_id
group by 1;


select e.source, count(e.id) as cnt, min(e.cov_eff_dt) as min_cov_eff_dt, max(e.cov_term_dt) as max_cov_term_dt
from data_warehouse.enrollment e
group by 1;

select count(*), min(dtstart), max(dtend)
from truven.ccaet;

--#1: all 25 year old women with minimum 10 month enrollment in 2017 by source and state
drop table dev.enrollment_2017;
create table dev.enrollment_2017
as select distinct e.*
from data_warehouse.enrollment e
join data_warehouse.dim_enrollment_date ed on e.id=ed.enrollment_id
join data_warehouse.dim_date d on d.id=ed.date_id
where d.year = 2017

select source, state, count(distinct mbr_id) as mbr_cnt
from dev.enrollment_2017
where gndr_cd='F'
and (mbr_dob + 25) = 2017
group by source, state
order by source, state;

--#2: Avg of 'member months' cnt for each age group and source

--#3: plantype by relcode in truven avg member count

select distinct source, plan_ty_cd
from data_warehouse.enrollment
order by 1, 2;

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



--#5: How many families added a new member and avg age of new member for a given UT fiscal year by source

--#6: Like above ut count of new members by age



