drop table dev.dw_recursive_test;

create table dev.dw_recursive_test
WITH (appendonly=true, orientation=column)
as select * from  dev.jw_mehta_allelig
order by patid, eligeff, eligend 
distributed by (patid);

select *
from dev.dw_recursive_test
order by patid, eligeff, eligend
limit 100;

select count(*)
from dev.dw_recursive_test;

select *
from dev.dw_recursive_test drt 
--limit 10
where patid=33003285910;

select *
from dev.dw_recursive_test3 drt 
--limit 10
where patid=33003285910;

select min("date"), max("date"), sum(value)
from (
    select
        "date", value,
        "date" - (dense_rank() over(order by "date"))::int g
    from t
) s
group by s.g
order by 1

vacuum analyze dev.dw_recursive_test;

drop table dev.dw_recursive_test2;
create table dev.dw_recursive_test2
WITH (appendonly=true, orientation=column)
as
with recursive datejoin (patid, eligeff, eligend) as
(
       select t1.patid   as patid,
              t1.eligeff as eligeff,
              t2.eligend as eligend
       from   dev.dw_recursive_test t1
       join   dev.dw_recursive_test t2
       on     t1.patid = t2.patid
       where  t2.eligeff - t1.eligend = 1
    union
       select t3.patid,
              t3.eligeff,
              t4.eligend
       from   datejoin t3
       join   dev.dw_recursive_test t4
       on     t3.patid = t4.patid
       where  t4.eligeff - t3.eligend = 1 
)
select   patid,  
		eligeff,
         max(eligend) as eligend     
from     datejoin 
group by 1, 2
distributed by (patid);

drop table dev.dw_recursive_test3;
create table dev.dw_recursive_test3
WITH (appendonly=true, orientation=column)
as
select patid, eligend, min(eligeff)
from dev.dw_recursive_test2
group by 1, 2;
