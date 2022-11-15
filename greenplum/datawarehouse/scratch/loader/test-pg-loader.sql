drop table if exists dev.delete_testpy;

select * 
into dev.delete_testpy
from dev.ms2_all_19_20 
limit 100;

drop table if exists dev.delete_testpy2;

select * 
into dev.delete_testpy2
from dev.ms2_all_19_20 
limit 100;

drop table if exists dev.delete_testpy3;

select * 
into dev.delete_testpy3
from dev.ms2_all_19_20 
limit 100;