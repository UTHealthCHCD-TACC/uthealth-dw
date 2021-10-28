create schema opt_20210916;

set client_encoding to 'UTF8';

/*
 * Requires cleaning
 * tr -cd '\11\12\15\40-\176' < cov_20210916_lab.txt > cov_20210916_lab-fixed.txt
 * sed -i 's:\\:\\\\:g' cov_20201210_lab-fixed.txt
 */
--Medical
drop table opt_20210916.lab;
create table opt_20210916.lab (
PTID varchar,ENCID varchar,TEST_CODE varchar,TEST_NAME varchar,TEST_TYPE varchar,
ORDER_DATE date,ORDER_TIME time,COLLECTED_DATE date,COLLECTED_TIME time,RESULT_DATE date,RESULT_TIME time,
TEST_RESULT varchar,RELATIVE_INDICATOR varchar,RESULT_UNIT varchar,NORMAL_RANGE varchar,EVALUATED_FOR_RANGE varchar,VALUE_WITHIN_RANGE varchar,SOURCEID varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (ptid);

-- NOTE: Load each year 1 by one updating the gpfdist string and hard coded YEAR value in labert statement.
PTID|ENCID|TEST_CODE|TEST_NAME|TEST_TYPE|
ORDER_DATE|ORDER_TIME|COLLECTED_DATE|COLLECTED_TIME|RESULT_DATE|RESULT_TIME|
TEST_RESULT|RELATIVE_INDICATOR|RESULT_UNIT|NORMAL_RANGE|EVALUATED_FOR_RANGE|
VALUE_WITHIN_RANGE|SOURCEID

drop external table ext_covid_lab;
CREATE EXTERNAL TABLE ext_covid_lab (
PTID varchar,ENCID varchar,TEST_CODE varchar,TEST_NAME varchar,TEST_TYPE varchar,
ORDER_DATE date,ORDER_TIME time,COLLECTED_DATE date, COLLECTED_TIME time, RESULT_DATE date,RESULT_TIME time,
TEST_RESULT varchar,RELATIVE_INDICATOR varchar,RESULT_UNIT varchar,NORMAL_RANGE varchar,EVALUATED_FOR_RANGE varchar,
VALUE_WITHIN_RANGE varchar,SOURCEID varchar
) 
LOCATION ( 
'gpfdist://greenplum01:8081/covid/20210916/*lab-fixed.txt'
)
FORMAT 'text' ( header DELIMITER '|' null as '' escape 'OFF');

-- Test
/*
select *
from ext_covid_lab
limit 1000;
*/
-- labert: 291s, Updated Rows	483,210,548
insert into opt_20210916.lab
select * from ext_covid_lab;

select count(*)
from opt_20210916.lab;

--Scratch
select l.test_name, count(*)
from opt_20210916.lab l
join opt_20210916.pt on l.ptid=pt.ptid
where pt.birth_yr != 'Unknown' and pt.birth_yr >= '2000'
group by 1
order by 2 desc;

create table g823235.young_lab_tests
WITH (appendonly=true, orientation=column, compresstype=zlib)
as
select pt.birth_yr, pt.gender, pt.race, pt.ethnicity, pt.region, l.*
from opt_20210916.lab l
join opt_20210916.pt on l.ptid=pt.ptid
where pt.birth_yr != 'Unknown' and pt.birth_yr >= '2000'
distributed by (ptid);

select distinct birth_yr
from opt_20210916.pt;
