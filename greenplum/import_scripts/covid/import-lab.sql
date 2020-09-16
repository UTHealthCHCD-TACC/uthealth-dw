set client_encoding to 'sql_ascii';

--Medical
drop table covid_20200525.lab;
create table covid_20200525.lab (
PTID varchar,ENCID varchar,TEST_CODE varchar,TEST_NAME varchar,TEST_TYPE varchar,
ORDER_DATE date,ORDER_TIME time,COLLECTED_DATE date,COLLECTED_TIME time,RESULT_DATE date,RESULT_TIME time,
TEST_RESULT varchar,RELATIVE_INDICATOR varchar,RESULT_UNIT varchar,NORMAL_RANGE varchar,EVALUATED_FOR_RANGE varchar,VALUE_WITHIN_RANGE varchar,SOURCEID varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (ptid);

-- NOTE: Load each year 1 by one updating the gpfdist string and hard coded YEAR value in labert statement.

drop external table ext_covid_lab;
CREATE EXTERNAL TABLE ext_covid_lab (
PTID varchar,ENCID varchar,TEST_CODE varchar,TEST_NAME varchar,TEST_TYPE varchar,
ORDER_DATE date,ORDER_TIME time,COLLECTED_DATE date,COLLECTED_TIME time,RESULT_DATE date,RESULT_TIME time,
TEST_RESULT varchar,RELATIVE_INDICATOR varchar,RESULT_UNIT varchar,NORMAL_RANGE varchar,EVALUATED_FOR_RANGE varchar,VALUE_WITHIN_RANGE varchar,SOURCEID varchar
) 
LOCATION ( 
'gpfdist://192.168.58.179:8081/covid/*lab.txt.fixed'
)
FORMAT 'text' ( HEADER DELIMITER '|' null as '' escape 'OFF');

-- Test
/*
select *
select count(*)
from ext_covid_lab
limit 1000;
*/
-- labert: 291s, Updated Rows	483,210,548
insert into covid_20200525.lab
select * from ext_covid_lab;

--Scratch
select count(*)
from covid_20200525.lab
group by 1
order by 1;

show client_encoding;