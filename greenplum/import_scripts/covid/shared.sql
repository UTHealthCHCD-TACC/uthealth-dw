--Medical
drop table shared.covid_positive_20201015;
create table shared.covid_positive_20201015 (
PTID varchar, TEST_NAME varchar, RESULT_DATE date, antibody_test boolean, Interpret varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (ptid);

-- NOTE: Load each year 1 by one updating the gpfdist string and hard coded YEAR value in insert statement.

drop external table ext_covid_positive_20201015;
CREATE EXTERNAL TABLE ext_covid_positive_20201015 (
PTID varchar, TEST_NAME varchar, RESULT_DATE date, antibody_test boolean, Interpret varchar
) 
LOCATION ( 
'gpfdist://greenplum01:8081/covid/other/COVID_POSITIVE_20201015.csv'
)
FORMAT 'CSV' ( HEADER );

-- Test
/*
select *
from ext_covid_positive_20201015
limit 1000;
*/
-- Insert: 14s, Updated Rows	26,567,167
insert into shared.covid_positive_20201015
select * from ext_covid_positive_20201015;

--Scratch
select count(*)
from shared.covid_positive_20201015
group by 1
order by 1;