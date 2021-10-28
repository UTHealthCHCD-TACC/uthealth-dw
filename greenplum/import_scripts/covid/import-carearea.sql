create schema opt_20210916;

--Medical
drop table opt_20210916.carearea;
create table opt_20210916.carearea (
PTID varchar,ENCID varchar,CAREAREA varchar,CAREAREA_DATE date,CAREAREA_TIME time,SOURCEID varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (ptid);

-- NOTE: Load each year 1 by one updating the gpfdist string and hard coded YEAR value in insert statement.

drop external table ext_covid_carearea;
CREATE EXTERNAL TABLE ext_covid_carearea (
PTID varchar,ENCID varchar,CAREAREA varchar,CAREAREA_DATE date,CAREAREA_TIME time,SOURCEID varchar
) 
LOCATION ( 
'gpfdist://greenplum01:8081/covid/20210916/*carearea*.*'
)
FORMAT 'text' ( HEADER DELIMITER '|' null as '' escape 'OFF');

-- Test
/*
select *
from ext_covid_carearea
limit 1000;
*/
-- Insert: 14s, Updated Rows	26,567,167
insert into opt_20210916.carearea
select * from ext_covid_carearea;

--Scratch
select count(*)
from opt_20210916.carearea
group by 1
order by 1;