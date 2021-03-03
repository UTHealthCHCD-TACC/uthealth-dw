--Medical
drop table opt_20210128.rx_immun;
create table opt_20210128.rx_immun (
PTID varchar,IMMUNIZATION_DATE date,IMMUNIZATION_DESC varchar,MAPPED_NAME varchar,NDC varchar,NDC_SOURCE varchar,PT_REPORTED varchar,SOURCEID varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (PTID);

-- NOTE: Load each year 1 by one updating the gpfdist string and hard coded YEAR value in rx_immunert statement.

drop external table ext_covid_rx_immun;
CREATE EXTERNAL TABLE ext_covid_rx_immun (
PTID varchar,IMMUNIZATION_DATE date,IMMUNIZATION_DESC varchar,MAPPED_NAME varchar,NDC varchar,NDC_SOURCE varchar,PT_REPORTED varchar,SOURCEID varchar
) 
LOCATION ( 
'gpfdist://greenplum01:8081/covid/20210128/*rx_immun*.txt.gz'
)
FORMAT 'text' ( HEADER DELIMITER '|' null as '' escape 'OFF');

-- Test
/*
select *
from ext_covid_rx_immun
limit 1000;
*/
-- rx_immunert: 4s, Updated Rows	2740928
insert into opt_20210128.rx_immun
select * from ext_covid_rx_immun;

--Scratch
select count(*)
from opt_20210128.rx_immun
group by 1
order by 1;