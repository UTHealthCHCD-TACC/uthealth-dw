--Medical
drop table covid_20200525.rx_immun;
create table covid_20200525.rx_immun (
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
'gpfdist://192.168.58.179:8081/covid/*rx_immun.txt'
)
FORMAT 'text' ( HEADER DELIMITER '|' null as '' escape 'OFF');

-- Test
/*
select *
from ext_covid_rx_immun
limit 1000;
*/
-- rx_immunert: 4s, Updated Rows	2740928
insert into covid_20200525.rx_immun
select * from ext_covid_rx_immun;

--Scratch
select count(*)
from covid_20200525.rx_immun
group by 1
order by 1;