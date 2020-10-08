--Medical
drop table covid_20200525.enc_prov;
create table covid_20200525.enc_prov (
PTID varchar, ENCID varchar, PROVIDERROLE varchar, SOURCEID varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed randomly;

-- NOTE: Load each year 1 by one updating the gpfdist string and hard coded YEAR value in insert statement.

drop external table ext_covid_enc_prov;
CREATE EXTERNAL TABLE ext_covid_enc_prov (
PTID varchar, ENCID varchar, PROVIDERROLE varchar, SOURCEID varchar
) 
LOCATION ( 
'gpfdist://192.168.58.179:8081/covid/*enc_prov.*'
)
FORMAT 'text' ( HEADER DELIMITER '|' null as '' escape 'OFF');

-- Test
/*
select *
from ext_covid_enc_prov
limit 1000;
*/
-- Insert: 78s, Updated Rows	178,532,862
insert into covid_20200525.enc_prov
select * from ext_covid_enc_prov;

--Scratch
select year, count(*), min(admit_date), max(admit_date)
from covid_20200525.enc_prov
group by 1
order by 1;