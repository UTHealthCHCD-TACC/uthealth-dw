--Medical
drop table covid_20200525.rx_adm;
create table covid_20200525.rx_adm (
PTID varchar,ENCID varchar,ORDERID varchar,DRUG_NAME varchar,NDC varchar,NDC_SOURCE varchar,
ORDER_DATE date,ORDER_TIME time,ADMIN_DATE date,ADMIN_TIME time,PROVID varchar,ROUTE varchar,
QUANTITY_OF_DOSE varchar,STRENGTH varchar,STRENGTH_UNIT varchar,DOSAGE_FORM varchar,DOSE_FREQUENCY varchar,
GENERIC_DESC varchar,DRUG_CLASS varchar,DISCONTINUE_REASON varchar,SOURCEID varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (PTID);

-- NOTE: Load each year 1 by one updating the gpfdist string and hard coded YEAR value in rx_admert statement.

drop external table ext_covid_rx_adm;
CREATE EXTERNAL TABLE ext_covid_rx_adm (
PTID text,ENCID text,ORDERID text,DRUG_NAME text,NDC text,NDC_SOURCE text,
ORDER_DATE date,ORDER_TIME time,ADMIN_DATE date, ADMIN_TIME time,PROVID text,ROUTE text,
QUANTITY_OF_DOSE text,STRENGTH text,STRENGTH_UNIT text,DOSAGE_FORM text,DOSE_FREQUENCY text,
GENERIC_DESC text,DRUG_CLASS text,DISCONTINUE_REASON text,SOURCEID text
) 
LOCATION ( 
'gpfdist://192.168.58.179:8081/covid/*rx_adm.txt'
)
FORMAT 'text' ( HEADER DELIMITER '|' null as '');

-- Test
/*
select count(*)
from ext_covid_rx_adm
limit 1000;
*/
-- rx_admert: 138s, Updated Rows	97,729,152
insert into covid_20200525.rx_adm
select * from ext_covid_rx_adm;

--Scratch
select count(*)
from covid_20200525.rx_adm
group by 1
order by 1;