--Medical
drop table covid_20200525.rx_presc;
create table covid_20200525.rx_presc (
PTID varchar,RXDATE date,RXTIME varchar,DRUG_NAME varchar,NDC varchar,NDC_SOURCE varchar,PROVID varchar,
ROUTE varchar,QUANTITY_OF_DOSE varchar,STRENGTH varchar,STRENGTH_UNIT varchar,DOSAGE_FORM varchar,DAILY_DOSE varchar,DOSE_FREQUENCY varchar,
QUANTITY_PER_FILL varchar,NUM_REFILLS varchar,DAYS_SUPPLY varchar,GENERIC_DESC varchar,DRUG_CLASS varchar,DISCONTINUE_REASON varchar,SOURCEID varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (PTID);

-- NOTE: Load each year 1 by one updating the gpfdist string and hard coded YEAR value in rx_prescert statement.

drop external table ext_covid_rx_presc;
CREATE EXTERNAL TABLE ext_covid_rx_presc (
PTID varchar,RXDATE date,RXTIME varchar,DRUG_NAME varchar,NDC varchar,NDC_SOURCE varchar,PROVID varchar,
ROUTE varchar,QUANTITY_OF_DOSE varchar,STRENGTH varchar,STRENGTH_UNIT varchar,DOSAGE_FORM varchar,DAILY_DOSE varchar,DOSE_FREQUENCY varchar,
QUANTITY_PER_FILL varchar,NUM_REFILLS varchar,DAYS_SUPPLY varchar,GENERIC_DESC varchar,DRUG_CLASS varchar,DISCONTINUE_REASON varchar,SOURCEID varchar
) 
LOCATION ( 
'gpfdist://192.168.58.179:8081/covid/*rx_presc.txt'
)
FORMAT 'text' ( HEADER DELIMITER '|' null as '' escape 'OFF');

-- Test
/*
select *
from ext_covid_rx_presc
limit 1000;
*/
-- rx_prescert: 20s, Updated Rows	43591290
insert into covid_20200525.rx_presc
select * from ext_covid_rx_presc;

--Scratch
select count(*)
from covid_20200525.rx_presc
group by 1
order by 1;