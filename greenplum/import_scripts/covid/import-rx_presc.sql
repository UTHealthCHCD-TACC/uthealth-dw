--Medical
drop table opt_20210916.rx_presc;
create table opt_20210916.rx_presc (
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
'gpfdist://greenplum01:8081/covid/20210916/*rx_presc*.txt.gz'
)
FORMAT 'text' ( HEADER DELIMITER '|' null as '' escape 'OFF');

-- Test
/*
select count(*)
from ext_covid_rx_presc
limit 1000;
*/
-- rx_prescert: 20s, Updated Rows	43591290
insert into opt_20210916.rx_presc
select * from ext_covid_rx_presc;

--Scratch
select count(*)
from opt_20210916.rx_presc
group by 1
order by 1;