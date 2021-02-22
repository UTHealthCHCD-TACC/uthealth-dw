--Medical
drop table opt_20210107.rx_patrep;
create table opt_20210107.rx_patrep (
PTID varchar,REPORTED_DATE date,DRUG_NAME varchar,NDC varchar,NDC_SOURCE varchar,PROVID varchar,ROUTE varchar,
QUANTITY_OF_DOSE varchar,STRENGTH varchar,STRENGTH_UNIT varchar,DOSAGE_FORM varchar,DOSE_FREQUENCY varchar,GENERIC_DESC varchar,DRUG_CLASS varchar,SOURCEID varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (PTID);

-- NOTE: Load each year 1 by one updating the gpfdist string and hard coded YEAR value in rx_patrepert statement.

drop external table ext_covid_rx_patrep;
CREATE EXTERNAL TABLE ext_covid_rx_patrep (
PTID varchar,REPORTED_DATE date,DRUG_NAME varchar,NDC varchar,NDC_SOURCE varchar,PROVID varchar,ROUTE varchar,
QUANTITY_OF_DOSE varchar,STRENGTH varchar,STRENGTH_UNIT varchar,DOSAGE_FORM varchar,DOSE_FREQUENCY varchar,GENERIC_DESC varchar,DRUG_CLASS varchar,SOURCEID varchar
) 
LOCATION ( 
'gpfdist://greenplum01:8081/covid/20210107/*rx_patrep.txt.gz'
)
FORMAT 'text' ( HEADER DELIMITER '|' null as '');

-- Test
/*
select *
from ext_covid_rx_patrep
limit 1000;
*/
-- rx_patrepert: 108s, Updated Rows	81970342
insert into opt_20210107.rx_patrep
select * from ext_covid_rx_patrep;

--Scratch
select count(*)
from opt_20210107.rx_patrep
group by 1
order by 1;