drop external table ext_bcarrier_demo_codes;

CREATE EXTERNAL TABLE ext_bcarrier_demo_codes (
year text,
BENE_ID varchar, CLM_ID varchar, NCH_CLM_TYPE_CD varchar, DEMO_ID_SQNC_NUM varchar, DEMO_ID_NUM varchar, DEMO_INFO_TXT varchar
) 
LOCATION ( 
'gpfdist://192.168.58.179:8081/medicare/*/*bcarrier_demo_codes.csv.gz#transform=add_parentname'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_bcarrier_demo_codes
limit 1000;

create table medicare_national.bcarrier_demo_codes
WITH (appendonly=true, orientation=column, compresstype=zlib)
as

--insert into medicare_national.bcarrier_demo_codes 
select * 
from ext_bcarrier_demo_codes

distributed by (BENE_ID);

select count(*)
from medicare_national.bcarrier_demo_codes;