drop external table ext_hha_demo_codes;

CREATE EXTERNAL TABLE ext_hha_demo_codes (
year text,
BENE_ID varchar, CLM_ID varchar, NCH_CLM_TYPE_CD varchar, DEMO_ID_SQNC_NUM varchar, DEMO_ID_NUM varchar, DEMO_INFO_TXT varchar
) 
LOCATION ( 
'gpfdist://192.168.58.179:8081/medicare/*/*hha_demo_codes.csv.gz#transform=add_parentname'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_hha_demo_codes
limit 1000;

create table medicare.hha_demo_codes
WITH (appendonly=true, orientation=column, compresstype=zlib)
as

--insert into medicare.hha_demo_codes 
select * 
from ext_hha_demo_codes

distributed randomly;

select count(*)
from medicare.hha_demo_codes;

