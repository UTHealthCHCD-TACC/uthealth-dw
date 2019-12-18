drop external table ext_hospice_demo_codes;

CREATE EXTERNAL TABLE ext_hospice_demo_codes (
BENE_ID varchar, CLM_ID varchar, NCH_CLM_TYPE_CD varchar, DEMO_ID_SQNC_NUM varchar, DEMO_ID_NUM varchar, DEMO_INFO_TXT varchar
) 
LOCATION ( 
'gpfdist://c252-140:8801/medicare/201*/hospice_demo_codes.csv'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_hospice_demo_codes
limit 1000;

create table medicare.hospice_demo_codes
WITH (appendonly=true, orientation=column)
as
select * 
from ext_hospice_demo_codes
distributed randomly;

select count(*)
from medicare.hospice_demo_codes;