drop external table ext_inpatient_demo_codes;

CREATE EXTERNAL TABLE ext_inpatient_demo_codes (
year text, filename text,
BENE_ID, CLM_ID, NCH_CLM_TYPE_CD, DEMO_ID_SQNC_NUM, DEMO_ID_NUM, DEMO_INFO_TXT
) 
LOCATION ( 
'gpfdist://192.168.58.179:8081/medicare_texas/*/*inpatient_demo_codes.csv.gz#transform=add_parentname_filename_comma'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_inpatient_demo_codes
limit 1000;

create table medicare_texas.inpatient_demo_codes
WITH (appendonly=true, orientation=column, compresstype=zlib)
as

insert into medicare_texas.inpatient_demo_codes (year,
BENE_ID, CLM_ID, NCH_CLM_TYPE_CD, DEMO_ID_SQNC_NUM, DEMO_ID_NUM, DEMO_INFO_TXT
)
select year,
BENE_ID, CLM_ID, NCH_CLM_TYPE_CD, DEMO_ID_SQNC_NUM, DEMO_ID_NUM, DEMO_INFO_TXT 
from ext_inpatient_demo_codes

distributed randomly;

select count(*)
from medicare_texas.inpatient_demo_codes;