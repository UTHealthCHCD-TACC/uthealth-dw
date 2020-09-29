drop external table ext_outpatient_occurrnce_codes;

CREATE EXTERNAL TABLE ext_outpatient_occurrnce_codes (
year text,
BENE_ID varchar, CLM_ID varchar, NCH_CLM_TYPE_CD varchar, RLT_OCRNC_CD_SEQ varchar, CLM_RLT_OCRNC_CD varchar, CLM_RLT_OCRNC_DT varchar
) 
LOCATION ( 
'gpfdist://192.168.58.179:8081/medicare_texas/*/*outpatient_occurrnce_codes.csv.gz#transform=add_parentname'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_outpatient_occurrnce_codes
limit 1000;

create table medicare_texas.outpatient_occurrnce_codes
WITH (appendonly=true, orientation=column, compresstype=zlib)
as

--insert into medicare_texas.outpatient_occurrnce_codes 
select * 
from ext_outpatient_occurrnce_codes

distributed randomly;

select count(*)
from medicare_texas.outpatient_occurrnce_codes;