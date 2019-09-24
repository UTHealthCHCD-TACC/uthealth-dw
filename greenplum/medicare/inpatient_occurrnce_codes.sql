drop external table ext_inpatient_occurrnce_codes;

CREATE EXTERNAL TABLE ext_inpatient_occurrnce_codes (
BENE_ID varchar, CLM_ID varchar, NCH_CLM_TYPE_CD varchar, RLT_OCRNC_CD_SEQ varchar, CLM_RLT_OCRNC_CD varchar, CLM_RLT_OCRNC_DT varchar
) 
LOCATION ( 
'gpfdist://c252-140:8801/medicare/201*/inpatient_occurrnce_codes.csv'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_inpatient_occurrnce_codes
limit 1000;

create table medicare.inpatient_occurrnce_codes
WITH (appendonly=true, orientation=column)
as
select * 
from ext_inpatient_occurrnce_codes
distributed randomly;

select count(*)
from medicare.inpatient_occurrnce_codes;