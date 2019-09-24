drop external table ext_inpatient_span_codes;

CREATE EXTERNAL TABLE ext_inpatient_span_codes (
BENE_ID varchar, CLM_ID varchar, NCH_CLM_TYPE_CD varchar, RLT_SPAN_CD_SEQ varchar, CLM_SPAN_CD varchar, CLM_SPAN_FROM_DT varchar, 
CLM_SPAN_THRU_DT varchar
) 
LOCATION ( 
'gpfdist://c252-140:8801/medicare/201*/inpatient_span_codes.csv'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_inpatient_span_codes
limit 1000;

create table medicare.inpatient_span_codes
WITH (appendonly=true, orientation=column)
as
select * 
from ext_inpatient_span_codes
distributed randomly;

select count(*)
from medicare.inpatient_span_codes;