drop external table ext_inpatient_span_codes;

CREATE EXTERNAL TABLE ext_inpatient_span_codes (
year text, filename text,
BENE_ID varchar, CLM_ID varchar, NCH_CLM_TYPE_CD varchar, RLT_SPAN_CD_SEQ varchar, CLM_SPAN_CD varchar, CLM_SPAN_FROM_DT varchar, 
CLM_SPAN_THRU_DT varchar
) 
LOCATION ( 
'gpfdist://greenplum01:8081/uthealth/medicare_national/*/*inpatient_span_codes.csv.gz#transform=add_parentname_filename_comma'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_inpatient_span_codes
limit 1000;

create table uthealth/medicare_national.inpatient_span_codes
WITH (appendonly=true, orientation=column, compresstype=zlib)
as

insert into uthealth/medicare_national.inpatient_span_codes (year,
BENE_ID, CLM_ID, NCH_CLM_TYPE_CD, RLT_SPAN_CD_SEQ, CLM_SPAN_CD, CLM_SPAN_FROM_DT, CLM_SPAN_THRU_DT
)
select year,
BENE_ID, CLM_ID, NCH_CLM_TYPE_CD, RLT_SPAN_CD_SEQ, CLM_SPAN_CD, CLM_SPAN_FROM_DT, CLM_SPAN_THRU_DT 
from ext_inpatient_span_codes

distributed randomly;

select count(*)
from uthealth/medicare_national.inpatient_span_codes;