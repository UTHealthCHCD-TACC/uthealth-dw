drop external table ext_hha_span_codes;

CREATE EXTERNAL TABLE ext_hha_span_codes (
year text, filename text,
BENE_ID varchar, CLM_ID varchar, NCH_CLM_TYPE_CD varchar, RLT_SPAN_CD_SEQ varchar, 
CLM_SPAN_CD varchar, CLM_SPAN_FROM_DT varchar, CLM_SPAN_THRU_DT varchar
) 
LOCATION ( 
'gpfdist://192.168.58.179:8081/medicare_texas/*/*hha_span_codes.csv.gz#transform=add_parentname_filename_comma'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_hha_span_codes
limit 1000;

create table medicare_texas.hha_span_codes
WITH (appendonly=true, orientation=column, compresstype=zlib)
as

insert into medicare_texas.hha_span_codes (year,
BENE_ID, CLM_ID, NCH_CLM_TYPE_CD, RLT_SPAN_CD_SEQ, 
CLM_SPAN_CD, CLM_SPAN_FROM_DT, CLM_SPAN_THRU_DT
)
select year,
BENE_ID, CLM_ID, NCH_CLM_TYPE_CD, RLT_SPAN_CD_SEQ, 
CLM_SPAN_CD, CLM_SPAN_FROM_DT, CLM_SPAN_THRU_DT 
from ext_hha_span_codes;

distributed randomly;

select count(*)
from medicare_texas.hha_span_codes;

