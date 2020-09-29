drop external table ext_snf_span_codes;

CREATE EXTERNAL TABLE ext_snf_span_codes (
year text,
BENE_ID varchar, CLM_ID varchar, NCH_CLM_TYPE_CD varchar, RLT_SPAN_CD_SEQ varchar, CLM_SPAN_CD varchar, CLM_SPAN_FROM_DT varchar, 
CLM_SPAN_THRU_DT varchar
) 
LOCATION ( 
'gpfdist://192.168.58.179:8081/medicare_texas/*/*snf_span_codes.csv.gz#transform=add_parentname'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_snf_span_codes
limit 1000;

create table medicare_texas.snf_span_codes
WITH (appendonly=true, orientation=column, compresstype=zlib)
as

--insert into medicare_texas.snf_span_codes 
select * 
from ext_snf_span_codes

distributed randomly;

select count(*)
from medicare_texas.snf_span_codes;