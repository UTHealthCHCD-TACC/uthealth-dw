drop external table ext_bcarrier_demo_codes;

CREATE EXTERNAL TABLE ext_bcarrier_demo_codes (
year text, filename text,
BENE_ID varchar, CLM_ID varchar, NCH_CLM_TYPE_CD varchar, DEMO_ID_SQNC_NUM varchar, DEMO_ID_NUM varchar, DEMO_INFO_TXT varchar
) 
LOCATION ( 
'gpfdist://192.168.58.179:8081/medicare_national/*/*bcarrier_demo_codes.csv.gz#transform=add_parentname_filename_comma'
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

--Scratch
select year, count(*)
from medicare_national.bcarrier_demo_codes
group by 1
order by 1;