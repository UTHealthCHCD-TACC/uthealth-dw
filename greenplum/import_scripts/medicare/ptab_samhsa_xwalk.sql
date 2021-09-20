drop external table ext_ptab_samhsa_xwalk;

CREATE EXTERNAL TABLE ext_ptab_samhsa_xwalk (
year text, filename text,
CLM_ID varchar, NCH_CLM_TYPE varchar
) 
LOCATION ( 
'gpfdist://greenplum01:8081/uthealth/medicare_national/*/*ptab_samhsa_xwalk.csv.gz#transform=add_parentname_filename_comma'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_ptab_samhsa_xwalk
limit 1000;

create table uthealth/medicare_national.ptab_samhsa_xwalk
WITH (appendonly=true, orientation=column, compresstype=zlib)
as

--insert into uthealth/medicare_national.ptab_samhsa_xwalk 
select year, clm_id, nch_clm_type 
from ext_ptab_samhsa_xwalk

distributed randomly;

-- Scratch
select year, count(*)
from uthealth/medicare_national.pde_file
group by 1
order by 1;