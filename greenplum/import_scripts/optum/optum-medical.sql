--Medical
drop table optum_dod.medical;
create table optum_dod.medical (
year smallint, file varchar,
PATID bigint,PAT_PLANID bigint,ADMIT_CHAN char(16),ADMIT_TYPE char(1),BILL_PROV numeric,CHARGE numeric,
CLMID char(19),CLMSEQ char(5),COB char(5),COINS numeric,CONF_ID char(21),COPAY numeric,DEDUCT numeric,
    DRG char(5),DSTATUS char(2),ENCTR char(2),FST_DT date,HCCC char(2),ICD_FLAG char(2),LOC_CD char(1),LST_DT date,NDC char(11),PAID_DT date,
PAID_STATUS char(2),POS char(5),PROC_CD char(7),
PROCMOD char(5),PROV numeric,PROV_PAR char(5),PROVCAT char(4),REFER_PROV numeric,RVNU_CD char(4),SERVICE_PROV numeric,STD_COST numeric,STD_COST_YR char(4),TOS_CD char(13),
UNITS numeric,EXTRACT_YM  char(6),VERSION  char(6),
ALT_UNITS text, BILL_TYPE text, NDC_UOM text, NDC_QTY text, OP_VISIT_ID text, PROCMOD2 text, PROCMOD3 text, PROCMOD4 text, TOS_EXT text
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (patid);

drop external table ext_medical;
CREATE EXTERNAL TABLE ext_medical (
year smallint, file varchar,
PATID bigint,PAT_PLANID bigint,ADMIT_CHAN char(16),ADMIT_TYPE char(1),BILL_PROV numeric,CHARGE numeric,
CLMID char(19),CLMSEQ char(5),COB char(5),COINS numeric,CONF_ID char(21),COPAY numeric,DEDUCT numeric,
    DRG char(5),DSTATUS char(2),ENCTR char(2),FST_DT date,HCCC char(2),ICD_FLAG char(2),LOC_CD char(1),LST_DT date,NDC char(11),PAID_DT date,
PAID_STATUS char(2),POS char(5),PROC_CD char(7),
PROCMOD char(5),PROV numeric,PROV_PAR char(5),PROVCAT char(4),REFER_PROV numeric,RVNU_CD char(4),SERVICE_PROV numeric,STD_COST numeric,STD_COST_YR char(4),TOS_CD char(13),
UNITS numeric,EXTRACT_YM  char(6),VERSION  char(6),
ALT_UNITS text, BILL_TYPE text, NDC_UOM text, NDC_QTY text, OP_VISIT_ID text, PROCMOD2 text, PROCMOD3 text, PROCMOD4 text, TOS_EXT text
) 
LOCATION ( 
'gpfdist://greenplum01.corral.tacc.utexas.edu:8081/uthealth/OPTUM_NEW/OPT_DOD_APril2021/\*/dod_m2*.txt.gz#transform=add_parentname_filename_vertbar'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
select *
from ext_medical
limit 1000;


-- Insert
insert into optum_dod.medical
select * from ext_medical;

--delete from optum_dod.medical where year=0;

select count(*)
from optum_dod.medical
where year=0;

-- Analyze
analyze optum.medical;

select count(*), min(year), max(year), count(distinct year) from optum_dod.medical;

select year, date_part('quarter', FST_DT) as quarter, count(*), min(FST_DT), max(FST_DT)
from optum_dod.medical
group by 1, 2
order by 1, 2;

--Refresh
delete
from optum_dod.medical 
where year > 2017 or file like '%2017q4%';