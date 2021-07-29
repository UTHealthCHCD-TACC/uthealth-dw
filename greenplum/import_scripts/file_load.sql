
drop external table ext_temp;
CREATE EXTERNAL TABLE ext_temp (
MemberID text,FSCYR int2,AGE smallint,AgeGrp smallint,MemberGender char,cntEnrlMth smallint,
chronic_cndcnt int2,AIMM int2,AMI int2,ASTH int2,CA int2,CFIB int2,CHF int2,CKD int2,CLIV int2,COPD int2,CRES int2,
DB int2,DEL int2,DEM int2,DEP int2,EPI int2,FBM int2,HEMO int2,HEP int2,HIP int2,HIV int2,HTN int2,KNEE int2,
LB int2,LBP int2,LYMP int2,MS int2,NICU int2,OPI int2,PAIN int2,PARK int2,PNEU int2,PREG int2,RA int2,
SMI int2,SPF int2,TBI int2,TRANS int2,TRAU int2,QEDB int2
) 
LOCATION ( 
'gpfdist://greenplum01:8081/uthealth/PERS_PROF_MEDICARE_202102040855.csv'
)
FORMAT 'CSV' ( HEADER );

-- Test
/*
select count(*)
from ext_temp;
*/
insert into conditions.sqlserv_pers_prof_medicare
select *
from ext_temp;

select count(*)
from conditions.sqlserv_pers_prof_medicare;


