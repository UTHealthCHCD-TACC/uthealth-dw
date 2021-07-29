
drop external table ext_temp;
CREATE EXTERNAL TABLE ext_temp (
MemberID varchar,FSCYR smallint,AGE smallint,AgeGrp smallint,MemberGender char,cntEnrlMth smallint,
chronic_cndcnt boolean,AIMM boolean,AMI boolean,ASTH boolean,CA boolean,CFIB boolean,CHF boolean,CKD boolean,CLIV boolean,COPD boolean,CRES boolean,DB boolean,DEL boolean,DEM boolean,DEP boolean,EPI boolean,FBM boolean,HEMO boolean,HEP boolean,HIP boolean,HIV boolean,HTN boolean,KNEE boolean,LB boolean,LBP boolean,LYMP boolean,MS boolean,NICU boolean,OPI boolean,PAIN boolean,PARK boolean,PNEU boolean,PREG boolean,RA boolean,SMI boolean,SPF boolean,TBI boolean,TRANS boolean,TRAU boolean,QEDB
) 
LOCATION ( 
'gpfdist://greenplum01:8081/uthealth/PERS_PROF_MEDICARE_202102040855.csv'
)
FORMAT 'CSV' ( HEADER );

-- Test
/*
select *
from ext_covid_positive_20201015
limit 1000;
*/
-- Insert: 14s, Updated Rows	26,567,167
insert into shared.covid_positive_20201015
select * from ext_covid_positive_20201015;

--Scratch
select count(*)
from shared.covid_positive_20201015
group by 1
order by 1;