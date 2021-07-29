/*
v1 Fields:

SEQNUM,VERSION,EFAMID,ENROLID,NDCNUM,SVCDATE,DOBYR,YEAR,AGE,AWP,CAP_SVC,COB,COINS,COPAY,DAYSUPP,DEDUCT,DISPFEE,
EMPZIP,GENERID,INGCOST,METQTY,MHSACOVG,NETPAY,NTWKPROV,PAIDNTWK,PAY,PDDATE,PHARMID,PLANTYP,QTY,REFILL,RXMR,SALETAX,
THERCLS,DAWIND,DEACLAS,GENIND,MAINTIN,THERGRP,REGION,
DATATYP,PLANKEY,WGTKEY,AGEGRP,EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,SEX,HLTHPLAN,INDSTRY

v2 Fields: (Drop PLANKEY and WGTKEY)

SEQNUM,VERSION,EFAMID,ENROLID,NDCNUM,SVCDATE,DOBYR,YEAR,AGE,AWP,CAP_SVC,COB,COINS,COPAY,DAYSUPP,DEDUCT,DISPFEE,
EMPZIP,GENERID,INGCOST,METQTY,MHSACOVG,NETPAY,NTWKPROV,PAIDNTWK,PAY,PDDATE,PHARMID,PLANTYP,QTY,REFILL,RXMR,SALETAX,
THERCLS,DAWIND,DEACLAS,GENIND,MAINTIN,THERGRP,REGION,
DATATYP,AGEGRP,EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,SEX,HLTHPLAN,INDSTRY 

*/

drop table truven.mdcrd;
CREATE TABLE truven.mdcrd (
	seqnum numeric NULL,
	version int2 NULL,
	efamid numeric NULL,
	enrolid numeric NULL,
	ndcnum numeric null,
	svcdate date null,
	dobyr numeric NULL,
	year numeric NULL,
	age numeric NULL,
	awp numeric null,
	cap_svc bpchar(1) NULL,
	cob numeric NULL,
	coins numeric NULL,
	copay numeric NULL,
	daysupp numeric null,
	deduct numeric NULL,
	dispfee numeric null,
	
	empzip numeric null,
	generid numeric null,
	ingcost numeric null,
	metqty numeric null,
	mhsacovg int2 NULL,
	netpay numeric NULL,
	ntwkprov bpchar(1) NULL,
	paidntwk bpchar(1) NULL,
	pay numeric NULL,
	pddate date NULL,
	pharmid numeric null,
	plantyp numeric NULL,
	qty numeric null,
	refill numeric null,
	rxmr bpchar(5) null,
	saletax numeric null,
	
	thercls numeric null,
	dawind bpchar(5) null,
	deaclas bpchar(5) null,
	genind  bpchar(5) null,
	maintin bpchar(5) null,
	thergrp bpchar(5) null,
	region int2 null,
	
	
	datatyp numeric null,
	plankey numeric null,
	wgtkey numeric null,
	agegrp int2 null,
	eeclass int2 null,
	eestatu int2 null,
	egeoloc int2 null,
	eidflag int2 null,
	emprel int2 null,
	enrflag int2 null,
	phyflag int2 null,
	sex int2 null,
	hlthplan int2 null,
	indstry bpchar(5) null
)
DISTRIBUTED RANDOMLY;

drop external table ext_mdcrd_v1;
CREATE EXTERNAL TABLE ext_mdcrd_v1 (
	seqnum numeric ,
	version int2 ,
	efamid numeric ,
	enrolid numeric ,
	ndcnum numeric ,
	svcdate date ,
	dobyr numeric ,
	year numeric ,
	age numeric ,
	awp numeric ,
	cap_svc bpchar(1) ,
	cob numeric ,
	coins numeric ,
	copay numeric ,
	daysupp numeric ,
	deduct numeric ,
	dispfee numeric ,
	
	empzip numeric ,
	generid numeric ,
	ingcost numeric ,
	metqty numeric ,
	mhsacovg int2 ,
	netpay numeric ,
	ntwkprov bpchar(1) ,
	paidntwk bpchar(1) ,
	pay numeric ,
	pddate date ,
	pharmid numeric ,
	plantyp numeric ,
	qty numeric ,
	refill numeric ,
	rxmr bpchar(5) ,
	saletax numeric ,
	
	thercls numeric ,
	dawind bpchar(5) ,
	deaclas bpchar(5) ,
	genind  bpchar(5) ,
	maintin bpchar(5) ,
	thergrp bpchar(5) ,
	region int2 ,
	
	
	datatyp numeric ,
	plankey numeric ,
	wgtkey numeric ,
	agegrp int2 ,
	eeclass int2 ,
	eestatu int2 ,
	egeoloc int2 ,
	eidflag int2 ,
	emprel int2 ,
	enrflag int2 ,
	phyflag int2 ,
	sex int2 ,
	hlthplan int2 ,
	indstry bpchar(5) 
) 
LOCATION ( 
'gpfdist://c252-140:8801/*'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_mdcrd_v1
limit 1000;

truncate table truven.mdcrd;

insert into truven.mdcrd (SEQNUM,VERSION,EFAMID,ENROLID,NDCNUM,SVCDATE,DOBYR,YEAR,AGE,AWP,CAP_SVC,COB,COINS,COPAY,DAYSUPP,DEDUCT,DISPFEE,
EMPZIP,GENERID,INGCOST,METQTY,MHSACOVG,NETPAY,NTWKPROV,PAIDNTWK,PAY,PDDATE,PHARMID,PLANTYP,QTY,REFILL,RXMR,SALETAX,
THERCLS,DAWIND,DEACLAS,GENIND,MAINTIN,THERGRP,REGION,
DATATYP,PLANKEY,WGTKEY,AGEGRP,EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,SEX,HLTHPLAN,INDSTRY)
select SEQNUM,VERSION,EFAMID,ENROLID,NDCNUM,SVCDATE,DOBYR,YEAR,AGE,AWP,CAP_SVC,COB,COINS,COPAY,DAYSUPP,DEDUCT,DISPFEE,
EMPZIP,GENERID,INGCOST,METQTY,MHSACOVG,NETPAY,NTWKPROV,PAIDNTWK,PAY,PDDATE,PHARMID,PLANTYP,QTY,REFILL,RXMR,SALETAX,
THERCLS,DAWIND,DEACLAS,GENIND,MAINTIN,THERGRP,REGION,
DATATYP,PLANKEY,WGTKEY,AGEGRP,EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,SEX,HLTHPLAN,INDSTRY
from ext_mdcrd_v1;

drop external table ext_mdcrd_v2;
CREATE EXTERNAL TABLE ext_mdcrd_v2 (
	seqnum numeric ,
	version int2 ,
	efamid numeric ,
	enrolid numeric ,
	ndcnum numeric ,
	svcdate date ,
	dobyr numeric ,
	year numeric ,
	age numeric ,
	awp numeric ,
	cap_svc bpchar(1) ,
	cob numeric ,
	coins numeric ,
	copay numeric ,
	daysupp numeric ,
	deduct numeric ,
	dispfee numeric ,
	
	empzip numeric ,
	generid numeric ,
	ingcost numeric ,
	metqty numeric ,
	mhsacovg int2 ,
	netpay numeric ,
	ntwkprov bpchar(1) ,
	paidntwk bpchar(1) ,
	pay numeric ,
	pddate date ,
	pharmid numeric ,
	plantyp numeric ,
	qty numeric ,
	refill numeric ,
	rxmr bpchar(5) ,
	saletax numeric ,
	
	thercls numeric ,
	dawind bpchar(5) ,
	deaclas bpchar(5) ,
	genind  bpchar(5) ,
	maintin bpchar(5) ,
	thergrp bpchar(5) ,
	region int2 ,
	
	
	datatyp numeric ,
	agegrp int2 ,
	eeclass int2 ,
	eestatu int2 ,
	egeoloc int2 ,
	eidflag int2 ,
	emprel int2 ,
	enrflag int2 ,
	phyflag int2 ,
	sex int2 ,
	hlthplan int2 ,
	indstry bpchar(5) 
) 
LOCATION ( 
'gpfdist://greenplum01:8081/uthealth/truven/*/MDCRD*'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_mdcrd_v2
limit 1000;

insert into truven.mdcrd (SEQNUM,VERSION,EFAMID,ENROLID,NDCNUM,SVCDATE,DOBYR,YEAR,AGE,AWP,CAP_SVC,COB,COINS,COPAY,DAYSUPP,DEDUCT,DISPFEE,
EMPZIP,GENERID,INGCOST,METQTY,MHSACOVG,NETPAY,NTWKPROV,PAIDNTWK,PAY,PDDATE,PHARMID,PLANTYP,QTY,REFILL,RXMR,SALETAX,
THERCLS,DAWIND,DEACLAS,GENIND,MAINTIN,THERGRP,REGION,
DATATYP,AGEGRP,EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,SEX,HLTHPLAN,INDSTRY )
select SEQNUM,VERSION,EFAMID,ENROLID,NDCNUM,SVCDATE,DOBYR,YEAR,AGE,AWP,CAP_SVC,COB,COINS,COPAY,DAYSUPP,DEDUCT,DISPFEE,
EMPZIP,GENERID,INGCOST,METQTY,MHSACOVG,NETPAY,NTWKPROV,PAIDNTWK,PAY,PDDATE,PHARMID,PLANTYP,QTY,REFILL,RXMR,SALETAX,
THERCLS,DAWIND,DEACLAS,GENIND,MAINTIN,THERGRP,REGION,
DATATYP,AGEGRP,EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,SEX,HLTHPLAN,INDSTRY 
from ext_mdcrd_v2;

-- Verify

select count(*), min(year), max(year) from truven.mdcrd;

-- Fix storage options
create table truven.mdcrd_2019
WITH (appendonly=true, orientation=column, compresstype=zlib)
as (select * from truven.mdcrd where year=2019)
distributed randomly;

delete from truven.mdcrd where year>=2019;

drop table truven.mdcrd;
alter table truven.mdcrd_new rename to mdcrd;

