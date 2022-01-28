/* ******************************************************************************************************
 *  Collection of queries for exploring database settings and performance/resource usage
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wallingTACC  || 10/27/2021 || Merging mdcr and mdcr into single script.  find/replace to switch between the two as fields are equal
 * ******************************************************************************************************
 */

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

vDMS Fields: (Drops EMPZIP, adds MSA)

SEQNUM,VERSION,EFAMID,ENROLID,NDCNUM,SVCDATE,DOBYR,YEAR,AGE,AWP,CAP_SVC,COB,COINS,COPAY,DAYSUPP,DEDUCT,DISPFEE,
GENERID,INGCOST,METQTY,MHSACOVG,NETPAY,NTWKPROV,PAIDNTWK,PAY,PDDATE,PHARMID,PLANTYP,QTY,REFILL,RXMR,SALETAX,THERCLS,
DAWIND,DEACLAS,GENIND,MAINTIN,THERGRP,REGION,MSA,DATATYP,PLANKEY,WGTKEY,AGEGRP,EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,
ENRFLAG,PHYFLAG,SEX,HLTHPLAN,INDSTRY

v3 fields: Added MEDADV
SEQNUM,VERSION,EFAMID,ENROLID,NDCNUM,SVCDATE,DOBYR,YEAR,AGE,AWP,CAP_SVC,COB,COINS,COPAY,DAYSUPP,DEDUCT,DISPFEE,
EMPZIP,GENERID,INGCOST,METQTY,MHSACOVG,NETPAY,NTWKPROV,PAIDNTWK,PAY,PDDATE,PHARMID,PLANTYP,QTY,REFILL,RXMR,SALETAX,
THERCLS,DAWIND,DEACLAS,GENIND,MAINTIN,THERGRP,REGION,
DATATYP,AGEGRP,EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,SEX,HLTHPLAN,INDSTRY 

v4 fields: Moved MEDADV to end
**/

create table truven.mdcrd (like truven.mdcrd)
WITH (appendonly=true, orientation=column, compresstype=zlib);

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
	msa bpchar(7) ,
	
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

/*
 * V4
 */

 alter table truven.mdcrd add column medadv int2;

drop external table ext_mdcrd;
CREATE EXTERNAL TABLE ext_mdcrd (
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
	indstry bpchar(5),
	medadv int2
) 
LOCATION ( 
'gpfdist://greenplum01:8081/uthealth/truven/MDCRD*'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_mdcrd
limit 1000;

insert into truven.mdcrd (SEQNUM,VERSION,EFAMID,ENROLID,NDCNUM,SVCDATE,DOBYR,YEAR,AGE,AWP,CAP_SVC,COB,COINS,COPAY,DAYSUPP,DEDUCT,DISPFEE,
EMPZIP,GENERID,INGCOST,METQTY,MHSACOVG,NETPAY,NTWKPROV,PAIDNTWK,PAY,PDDATE,PHARMID,PLANTYP,QTY,REFILL,RXMR,SALETAX,
THERCLS,DAWIND,DEACLAS,GENIND,MAINTIN,THERGRP,REGION,
DATATYP,MEDADV,AGEGRP,EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,SEX,HLTHPLAN,INDSTRY )
select SEQNUM,VERSION,EFAMID,ENROLID,NDCNUM,SVCDATE,DOBYR,YEAR,AGE,AWP,CAP_SVC,COB,COINS,COPAY,DAYSUPP,DEDUCT,DISPFEE,
EMPZIP,GENERID,INGCOST,METQTY,MHSACOVG,NETPAY,NTWKPROV,PAIDNTWK,PAY,PDDATE,PHARMID,PLANTYP,QTY,REFILL,RXMR,SALETAX,
THERCLS,DAWIND,DEACLAS,GENIND,MAINTIN,THERGRP,REGION,
DATATYP,MEDADV,AGEGRP,EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,SEX,HLTHPLAN,INDSTRY 
from ext_mdcrd;


-- Verify

select count(*), min(year), max(year)  from truven.mdcrd;

-- Truncate and Reload

delete from truven.mdcrd where year>=2020;


