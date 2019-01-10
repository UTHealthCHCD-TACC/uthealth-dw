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

drop table truven.ccaed;
CREATE TABLE truven.ccaed (
	seqnum numeric NULL,
	version int2 NULL,
	efamid numeric NULL,
	enrolid numeric NULL,
	ndcnum numeric null,
	svcdate date null,
	dobyr numeric NULL,
	year numeric NULL,
	age numeric NULL,
	awp null,
	cap_svc bpchar(1) NULL,
	cob numeric NULL,
	coins numeric NULL,
	copay numeric NULL,
	daysupp numeric null,
	deduct numeric NULL,
	dispfee numeric null,
	
	empzip numeric null,
	generid null,
	ingcost numeric null,
	metqty numeric null,
	mhsacovg int2 NULL,
	netpay numeric NULL,
	ntwkprov bpchar(1) NULL,
	paidntwk bpchar(1) NULL,
	pay numeric NULL,
	pddate date NULL,
	pharmid null,
	plantyp numeric NULL,
	qty numeric null,
	refill null,
	rxmr null,
	saletax numeric null,
	
	thercls null,
	dawind null,
	deaclas null,
	genind null,
	maintin null,
	thergrp null,
	region int2 null,
	
	
	datatyp numeric,
	plankey numeric,
	wgtkey numeric,
	agegrp int2,
	eeclass int2,
	eestatu int2,
	egeoloc int2,
	eidflag int2,
	emprel int2,
	enrflag int2,
	phyflag int2,
	rx int2,
	sex int2,
	hlthplan int2,
	indstry bpchar(5)
)
DISTRIBUTED RANDOMLY;

drop external table ext_ccaed_v1;
CREATE EXTERNAL TABLE ext_ccaed_v1 (
	seqnum numeric ,
	version int2 ,
	efamid numeric ,
	enrolid numeric ,
	memdays numeric ,
	year numeric ,
	age numeric ,
	dobyr numeric ,
	agegrp int2 ,
	emprel int2 ,
	phyflag int2 ,
	rx int2 ,
	sex int2 ,
	hlthplan int2 ,
	enrmon numeric ,
	
	dattyp1 numeric ,dattyp2 numeric ,dattyp3 numeric ,dattyp4 numeric ,dattyp5 numeric ,dattyp6 numeric ,dattyp7 numeric ,dattyp8 numeric ,dattyp9 numeric ,dattyp10 numeric ,dattyp11 numeric ,dattyp12 numeric ,
	enrind1 numeric ,enrind2 numeric ,enrind3 numeric ,enrind4 numeric ,enrind5 numeric ,enrind6 numeric ,enrind7 numeric ,enrind8 numeric ,enrind9 numeric ,enrind10 numeric ,enrind11 numeric ,enrind12 numeric ,
	memday1 numeric ,memday2 numeric ,memday3 numeric ,memday4 numeric ,memday5 numeric ,memday6 numeric ,memday7 numeric ,memday8 numeric ,memday9 numeric ,memday10 numeric ,memday11 numeric ,memday12 numeric ,
	plnkey1 numeric ,plnkey2 numeric ,plnkey3 numeric ,plnkey4 numeric ,plnkey5 numeric ,plnkey6 numeric ,plnkey7 numeric ,plnkey8 numeric ,plnkey9 numeric ,plnkey10 numeric ,plnkey11 numeric ,plnkey12 numeric ,
	plntyp1 numeric ,plntyp2 numeric ,plntyp3 numeric ,plntyp4 numeric ,plntyp5 numeric ,plntyp6 numeric ,plntyp7 numeric ,plntyp8 numeric ,plntyp9 numeric ,plntyp10 numeric ,plntyp11 numeric ,plntyp12 numeric ,

	eeclass int2 ,
	eestatu int2 ,
	egeoloc int2 ,
	empzip numeric ,
	indstry bpchar(5) ,
	mhsacovg bpchar(5) ,
	region int2 ,
	wgtkey numeric 
) 
LOCATION ( 
'gpfdist://c252-140:8801/*'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_ccaed_v1
limit 1000;

truncate table truven.ccaed;

insert into truven.ccaed (SEQNUM,VERSION,EFAMID,ENROLID,MEMDAYS,YEAR,AGE,DOBYR,AGEGRP,EMPREL,PHYFLAG,RX,SEX,HLTHPLAN,ENRMON,
DATTYP1,DATTYP2,DATTYP3,DATTYP4,DATTYP5,DATTYP6,DATTYP7,DATTYP8,DATTYP9,DATTYP10,DATTYP11,DATTYP12,
ENRIND1,ENRIND2,ENRIND3,ENRIND4,ENRIND5,ENRIND6,ENRIND7,ENRIND8,ENRIND9,ENRIND10,ENRIND11,ENRIND12,
MEMDAY1,MEMDAY2,MEMDAY3,MEMDAY4,MEMDAY5,MEMDAY6,MEMDAY7,MEMDAY8,MEMDAY9,MEMDAY10,MEMDAY11,MEMDAY12,
PLNKEY1,PLNKEY2,PLNKEY3,PLNKEY4,PLNKEY5,PLNKEY6,PLNKEY7,PLNKEY8,PLNKEY9,PLNKEY10,PLNKEY11,PLNKEY12,
PLNTYP1,PLNTYP2,PLNTYP3,PLNTYP4,PLNTYP5,PLNTYP6,PLNTYP7,PLNTYP8,PLNTYP9,PLNTYP10,PLNTYP11,PLNTYP12,
EECLASS,EESTATU,EGEOLOC,EMPZIP,INDSTRY,MHSACOVG,REGION,WGTKEY)
select SEQNUM,VERSION,EFAMID,ENROLID,MEMDAYS,YEAR,AGE,DOBYR,AGEGRP,EMPREL,PHYFLAG,RX,SEX,HLTHPLAN,ENRMON,
DATTYP1,DATTYP2,DATTYP3,DATTYP4,DATTYP5,DATTYP6,DATTYP7,DATTYP8,DATTYP9,DATTYP10,DATTYP11,DATTYP12,
ENRIND1,ENRIND2,ENRIND3,ENRIND4,ENRIND5,ENRIND6,ENRIND7,ENRIND8,ENRIND9,ENRIND10,ENRIND11,ENRIND12,
MEMDAY1,MEMDAY2,MEMDAY3,MEMDAY4,MEMDAY5,MEMDAY6,MEMDAY7,MEMDAY8,MEMDAY9,MEMDAY10,MEMDAY11,MEMDAY12,
PLNKEY1,PLNKEY2,PLNKEY3,PLNKEY4,PLNKEY5,PLNKEY6,PLNKEY7,PLNKEY8,PLNKEY9,PLNKEY10,PLNKEY11,PLNKEY12,
PLNTYP1,PLNTYP2,PLNTYP3,PLNTYP4,PLNTYP5,PLNTYP6,PLNTYP7,PLNTYP8,PLNTYP9,PLNTYP10,PLNTYP11,PLNTYP12,
EECLASS,EESTATU,EGEOLOC,EMPZIP,INDSTRY,MHSACOVG,REGION,WGTKEY
from ext_ccaed_v1;
limit 1000;

drop external table ext_ccaed_v2;
CREATE EXTERNAL TABLE ext_ccaed_v2 (
	seqnum numeric ,
	version int2 ,
	efamid numeric ,
	enrolid numeric ,
	memdays numeric ,
	year numeric ,
	age numeric ,
	dobyr numeric ,
	agegrp int2 ,
	emprel int2 ,
	phyflag int2 ,
	rx int2 ,
	sex int2 ,
	hlthplan int2 ,
	enrmon numeric ,
	
	dattyp1 numeric ,dattyp2 numeric ,dattyp3 numeric ,dattyp4 numeric ,dattyp5 numeric ,dattyp6 numeric ,dattyp7 numeric ,dattyp8 numeric ,dattyp9 numeric ,dattyp10 numeric ,dattyp11 numeric ,dattyp12 numeric ,
	enrind1 numeric ,enrind2 numeric ,enrind3 numeric ,enrind4 numeric ,enrind5 numeric ,enrind6 numeric ,enrind7 numeric ,enrind8 numeric ,enrind9 numeric ,enrind10 numeric ,enrind11 numeric ,enrind12 numeric ,
	memday1 numeric ,memday2 numeric ,memday3 numeric ,memday4 numeric ,memday5 numeric ,memday6 numeric ,memday7 numeric ,memday8 numeric ,memday9 numeric ,memday10 numeric ,memday11 numeric ,memday12 numeric ,
	plntyp1 numeric ,plntyp2 numeric ,plntyp3 numeric ,plntyp4 numeric ,plntyp5 numeric ,plntyp6 numeric ,plntyp7 numeric ,plntyp8 numeric ,plntyp9 numeric ,plntyp10 numeric ,plntyp11 numeric ,plntyp12 numeric ,

	eeclass int2 ,
	eestatu int2 ,
	egeoloc int2 ,
	empzip numeric ,
	indstry bpchar(5) ,
	mhsacovg bpchar(5) ,
	region int2 ,
	mswgtkey numeric 
) 
LOCATION ( 
'gpfdist://c252-140:8801/*'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_ccaed_v2
limit 1000;

insert into truven.ccaed (SEQNUM,VERSION,EFAMID,ENROLID,MEMDAYS,YEAR,AGE,DOBYR,AGEGRP,EMPREL,PHYFLAG,RX,SEX,HLTHPLAN,ENRMON,
DATTYP1,DATTYP2,DATTYP3,DATTYP4,DATTYP5,DATTYP6,DATTYP7,DATTYP8,DATTYP9,DATTYP10,DATTYP11,DATTYP12,
ENRIND1,ENRIND2,ENRIND3,ENRIND4,ENRIND5,ENRIND6,ENRIND7,ENRIND8,ENRIND9,ENRIND10,ENRIND11,ENRIND12,
MEMDAY1,MEMDAY2,MEMDAY3,MEMDAY4,MEMDAY5,MEMDAY6,MEMDAY7,MEMDAY8,MEMDAY9,MEMDAY10,MEMDAY11,MEMDAY12,
PLNTYP1,PLNTYP2,PLNTYP3,PLNTYP4,PLNTYP5,PLNTYP6,PLNTYP7,PLNTYP8,PLNTYP9,PLNTYP10,PLNTYP11,PLNTYP12,
EECLASS,EESTATU,EGEOLOC,EMPZIP,INDSTRY,MHSACOVG,REGION,MSWGTKEY)
select SEQNUM,VERSION,EFAMID,ENROLID,MEMDAYS,YEAR,AGE,DOBYR,AGEGRP,EMPREL,PHYFLAG,RX,SEX,HLTHPLAN,ENRMON,
DATTYP1,DATTYP2,DATTYP3,DATTYP4,DATTYP5,DATTYP6,DATTYP7,DATTYP8,DATTYP9,DATTYP10,DATTYP11,DATTYP12,
ENRIND1,ENRIND2,ENRIND3,ENRIND4,ENRIND5,ENRIND6,ENRIND7,ENRIND8,ENRIND9,ENRIND10,ENRIND11,ENRIND12,
MEMDAY1,MEMDAY2,MEMDAY3,MEMDAY4,MEMDAY5,MEMDAY6,MEMDAY7,MEMDAY8,MEMDAY9,MEMDAY10,MEMDAY11,MEMDAY12,
PLNTYP1,PLNTYP2,PLNTYP3,PLNTYP4,PLNTYP5,PLNTYP6,PLNTYP7,PLNTYP8,PLNTYP9,PLNTYP10,PLNTYP11,PLNTYP12,
EECLASS,EESTATU,EGEOLOC,EMPZIP,INDSTRY,MHSACOVG,REGION,MSWGTKEY
from ext_ccaed_v2;

-- Verify

select count(*) from truven.ccaed;



