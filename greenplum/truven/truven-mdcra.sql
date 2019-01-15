/*
v1 Fields:

SEQNUM,VERSION,EFAMID,ENROLID,MEMDAYS,YEAR,AGE,DOBYR,AGEGRP,EMPREL,PHYFLAG,RX,SEX,HLTHPLAN,ENRMON,
DATTYP1,DATTYP2,DATTYP3,DATTYP4,DATTYP5,DATTYP6,DATTYP7,DATTYP8,DATTYP9,DATTYP10,DATTYP11,DATTYP12,
ENRIND1,ENRIND2,ENRIND3,ENRIND4,ENRIND5,ENRIND6,ENRIND7,ENRIND8,ENRIND9,ENRIND10,ENRIND11,ENRIND12,
MEMDAY1,MEMDAY2,MEMDAY3,MEMDAY4,MEMDAY5,MEMDAY6,MEMDAY7,MEMDAY8,MEMDAY9,MEMDAY10,MEMDAY11,MEMDAY12,
PLNKEY1,PLNKEY2,PLNKEY3,PLNKEY4,PLNKEY5,PLNKEY6,PLNKEY7,PLNKEY8,PLNKEY9,PLNKEY10,PLNKEY11,PLNKEY12,
PLNTYP1,PLNTYP2,PLNTYP3,PLNTYP4,PLNTYP5,PLNTYP6,PLNTYP7,PLNTYP8,PLNTYP9,PLNTYP10,PLNTYP11,PLNTYP12,
EECLASS,EESTATU,EGEOLOC,EMPZIP,INDSTRY,MHSACOVG,REGION,WGTKEY

v2 Fields: (missing all PLNKEY* fields, also renamed WGTKEY)

SEQNUM,VERSION,EFAMID,ENROLID,MEMDAYS,YEAR,AGE,DOBYR,AGEGRP,EMPREL,PHYFLAG,RX,SEX,HLTHPLAN,ENRMON,
DATTYP1,DATTYP2,DATTYP3,DATTYP4,DATTYP5,DATTYP6,DATTYP7,DATTYP8,DATTYP9,DATTYP10,DATTYP11,DATTYP12,
ENRIND1,ENRIND2,ENRIND3,ENRIND4,ENRIND5,ENRIND6,ENRIND7,ENRIND8,ENRIND9,ENRIND10,ENRIND11,ENRIND12,
MEMDAY1,MEMDAY2,MEMDAY3,MEMDAY4,MEMDAY5,MEMDAY6,MEMDAY7,MEMDAY8,MEMDAY9,MEMDAY10,MEMDAY11,MEMDAY12,
PLNTYP1,PLNTYP2,PLNTYP3,PLNTYP4,PLNTYP5,PLNTYP6,PLNTYP7,PLNTYP8,PLNTYP9,PLNTYP10,PLNTYP11,PLNTYP12,
EECLASS,EESTATU,EGEOLOC,EMPZIP,INDSTRY,MHSACOVG,REGION,MSWGTKEY

*/

drop table truven.mdcra;
CREATE TABLE truven.mdcra (
	seqnum numeric NULL,
	version int2 NULL,
	efamid numeric NULL,
	enrolid numeric NULL,
	memdays numeric null,
	year numeric NULL,
	age numeric NULL,
	dobyr numeric NULL,
	agegrp int2 NULL,
	emprel int2 NULL,
	phyflag int2 NULL,
	rx int2 NULL,
	sex int2 NULL,
	hlthplan int2 NULL,
	enrmon numeric null,
	
	dattyp1 numeric null,dattyp2 numeric null,dattyp3 numeric null,dattyp4 numeric null,dattyp5 numeric null,dattyp6 numeric null,dattyp7 numeric null,dattyp8 numeric null,dattyp9 numeric null,dattyp10 numeric null,dattyp11 numeric null,dattyp12 numeric null,
	enrind1 numeric null,enrind2 numeric null,enrind3 numeric null,enrind4 numeric null,enrind5 numeric null,enrind6 numeric null,enrind7 numeric null,enrind8 numeric null,enrind9 numeric null,enrind10 numeric null,enrind11 numeric null,enrind12 numeric null,
	memday1 numeric null,memday2 numeric null,memday3 numeric null,memday4 numeric null,memday5 numeric null,memday6 numeric null,memday7 numeric null,memday8 numeric null,memday9 numeric null,memday10 numeric null,memday11 numeric null,memday12 numeric null,
	plnkey1 numeric null,plnkey2 numeric null,plnkey3 numeric null,plnkey4 numeric null,plnkey5 numeric null,plnkey6 numeric null,plnkey7 numeric null,plnkey8 numeric null,plnkey9 numeric null,plnkey10 numeric null,plnkey11 numeric null,plnkey12 numeric null,
	plntyp1 numeric null,plntyp2 numeric null,plntyp3 numeric null,plntyp4 numeric null,plntyp5 numeric null,plntyp6 numeric null,plntyp7 numeric null,plntyp8 numeric null,plntyp9 numeric null,plntyp10 numeric null,plntyp11 numeric null,plntyp12 numeric null,

	eeclass int2 NULL,
	eestatu int2 NULL,
	egeoloc int2 NULL,
	empzip numeric null,
	indstry bpchar(5) null,
	mhsacovg bpchar(5) null,
	region int2 NULL,
	wgtkey numeric null,
	mswgtkey numeric NULL
)
DISTRIBUTED RANDOMLY;

drop external table ext_mdcra_v1;
CREATE EXTERNAL TABLE ext_mdcra_v1 (
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
from ext_mdcra_v1
limit 1000;

truncate table truven.mdcra;

insert into truven.mdcra (SEQNUM,VERSION,EFAMID,ENROLID,MEMDAYS,YEAR,AGE,DOBYR,AGEGRP,EMPREL,PHYFLAG,RX,SEX,HLTHPLAN,ENRMON,
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
from ext_mdcra_v1;


drop external table ext_mdcra_v2;
CREATE EXTERNAL TABLE ext_mdcra_v2 (
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
from ext_mdcra_v2
limit 1000;

insert into truven.mdcra (SEQNUM,VERSION,EFAMID,ENROLID,MEMDAYS,YEAR,AGE,DOBYR,AGEGRP,EMPREL,PHYFLAG,RX,SEX,HLTHPLAN,ENRMON,
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
from ext_mdcra_v2;

-- Verify

select count(*) from truven.mdcra;



