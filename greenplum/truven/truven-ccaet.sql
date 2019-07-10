/*
v1 Fields:

SEQNUM,VERSION,EFAMID,ENROLID,DTEND,DTSTART,EMPZIP,MEMDAYS,MHSACOVG,PLANTYP,YEAR,AGE,DOBYR,REGION,
DATATYP,PLANKEY,WGTKEY,AGEGRP,EECLASS,EESTATU,EGEOLOC,EMPREL,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY

v2 Fields: (Drop PLANKEY and WGTKEY)

SEQNUM,VERSION,EFAMID,ENROLID,DTEND,DTSTART,EMPZIP,MEMDAYS,MHSACOVG,PLANTYP,YEAR,AGE,DOBYR,REGION,
DATATYP,AGEGRP,EECLASS,EESTATU,EGEOLOC,EMPREL,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY

*/

drop table truven.ccaet;
CREATE TABLE truven.ccaet (
	seqnum numeric NULL,
	version int2 NULL,
	efamid numeric NULL,
	enrolid numeric NULL,
	dtend date null,
	dtstart date null,
	empzip numeric null,
	memdays numeric null,
	mhsacovg int2 NULL,
	plantyp numeric NULL,
	year numeric NULL,
	age numeric NULL,
	dobyr numeric NULL,
	region int2 null,
	
	datatyp numeric null,
	plankey numeric null,
	wgtkey numeric null,
	agegrp int2 null,
	eeclass int2 null,
	eestatu int2 null,
	egeoloc int2 null,
	emprel int2 null,
	phyflag int2 null,
	rx int2 null,
	sex int2 null,
	hlthplan int2 null,
	indstry bpchar(5) null
)
DISTRIBUTED RANDOMLY;

drop external table ext_ccaet_v1;
CREATE EXTERNAL TABLE ext_ccaet_v1 (
	seqnum numeric ,
	version int2 ,
	efamid numeric ,
	enrolid numeric ,
	dtend date ,
	dtstart date ,
	empzip numeric ,
	memdays numeric ,
	mhsacovg int2 ,
	plantyp numeric ,
	year numeric ,
	age numeric ,
	dobyr numeric ,
	region int2 ,
	
	datatyp numeric ,
	plankey numeric ,
	wgtkey numeric ,
	agegrp int2 ,
	eeclass int2 ,
	eestatu int2 ,
	egeoloc int2 ,
	emprel int2 ,
	phyflag int2 ,
	rx int2 ,
	sex int2 ,
	hlthplan int2 ,
	indstry bpchar(5) 
) 
LOCATION ( 
'gpfdist://c252-140:8801/*'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_ccaet_v1
limit 1000;

truncate table truven.ccaet;

insert into truven.ccaet (SEQNUM,VERSION,EFAMID,ENROLID,DTEND,DTSTART,EMPZIP,MEMDAYS,MHSACOVG,PLANTYP,YEAR,AGE,DOBYR,REGION,
DATATYP,PLANKEY,WGTKEY,AGEGRP,EECLASS,EESTATU,EGEOLOC,EMPREL,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY)
select SEQNUM,VERSION,EFAMID,ENROLID,DTEND,DTSTART,EMPZIP,MEMDAYS,MHSACOVG,PLANTYP,YEAR,AGE,DOBYR,REGION,
DATATYP,PLANKEY,WGTKEY,AGEGRP,EECLASS,EESTATU,EGEOLOC,EMPREL,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY
from ext_ccaet_v1;

drop external table ext_ccaet_v2;
CREATE EXTERNAL TABLE ext_ccaet_v2 (
	seqnum numeric ,
	version int2 ,
	efamid numeric ,
	enrolid numeric ,
	dtend date ,
	dtstart date ,
	empzip numeric ,
	memdays numeric ,
	mhsacovg int2 ,
	plantyp numeric ,
	year numeric ,
	age numeric ,
	dobyr numeric ,
	region int2 ,
	
	datatyp numeric ,
	agegrp int2 ,
	eeclass int2 ,
	eestatu int2 ,
	egeoloc int2 ,
	emprel int2 ,
	phyflag int2 ,
	rx int2 ,
	sex int2 ,
	hlthplan int2 ,
	indstry bpchar(5) 
) 
LOCATION ( 
'gpfdist://c252-140:8801/ccaet*'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_ccaet_v2
limit 1000;

insert into truven.ccaet (SEQNUM,VERSION,EFAMID,ENROLID,DTEND,DTSTART,EMPZIP,MEMDAYS,MHSACOVG,PLANTYP,YEAR,AGE,DOBYR,REGION,
DATATYP,AGEGRP,EECLASS,EESTATU,EGEOLOC,EMPREL,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY)
select SEQNUM,VERSION,EFAMID,ENROLID,DTEND,DTSTART,EMPZIP,MEMDAYS,MHSACOVG,PLANTYP,YEAR,AGE,DOBYR,REGION,
DATATYP,AGEGRP,EECLASS,EESTATU,EGEOLOC,EMPREL,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY
from ext_ccaet_v2;

-- Verify

select count(*), min(year), max(year)
from truven.ccaet;



-- Fix storage options
create table truven.ccaet_new 
WITH (appendonly=true, orientation=column)
as (select * from truven.ccaet)
distributed randomly;

drop table truven.ccaet;
alter table truven.ccaet_new rename to ccaet;

