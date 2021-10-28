/* ******************************************************************************************************
 *  Collection of queries for exploring database settings and performance/resource usage
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wallingTACC  || 10/27/2021 || Merging CCAE and MDCR into single script.  find/replace to switch between the two as fields are equal
 * ******************************************************************************************************
 */
/*
v1 Fields:

SEQNUM,VERSION,EFAMID,ENROLID,DTEND,DTSTART,EMPZIP,MEMDAYS,MHSACOVG,PLANTYP,YEAR,AGE,DOBYR,REGION,
DATATYP,PLANKEY,WGTKEY,AGEGRP,EECLASS,EESTATU,EGEOLOC,EMPREL,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY

v2 Fields: (Drop PLANKEY and WGTKEY)

SEQNUM,VERSION,EFAMID,ENROLID,DTEND,DTSTART,EMPZIP,MEMDAYS,MHSACOVG,PLANTYP,YEAR,AGE,DOBYR,REGION,
DATATYP,AGEGRP,EECLASS,EESTATU,EGEOLOC,EMPREL,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY

v3 fields: (Added MEDADV)
SEQNUM,VERSION,EFAMID,ENROLID,DTEND,DTSTART,EMPZIP,MEMDAYS,MHSACOVG,PLANTYP,YEAR,AGE,DOBYR,REGION,
DATATYP,MEDADV,AGEGRP,EECLASS,EESTATU,EGEOLOC,EMPREL,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY

*/

/*
 * V1
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
	msa numeric null,
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

/*
 * V3
 */
alter table truven.ccaet add column medadv int2;

drop external table ext_ccaet;
CREATE EXTERNAL TABLE ext_ccaet (
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
	medadv int2,
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
'gpfdist://greenplum01:8081/uthealth/truven/CCAET*'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select min(dtstart), max(dtstart)
from ext_ccaet;
limit 1000;

insert into truven.ccaet (SEQNUM,VERSION,EFAMID,ENROLID,DTEND,DTSTART,EMPZIP,MEMDAYS,MHSACOVG,PLANTYP,YEAR,AGE,DOBYR,REGION,
DATATYP,MEDADV, AGEGRP,EECLASS,EESTATU,EGEOLOC,EMPREL,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY)
select SEQNUM,VERSION,EFAMID,ENROLID,DTEND,DTSTART,EMPZIP,MEMDAYS,MHSACOVG,PLANTYP,YEAR,AGE,DOBYR,REGION,
DATATYP,MEDADV,AGEGRP,EECLASS,EESTATU,EGEOLOC,EMPREL,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY
from ext_ccaet;


-- Verify

select count(*), min(year), max(year)
from truven.ccaet;



-- Fix storage options
create table truven.ccaet_2019
WITH (appendonly=true, orientation=column, compresstype=zlib)
as (select * from truven.ccaet where year=2019)
distributed randomly;

delete from truven.ccaet where year>=2020;

--drop table truven.ccaet;
--alter table truven.ccaet_new rename to ccaet;

select distinct empzip
from truven.ccaet;

delete from truven.ccae
