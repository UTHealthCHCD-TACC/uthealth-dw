/* ******************************************************************************************************
 *  Collection of queries for exploring database settings and performance/resource usage
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wallingTACC  || 10/27/2021 || Merging mdcr and MDCR into single script.  find/replace to switch between the two as fields are equal
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

v4 fields: (Moved MEDADV to end)
SEQNUM,VERSION,EFAMID,ENROLID,DTEND,DTSTART,EMPZIP,MEMDAYS,MHSACOVG,PLANTYP,YEAR,AGE,DOBYR,REGION,
DATATYP,AGEGRP,EECLASS,EESTATU,EGEOLOC,EMPREL,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY,MEDADV

*/

/*
 * V1
 */
drop table truven.mdcrt;
CREATE TABLE truven.mdcrt (
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
alter table truven.mdcrt add column medadv int2;

drop external table ext_mdcrt;
CREATE EXTERNAL TABLE ext_mdcrt (
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
	indstry bpchar(5),
	medadv int2
) 
LOCATION ( 
'gpfdist://greenplum01:8081/uthealth/truven/MDCRT*'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

/*
select min(dtstart), max(dtstart)

select *
from ext_mdcrt
limit 1000;

*/

insert into truven.mdcrt (SEQNUM,VERSION,EFAMID,ENROLID,DTEND,DTSTART,EMPZIP,MEMDAYS,MHSACOVG,PLANTYP,YEAR,AGE,DOBYR,REGION,
DATATYP,MEDADV, AGEGRP,EECLASS,EESTATU,EGEOLOC,EMPREL,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY)
select SEQNUM,VERSION,EFAMID,ENROLID,DTEND,DTSTART,EMPZIP,MEMDAYS,MHSACOVG,PLANTYP,YEAR,AGE,DOBYR,REGION,
DATATYP,MEDADV,AGEGRP,EECLASS,EESTATU,EGEOLOC,EMPREL,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY
from ext_mdcrt;


-- Verify

select count(*), min(year), max(year)
from truven.mdcrt;



-- Truncate and Reload

delete from truven.mdcrt where year>=2020;
