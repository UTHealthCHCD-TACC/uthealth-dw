/*

v1 Fields:

ADV_CASE,SEQNUM,CASESTAT,DTINJ,CASEDX,PAYINDM,PAYMED,PAYOTH,DTBEG,DTLAST,PAYTOT,ENROLID,BODYPART,CAUSE,NATURE,CASEID,DAYSABS,DTRTW,RTW_FLAG,DXVER,VERSION


v2 Fields: (Add EFAMID)

ADV_CASE,SEQNUM,CASESTAT,DTINJ,CASEDX,PAYINDM,PAYMED,PAYOTH,DTBEG,DTLAST,PAYTOT,EFAMID,ENROLID,BODYPART,CAUSE,NATURE,CASEID,DAYSABS,DTRTW,RTW_FLAG,VERSION


v3 Fields: (Add DXVER)

ADV_CASE,SEQNUM,CASESTAT,DTINJ,CASEDX,PAYINDM,PAYMED,PAYOTH,DTBEG,DTLAST,PAYTOT,EFAMID,ENROLID,BODYPART,CAUSE,NATURE,CASEID,DAYSABS,DTRTW,RTW_FLAG,DXVER,VERSION


*/

drop table truven.hpm_wc;
CREATE TABLE truven.hpm_wc (
	year int2,
	adv_case bpchar(30) null,
	seqnum numeric NULL,	
	casestat bpchar(1) null,
	dtinj date null,
	casedx bpchar(10) null,
	payindm numeric null,
	paymed numeric null,
	payoth numeric null,
	dtbeg date null,
	dtlast date NULL,
	paytot numeric null,
	efamid numeric null,
	enrolid numeric NULL,
	bodypart bpchar(255) null,
	cause bpchar(255) null,
	nature bpchar(255) null,
	caseid numeric null,
	daysabs numeric null,
	dtrtw date null,
	rtw_flag bpchar(1) null,
	dxver bpchar(1) null,
	version int2 NULL
	
)
DISTRIBUTED RANDOMLY;

drop external table ext_wc_v1;
CREATE EXTERNAL TABLE ext_wc_v1 (
	adv_case bpchar(30) ,
	seqnum numeric ,	
	casestat bpchar(1) ,
	dtinj date ,
	casedx bpchar(10) ,
	payindm numeric ,
	paymed numeric ,
	payoth numeric ,
	dtbeg date ,
	dtlast date ,
	paytot numeric ,
	enrolid numeric ,
	bodypart bpchar(255) ,
	cause bpchar(255) ,
	nature bpchar(255) ,
	caseid numeric ,
	daysabs numeric ,
	dtrtw date ,
	rtw_flag bpchar(1) ,
	version int2 
) 
LOCATION ( 
'gpfdist://greenplum01:8081/uthealth/truven/HPM/CSV/V1/WC*.CSV'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_wc_v1
limit 1000;

truncate table truven.hpm_wc;

insert into truven.hpm_wc (ADV_CASE,SEQNUM,CASESTAT,DTINJ,CASEDX,PAYINDM,PAYMED,PAYOTH,DTBEG,DTLAST,PAYTOT,ENROLID,BODYPART,CAUSE,NATURE,CASEID,DAYSABS,DTRTW,RTW_FLAG,VERSION)
select ADV_CASE,SEQNUM,CASESTAT,DTINJ,CASEDX,PAYINDM,PAYMED,PAYOTH,DTBEG,DTLAST,PAYTOT,ENROLID,BODYPART,CAUSE,NATURE,CASEID,DAYSABS,DTRTW,RTW_FLAG,VERSION
from ext_wc_v1;

drop external table ext_wc_v2;
CREATE EXTERNAL TABLE ext_wc_v2 (
	adv_case bpchar(30) ,
	seqnum numeric ,	
	casestat bpchar(1) ,
	dtinj date ,
	casedx bpchar(10) ,
	payindm numeric ,
	paymed numeric ,
	payoth numeric ,
	dtbeg date ,
	dtlast date ,
	paytot numeric ,
	efamid numeric ,
	enrolid numeric ,
	bodypart bpchar(255) ,
	cause bpchar(255) ,
	nature bpchar(255) ,
	caseid numeric ,
	daysabs numeric ,
	dtrtw date ,
	rtw_flag bpchar(1) ,
	version int2 
) 
LOCATION ( 
'gpfdist://greenplum01:8081/uthealth/truven/HPM/CSV/V2/WC*.CSV'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

--select *
--from ext_wc_v2
--limit 1000;

insert into truven.hpm_wc (ADV_CASE,SEQNUM,CASESTAT,DTINJ,CASEDX,PAYINDM,PAYMED,PAYOTH,DTBEG,DTLAST,PAYTOT,EFAMID,ENROLID,BODYPART,CAUSE,NATURE,CASEID,DAYSABS,DTRTW,RTW_FLAG,VERSION)
select ADV_CASE,SEQNUM,CASESTAT,DTINJ,CASEDX,PAYINDM,PAYMED,PAYOTH,DTBEG,DTLAST,PAYTOT,EFAMID,ENROLID,BODYPART,CAUSE,NATURE,CASEID,DAYSABS,DTRTW,RTW_FLAG,VERSION 
from ext_wc_v2;


drop external table ext_wc_v3;
CREATE EXTERNAL TABLE ext_wc_v3 (
	adv_case bpchar(30) ,
	seqnum numeric ,	
	casestat bpchar(1) ,
	dtinj date ,
	casedx bpchar(10) ,
	payindm numeric ,
	paymed numeric ,
	payoth numeric ,
	dtbeg date ,
	dtlast date ,
	paytot numeric ,
	efamid numeric ,
	enrolid numeric ,
	bodypart bpchar(255) ,
	cause bpchar(255) ,
	nature bpchar(255) ,
	caseid numeric ,
	daysabs numeric ,
	dtrtw date ,
	rtw_flag bpchar(1) ,
	dxver bpchar(1) ,
	version int2 
) 
LOCATION ( 
'gpfdist://greenplum01:8081/uthealth/truven/HPM/CSV/V3/WC*.CSV'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

/*
select *
from ext_wc_v3
limit 1000;
*/

insert into truven.hpm_wc (ADV_CASE,SEQNUM,CASESTAT,DTINJ,CASEDX,PAYINDM,PAYMED,PAYOTH,DTBEG,DTLAST,PAYTOT,EFAMID,ENROLID,BODYPART,CAUSE,NATURE,CASEID,DAYSABS,DTRTW,RTW_FLAG,DXVER,VERSION)
select ADV_CASE,SEQNUM,CASESTAT,DTINJ,CASEDX,PAYINDM,PAYMED,PAYOTH,DTBEG,DTLAST,PAYTOT,EFAMID,ENROLID,BODYPART,CAUSE,NATURE,CASEID,DAYSABS,DTRTW,RTW_FLAG,DXVER,VERSION 
from ext_wc_v3;


-- Verify

select count(*) from truven.hpm_wc;

-- Fix storage options
create table truven.hpm_wc_new 
WITH (appendonly=true, orientation=column)
as (select * from truven.hpm_wc)
distributed randomly;

drop table truven.hpm_wc;
alter table truven.hpm_wc_new rename to wc;

select distinct extract(year from DTBEG)
from truven.hpm_wc;

update truven.hpm_wc set year=extract(year from DTBEG);

