/*

  !!!!!!!!!!!
  NOTE: Manually adding a 'year' column to every table and populating it based on which file the data comes from.
  Ex. ltd2011 = 2011, etc.... 
  !!!!!!!!!!!
  
v1 Fields:

NOTE: Missing 2011 file


v2 Fields:

SEQNUM,ADV_CASE,CASESTAT,BASE_PAY,DTABS1,DTLAST,DAYSABS,OFFSET_PAY,DTRTW,EFAMID,ENROLID,RTW_FLAG,CASEID,CASEDX,PAYTOT,VERSION


v3 Fields: (Add DXVER)

SEQNUM,ADV_CASE,CASESTAT,BASE_PAY,DTABS1,DTLAST,DAYSABS,OFFSET_PAY,DTRTW,EFAMID,ENROLID,RTW_FLAG,CASEID,CASEDX,DXVER,PAYTOT,VERSION

*/


drop table truven.ltd;
CREATE TABLE truven.ltd (
	year int2,
	seqnum numeric NULL,
	adv_case bpchar(30) null,
	casestat bpchar(1) null,
	base_pay numeric null,
	dtabs1 date null,
	dtlast date NULL,
	daysabs numeric null,
	offset_pay numeric null,
	dtrtw date null, 
	efamid numeric null,
	enrolid numeric NULL,
	rtw_flag bpchar(1) null,
	caseid numeric null,
	casedx bpchar(10) null,
	dxver bpchar(1) null,
	paytot numeric null,
	version int2 NULL
	
)
DISTRIBUTED RANDOMLY;

drop external table ext_ltd_v2;
CREATE EXTERNAL TABLE ext_ltd_v2 (
	seqnum numeric ,
	adv_case bpchar(30) ,
	casestat bpchar(1) ,
	base_pay numeric ,
	dtabs1 date ,
	dtlast date ,
	daysabs numeric ,
	offset_pay numeric ,
	dtrtw date , 
	efamid numeric ,
	enrolid numeric ,
	rtw_flag bpchar(1) ,
	caseid numeric ,
	casedx bpchar(10) ,
	paytot numeric ,
	version int2 
) 
LOCATION ( 
'gpfdist://c252-140:8801/*2014*'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

/*
select *
from ext_ltd_v2
limit 1000;

truncate table truven.ltd;

*/

insert into truven.ltd (year, SEQNUM,ADV_CASE,CASESTAT,BASE_PAY,DTABS1,DTLAST,DAYSABS,OFFSET_PAY,DTRTW,EFAMID,ENROLID,RTW_FLAG,CASEID,CASEDX,PAYTOT,VERSION)
select 2014, SEQNUM,ADV_CASE,CASESTAT,BASE_PAY,DTABS1,DTLAST,DAYSABS,OFFSET_PAY,DTRTW,EFAMID,ENROLID,RTW_FLAG,CASEID,CASEDX,PAYTOT,VERSION
from ext_ltd_v2;

drop external table ext_ltd_v3;
CREATE EXTERNAL TABLE ext_ltd_v3 (
	seqnum numeric ,
	adv_case bpchar(30) ,
	casestat bpchar(1) ,
	base_pay numeric ,
	dtabs1 date ,
	dtlast date ,
	daysabs numeric ,
	offset_pay numeric ,
	dtrtw date , 
	efamid numeric ,
	enrolid numeric ,
	rtw_flag bpchar(1) ,
	caseid numeric ,
	casedx bpchar(10) ,
	dxver bpchar(1) ,
	paytot numeric ,
	version int2 
) 
LOCATION ( 
'gpfdist://c252-140:8801/*'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

/*
select *
from ext_ltd_v3
limit 1000;
*/

insert into truven.ltd (year, SEQNUM,ADV_CASE,CASESTAT,BASE_PAY,DTABS1,DTLAST,DAYSABS,OFFSET_PAY,DTRTW,EFAMID,ENROLID,RTW_FLAG,CASEID,CASEDX,DXVER,PAYTOT,VERSION)
select 2015, SEQNUM,ADV_CASE,CASESTAT,BASE_PAY,DTABS1,DTLAST,DAYSABS,OFFSET_PAY,DTRTW,EFAMID,ENROLID,RTW_FLAG,CASEID,CASEDX,DXVER,PAYTOT,VERSION
from ext_ltd_v3;

-- Verify

select count(*) from truven.ltd;



-- Fix storage options
create table truven.ltd_new 
WITH (appendonly=true, orientation=column)
as (select * from truven.ltd)
distributed randomly;

drop table truven.ltd;
alter table truven.ltd_new rename to ltd;

