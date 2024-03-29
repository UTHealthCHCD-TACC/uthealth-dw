/*

  !!!!!!!!!!!
  NOTE: Manually adding a 'year' column to every table and populating it based on which file the data comes from.
  Ex. abs2011 = 2011, etc.... 
  !!!!!!!!!!!
   
v1 Fields:

SEQNUM,ADV_CASE,DTABS1,DTRTW,CASESTAT,DTBEG,DTLAST,ENROLID,CASEID,RTW_FLAG,DAYSABS,CASEDX,PAYTOT,VERSION


v2 Fields: (Add EFAMID)

SEQNUM,ADV_CASE,DTABS1,DTRTW,CASESTAT,DTBEG,DTLAST,EFAMID,ENROLID,CASEID,RTW_FLAG,DAYSABS,CASEDX,PAYTOT,VERSION 

v3 Fields: (Add DXVER)

SEQNUM,ADV_CASE,DTABS1,DTRTW,CASESTAT,DTBEG,DTLAST,EFAMID,ENROLID,CASEID,RTW_FLAG,DAYSABS,CASEDX,DXVER,PAYTOT,VERSION

*/


drop table truven.hpm_std;
CREATE TABLE truven.hpm_std (
	YEAR int2,
	seqnum numeric NULL,
	adv_case bpchar(30) null,
	dtabs1 date null,
	dtrtw date null,
	casestat bpchar(1) null,
	dtbeg date null,
	dtlast date NULL,
	efamid numeric null,
	enrolid numeric NULL,
	caseid numeric null,
	rtw_flag bpchar(1) null,
	daysabs numeric null,
	casedx bpchar(10) null,
	dxver bpchar(1) null,
	paytot numeric null,
	version int2 NULL
	
)
DISTRIBUTED RANDOMLY;

drop external table ext_std_v1;
CREATE EXTERNAL TABLE ext_std_v1 (
	seqnum numeric ,
	adv_case bpchar(30) ,
	dtabs1 date ,
	dtrtw date ,
	casestat bpchar(1) ,
	dtbeg date ,
	dtlast date ,
	enrolid numeric ,
	caseid numeric ,
	rtw_flag bpchar(1) ,
	daysabs numeric ,
	casedx bpchar(10) ,
	paytot numeric ,
	version int2 
) 
LOCATION ( 
'gpfdist://greenplum01:8081/uthealth/truven/HPM/CSV/V1/STD*.CSV'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

/*
select *
from ext_std_v1
limit 100000;

truncate table truven.hpm_std;
*/

insert into truven.hpm_std (SEQNUM,ADV_CASE,DTABS1,DTRTW,CASESTAT,DTBEG,DTLAST,ENROLID,CASEID,RTW_FLAG,DAYSABS,CASEDX,PAYTOT,VERSION)
select SEQNUM,ADV_CASE,DTABS1,DTRTW,CASESTAT,DTBEG,DTLAST,ENROLID,CASEID,RTW_FLAG,DAYSABS,CASEDX,PAYTOT,VERSION
from ext_std_v1;

drop external table ext_std_v2;
CREATE EXTERNAL TABLE ext_std_v2 (
	seqnum numeric ,
	adv_case bpchar(30) ,
	dtabs1 date ,
	dtrtw date ,
	casestat bpchar(1) ,
	dtbeg date ,
	dtlast date ,
	efamid numeric ,
	enrolid numeric ,
	caseid numeric ,
	rtw_flag bpchar(1) ,
	daysabs numeric ,
	casedx bpchar(10) ,
	paytot numeric ,
	version int2 
) 
LOCATION ( 
'gpfdist://greenplum01:8081/uthealth/truven/HPM/CSV/V2/STD*.CSV'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

/*
select *
from ext_std_v2
limit 100000;
*/

insert into truven.hpm_std (SEQNUM,ADV_CASE,DTABS1,DTRTW,CASESTAT,DTBEG,DTLAST,EFAMID,ENROLID,CASEID,RTW_FLAG,DAYSABS,CASEDX,PAYTOT,VERSION )
select SEQNUM,ADV_CASE,DTABS1,DTRTW,CASESTAT,DTBEG,DTLAST,EFAMID,ENROLID,CASEID,RTW_FLAG,DAYSABS,CASEDX,PAYTOT,VERSION 
from ext_std_v2;

drop external table ext_std_v3;
CREATE EXTERNAL TABLE ext_std_v3 (
	seqnum numeric ,
	adv_case bpchar(30) ,
	dtabs1 date ,
	dtrtw date ,
	casestat bpchar(1) ,
	dtbeg date ,
	dtlast date ,
	efamid numeric ,
	enrolid numeric ,
	caseid numeric ,
	rtw_flag bpchar(1) ,
	daysabs numeric ,
	casedx bpchar(10) ,
	dxver bpchar(1) ,
	paytot numeric ,
	version int2 
) 
LOCATION ( 
'gpfdist://greenplum01:8081/uthealth/truven/HPM/CSV/V3/STD*.CSV'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

/*
select *
from ext_std_v3
limit 100000;
*/

insert into truven.hpm_std (SEQNUM,ADV_CASE,DTABS1,DTRTW,CASESTAT,DTBEG,DTLAST,EFAMID,ENROLID,CASEID,RTW_FLAG,DAYSABS,CASEDX,DXVER,PAYTOT,VERSION)
select SEQNUM,ADV_CASE,DTABS1,DTRTW,CASESTAT,DTBEG,DTLAST,EFAMID,ENROLID,CASEID,RTW_FLAG,DAYSABS,CASEDX,DXVER,PAYTOT,VERSION
from ext_std_v3;

-- Verify

select count(*) from truven.hpm_std;

-- Fix storage options
create table truven.hpm_std_new 
WITH (appendonly=true, orientation=column)
as (select * from truven.hpm_std)
distributed randomly;

update truven.hpm_std set year=extract(year from DTBEG);

drop table truven.hpm_std;
alter table truven.hpm_std_new rename to std;





