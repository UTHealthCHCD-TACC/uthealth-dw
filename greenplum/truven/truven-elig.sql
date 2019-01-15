/*
v1 Fields:

abs2012,std2012,wc2012,ltd2012,ENROLID,absfreq,absgrp,SEX,DOBYR,seqnum,version


v2 Fields: (Add EFAMID)

abs2012,std2012,wc2012,ltd2012,ENROLID,EFAMID,absfreq,absgrp,SEX,DOBYR,seqnum,versionn 

*/

-- !!!!!!!!!!!!!!!!
-- NOTE: Issue with a 'F' value found in MHSACOVG and 's' for dstatus in elig113.csv and also non-character data.  Leaving that file out for now.
-- !!!!!!!!!!!!!!!
drop table truven.elig;
CREATE TABLE truven.elig (
	hours numeric null,
	eligtyp int2 null,
	eligfrom date null,
	eligto date null,
	efamid numeric NULL,
	enrolid numeric NULL,
	paid_ind bpchar(1) null,
	seqnum numeric NULL,
	version int2 NULL
	
)
DISTRIBUTED RANDOMLY;

drop external table ext_elig_v1;
CREATE EXTERNAL TABLE ext_elig_v1 (
	hours numeric ,
	eligtyp int2 ,
	eligfrom date ,
	eligto date ,
	enrolid numeric ,
	paid_ind bpchar(1) ,
	seqnum numeric ,
	version int2
) 
LOCATION ( 
'gpfdist://c252-140:8801/*'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_elig_v1
limit 1000;

truncate table truven.elig;

insert into truven.elig (HOURS,eligTYP,eligFROM,eligTO,ENROLID,PAID_IND,seqnum,version)
select HOURS,eligTYP,eligFROM,eligTO,ENROLID,PAID_IND,seqnum,version
from ext_elig_v1;

drop external table ext_elig_v2;
CREATE EXTERNAL TABLE ext_elig_v2 (
	hours numeric ,
	eligtyp int2 ,
	eligfrom date ,
	eligto date ,
	efamid numeric ,
	enrolid numeric ,
	paid_ind bpchar(1) ,
	seqnum numeric ,
	version int2 
) 
LOCATION ( 
'gpfdist://c252-140:8801/*'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_elig_v2
limit 1000;

insert into truven.elig (HOURS,eligTYP,eligFROM,eligTO,EFAMID,ENROLID,PAID_IND,seqnum,version )
select HOURS,eligTYP,eligFROM,eligTO,EFAMID,ENROLID,PAID_IND,seqnum,version 
from ext_elig_v2;

-- Verify

select count(*) from truven.elig;



