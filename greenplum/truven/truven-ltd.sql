/*
v1 Fields:

abs2012,std2012,wc2012,ltd2012,ENROLID,absfreq,absgrp,SEX,DOBYR,seqnum,version


v2 Fields: (Add EFAMID)

abs2012,std2012,wc2012,ltd2012,ENROLID,EFAMID,absfreq,absgrp,SEX,DOBYR,seqnum,versionn 

*/

-- !!!!!!!!!!!!!!!!
-- NOTE: Issue with a 'F' value found in MHSACOVG and 's' for dstatus in ltd113.csv and also non-character data.  Leaving that file out for now.
-- !!!!!!!!!!!!!!!
drop table truven.ltd;
CREATE TABLE truven.ltd (
	hours numeric null,
	ltdtyp int2 null,
	ltdfrom date null,
	ltdto date null,
	efamid numeric NULL,
	enrolid numeric NULL,
	paid_ind bpchar(1) null,
	seqnum numeric NULL,
	version int2 NULL
	
)
DISTRIBUTED RANDOMLY;

drop external table ext_ltd_v1;
CREATE EXTERNAL TABLE ext_ltd_v1 (
	hours numeric ,
	ltdtyp int2 ,
	ltdfrom date ,
	ltdto date ,
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
from ext_ltd_v1
limit 1000;

truncate table truven.ltd;

insert into truven.ltd (HOURS,ltdTYP,ltdFROM,ltdTO,ENROLID,PAID_IND,seqnum,version)
select HOURS,ltdTYP,ltdFROM,ltdTO,ENROLID,PAID_IND,seqnum,version
from ext_ltd_v1;

drop external table ext_ltd_v2;
CREATE EXTERNAL TABLE ext_ltd_v2 (
	hours numeric ,
	ltdtyp int2 ,
	ltdfrom date ,
	ltdto date ,
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
from ext_ltd_v2
limit 1000;

insert into truven.ltd (HOURS,ltdTYP,ltdFROM,ltdTO,EFAMID,ENROLID,PAID_IND,seqnum,version )
select HOURS,ltdTYP,ltdFROM,ltdTO,EFAMID,ENROLID,PAID_IND,seqnum,version 
from ext_ltd_v2;

-- Verify

select count(*) from truven.ltd;



