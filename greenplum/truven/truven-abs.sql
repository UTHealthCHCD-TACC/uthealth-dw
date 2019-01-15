/*
v1 Fields:

HOURS,ABSTYP,ABSFROM,ABSTO,ENROLID,PAID_IND,seqnum,version


v2 Fields: (Add EFAMID)

HOURS,ABSTYP,ABSFROM,ABSTO,EFAMID,ENROLID,PAID_IND,seqnum,version 

*/

-- !!!!!!!!!!!!!!!!
-- NOTE: Issue with a 'F' value found in MHSACOVG and 's' for dstatus in abs113.csv and also non-character data.  Leaving that file out for now.
-- !!!!!!!!!!!!!!!
drop table truven.abs;
CREATE TABLE truven.abs (
	hours numeric null,
	abstyp int2 null,
	absfrom date null,
	absto date null,
	efamid numeric NULL,
	enrolid numeric NULL,
	paid_ind bpchar(1) null,
	seqnum numeric NULL,
	version int2 NULL
	
)
DISTRIBUTED RANDOMLY;

drop external table ext_abs_v1;
CREATE EXTERNAL TABLE ext_abs_v1 (
	hours numeric ,
	abstyp int2 ,
	absfrom date ,
	absto date ,
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
from ext_abs_v1
limit 1000;

truncate table truven.abs;

insert into truven.abs (HOURS,ABSTYP,ABSFROM,ABSTO,ENROLID,PAID_IND,seqnum,version)
select HOURS,ABSTYP,ABSFROM,ABSTO,ENROLID,PAID_IND,seqnum,version
from ext_abs_v1;

drop external table ext_abs_v2;
CREATE EXTERNAL TABLE ext_abs_v2 (
	hours numeric ,
	abstyp int2 ,
	absfrom date ,
	absto date ,
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
from ext_abs_v2
limit 1000;

insert into truven.abs (HOURS,ABSTYP,ABSFROM,ABSTO,EFAMID,ENROLID,PAID_IND,seqnum,version )
select HOURS,ABSTYP,ABSFROM,ABSTO,EFAMID,ENROLID,PAID_IND,seqnum,version 
from ext_abs_v2;

-- Verify

select count(*) from truven.abs;



