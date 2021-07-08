/*
 
  !!!!!!!!!!!
  NOTE: Manually adding a 'year' column to every table and populating it based on which file the data comes from.
  Ex. abs2011 = 2011, etc.... 
  !!!!!!!!!!!
  
v1 Fields:

HOURS,ABSTYP,ABSFROM,ABSTO,ENROLID,PAID_IND,seqnum,version


v2 Fields: (Add EFAMID)

HOURS,ABSTYP,ABSFROM,ABSTO,EFAMID,ENROLID,PAID_IND,seqnum,version 

NOTE: 2015 file is missing DXVER.  Inconsistent with other 2015 files.

*/

drop table truven.hpm_abs;
CREATE TABLE truven.hpm_abs (
	--year int2 null,
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
'gpfdist://greenplum01:8081/uthealth/truven/HPM/CSV/2011/ABS*.CSV'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_abs_v1
limit 1000;

--truncate table truven.hpm_abs;

insert into truven.hpm_abs (HOURS,ABSTYP,ABSFROM,ABSTO,ENROLID,PAID_IND,seqnum,version)
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
'gpfdist://greenplum01:8081/uthealth/truven/HPM/CSV/ABS*.CSV'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

--select *
--from ext_abs_v2
--limit 1000;

insert into truven.hpm_abs (HOURS,ABSTYP,ABSFROM,ABSTO,EFAMID,ENROLID,PAID_IND,seqnum,version )
select HOURS,ABSTYP,ABSFROM,ABSTO,EFAMID,ENROLID,PAID_IND,seqnum,version 
from ext_abs_v2;

-- Verify

select count(*) from truven.hpm_abs;

-- Fix storage options
create table truven.hpm_abs_new 
WITH (appendonly=true, orientation=column)
as (select * from truven.hpm_abs)
distributed randomly;

drop table truven.hpm_abs;
alter table truven.hpm_abs_new rename to abs;

select distinct extract(year from ABSFROM)
from truven.hpm_abs;

update truven.hpm_abs set year=extract(year from ABSFROM);



