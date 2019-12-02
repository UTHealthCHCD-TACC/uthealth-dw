/*
 
v1 Fields:

abs2012,std2012,wc2012,ltd2012,ENROLID,absfreq,absgrp,SEX,DOBYR,seqnum,version


v2 Fields: (Add EFAMID)

abs2012,std2012,wc2012,ltd2012,ENROLID,EFAMID,absfreq,absgrp,SEX,DOBYR,seqnum,version 

*/


drop table truven.elig;
CREATE TABLE truven.elig (
	year int2,
	abs numeric,
	std numeric,
	wc numeric,
	ltd numeric,
	enrolid numeric,
	efamid numeric null,
	absfreq bpchar(10),
	absgrp numeric,
	sex int2,
	dobyr numeric,
	seqnum numeric,
	version int2
)
DISTRIBUTED RANDOMLY;

drop external table ext_elig_v1;
CREATE EXTERNAL TABLE ext_elig_v1 (
	abs numeric,
	std numeric,
	wc numeric,
	ltd numeric,
	enrolid numeric,
	absfreq bpchar(10),
	absgrp numeric,
	sex int2,
	dobyr numeric,
	seqnum numeric,
	version int2
) 
LOCATION ( 
'gpfdist://c252-140:8801/*2011*'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

/*
select *
from ext_elig_v1
limit 1000;
*/


insert into truven.elig (year, abs,std,wc,ltd,ENROLID,absfreq,absgrp,SEX,DOBYR,seqnum,version )
select 2011, abs,std,wc,ltd,ENROLID,absfreq,absgrp,SEX,DOBYR,seqnum,version 
from ext_elig_v1;

drop external table ext_elig_v2;
CREATE EXTERNAL TABLE ext_elig_v2 (
	abs numeric,
	std numeric,
	wc numeric,
	ltd numeric,
	enrolid numeric,
	efamid numeric,
	absfreq bpchar(10),
	absgrp numeric,
	sex int2,
	dobyr numeric,
	seqnum numeric,
	version int2
) 
LOCATION ( 
'gpfdist://c252-140:8801/*2015*'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

/*
select *
from ext_elig_v2
limit 1000;
*/

insert into truven.elig (year, abs,std,wc,ltd,ENROLID,efamid,absfreq,absgrp,SEX,DOBYR,seqnum,version )
select 2015, abs,std,wc,ltd,ENROLID,efamid,absfreq,absgrp,SEX,DOBYR,seqnum,version 
from ext_elig_v2;

-- Verify

select count(*) from truven.elig;

-- Fix storage options
create table truven.elig_new 
WITH (appendonly=true, orientation=column)
as (select * from truven.elig)
distributed randomly;

drop table truven.elig;
alter table truven.elig_new rename to elig;



