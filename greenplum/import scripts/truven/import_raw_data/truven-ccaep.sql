/*
v1 Fields:

AGEGRP,DATATYP,EECLASS,EESTATU,EGEOLOC,EMPREL,ENRFLAG,HLTHPLAN,INDSTRY,MHSACOVG,PHYFLAG,PLANKEY,PLANTYP,POPDATE,REGION,RX,SEX,VERSION,WGTKEY,YEAR,POPCNT


v2 Fields: Currently missing a ccaep151 file

*/

drop table truven.ccaep;
CREATE TABLE truven.ccaep (
	agegrp int2 null,
	datatyp numeric null,
	eeclass int2 null,
	eestatu int2 null,
	egeoloc int2 null,
	emprel int2 null,
	enrflag int2 null,
	hlthplan int2 null,
	indstry bpchar(5) null,
	mhsacovg numeric null,
	phyflag int2 NULL,
	plankey numeric null,
	plantyp numeric null,
	popdate numeric null,
	region int2 null,
	rx int2 null,
	sex int2 null,
	version int2 null,
	wgtkey numeric null,
	year numeric null,
	popcnt numeric null
	
)
DISTRIBUTED RANDOMLY;

drop external table ext_ccaep_v1;
CREATE EXTERNAL TABLE ext_ccaep_v1 (
	agegrp int2 ,
	datatyp numeric ,
	eeclass int2 ,
	eestatu int2 ,
	egeoloc int2 ,
	emprel int2 ,
	enrflag int2 ,
	hlthplan int2 ,
	indstry bpchar(5) ,
	mhsacovg numeric ,
	phyflag int2 ,
	plankey numeric ,
	plantyp numeric ,
	popdate numeric ,
	region int2 ,
	rx int2 ,
	sex int2 ,
	version int2 ,
	wgtkey numeric ,
	year numeric ,
	popcnt numeric 
) 
LOCATION ( 
'gpfdist://c252-140:8801/*'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_ccaep_v1
limit 1000;

--truncate table truven.ccaep;

insert into truven.ccaep (AGEGRP,DATATYP,EECLASS,EESTATU,EGEOLOC,EMPREL,ENRFLAG,HLTHPLAN,INDSTRY,MHSACOVG,PHYFLAG,PLANKEY,PLANTYP,POPDATE,REGION,RX,SEX,VERSION,WGTKEY,YEAR,POPCNT)
select AGEGRP,DATATYP,EECLASS,EESTATU,EGEOLOC,EMPREL,ENRFLAG,HLTHPLAN,INDSTRY,MHSACOVG,PHYFLAG,PLANKEY,PLANTYP,POPDATE,REGION,RX,SEX,VERSION,WGTKEY,YEAR,POPCNT
from ext_ccaep_v1;


-- Verify

select count(*) from truven.ccaep;

-- Fix storage options
create table truven.ccaep_new 
WITH (appendonly=true, orientation=column)
as (select * from truven.ccaep)
distributed randomly;

drop table truven.ccaep;
alter table truven.ccaep_new rename to ccaep;



