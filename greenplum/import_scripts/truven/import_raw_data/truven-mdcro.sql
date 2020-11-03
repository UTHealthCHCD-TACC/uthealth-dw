create schema truven;

drop table truven.mdcro;
CREATE TABLE truven.mdcro (
	seqnum numeric NULL,
	version int2 NULL,
	dx1 bpchar(10) NULL,
	dx2 bpchar(10) NULL,
	proc1 bpchar(5) NULL,
	proctyp bpchar(5) NULL,
	efamid numeric NULL,
	enrolid numeric NULL,
	revcode int2 NULL,
	svcdate date NULL,
	dobyr numeric NULL,
	"year" numeric NULL,
	age numeric NULL,
	cap_svc bpchar(1) NULL,
	cob numeric NULL,
	coins numeric NULL,
	copay numeric NULL,
	deduct numeric NULL,
	dx3 bpchar(10) NULL,
	dx4 bpchar(10) NULL,
	dxver bpchar(10) NULL,
	empzip numeric null,
	fachdid numeric NULL,
	facprof bpchar(1) NULL,
	mhsacovg int2 NULL,
	netpay numeric NULL,
	ntwkprov bpchar(1) NULL,
	paidntwk bpchar(1) NULL,
	pay numeric NULL,
	pddate date NULL,
	plantyp numeric NULL,
	procgrp numeric NULL,
	procmod bpchar(2) NULL,
	provid numeric NULL,
	qty numeric NULL,
	svcscat int4 NULL,
	tsvcdat date NULL,
	mdc int2 NULL,
	region int2 NULL,
	msa numeric NULL,
	stdplac numeric NULL,
	stdprov numeric NULL,
	datatyp numeric NULL,
	plankey numeric NULL,
	wgtkey numeric NULL,
	agegrp int2 NULL,
	eeclass int2 NULL,
	eestatu int2 NULL,
	egeoloc int2 NULL,
	eidflag int2 NULL,
	emprel int2 NULL,
	enrflag int2 NULL,
	phyflag int2 NULL,
	rx int2 NULL,
	sex int2 NULL,
	hlthplan int2 NULL,
	indstry bpchar(5) NULL,
	msclmid numeric NULL,
	npi bpchar(10) NULL,
	units numeric NULL
)
DISTRIBUTED RANDOMLY;

drop external table ext_mdcro_v1;
CREATE EXTERNAL TABLE ext_mdcro_v1 (
	seqnum numeric,
	version int2,
	dx1 bpchar(10),
	dx2 bpchar(10),
	proc1 bpchar(5),
	proctyp bpchar(5),
	efamid numeric,
	enrolid numeric,
	revcode int2,
	svcdate date,
	dobyr numeric,
	year numeric,
	age numeric,
	cap_svc bpchar(1),
	cob numeric,
	coins numeric,
	copay numeric,
	deduct numeric,
	dx3 bpchar(10),
	dx4 bpchar(10),
	empzip numeric,
	fachdid numeric,
	facprof bpchar(1),
	mhsacovg int2,
	msclmid numeric,
	netpay numeric,
	ntwkprov bpchar(1),
	paidntwk bpchar(1),
	pay numeric,
	pddate date,
	plantyp numeric,
	procgrp numeric,
	procmod bpchar(2),
	provid numeric,
	qty numeric,
	svcscat int4,
	tsvcdat date,
	mdc int2,
	region int2,
	stdplac numeric,
	stdprov numeric,
	datatyp numeric,
	plankey numeric,
	wgtkey numeric,
	agegrp int2,
	eeclass int2,
	eestatu int2,
	egeoloc int2,
	eidflag int2,
	emprel int2,
	enrflag int2,
	phyflag int2,
	rx int2,
	sex int2,
	hlthplan int2,
	indstry bpchar(5)
) 
LOCATION ( 
'gpfdist://c252-140:8801/*'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_mdcro_v1
limit 1000;

truncate table truven.mdcro;

insert into truven.mdcro (SEQNUM,VERSION,DX1,DX2,PROC1,PROCTYP,EFAMID,ENROLID,REVCODE,SVCDATE,DOBYR,YEAR,
AGE,CAP_SVC,COB,COINS,COPAY,DEDUCT,DX3,DX4,EMPZIP,FACHDID,FACPROF,MHSACOVG,MSCLMID,NETPAY,NTWKPROV,
PAIDNTWK,PAY,PDDATE,PLANTYP,PROCGRP,PROCMOD,PROVID,QTY,SVCSCAT,TSVCDAT,MDC,REGION,STDPLAC,STDPROV,DATATYP,PLANKEY,
WGTKEY,AGEGRP,EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY)
select SEQNUM,VERSION,DX1,DX2,PROC1,PROCTYP,EFAMID,ENROLID,REVCODE,SVCDATE,DOBYR,YEAR,
AGE,CAP_SVC,COB,COINS,COPAY,DEDUCT,DX3,DX4,EMPZIP,FACHDID,FACPROF,MHSACOVG,MSCLMID,NETPAY,NTWKPROV,
PAIDNTWK,PAY,PDDATE,PLANTYP,PROCGRP,PROCMOD,PROVID,QTY,SVCSCAT,TSVCDAT,MDC,REGION,STDPLAC,STDPROV,DATATYP,PLANKEY,
WGTKEY,AGEGRP,EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY
from ext_mdcro_v1;
limit 1000;

drop external table ext_mdcro_v2;
CREATE EXTERNAL TABLE ext_mdcro_v2 (
	seqnum numeric,
	version int2,
	dx1 bpchar(10),
	dx2 bpchar(10),
	dxver bpchar(10),
	proc1 bpchar(5),
	proctyp bpchar(5),
	efamid numeric,
	enrolid numeric,
	revcode int2,
	svcdate date,
	dobyr numeric,
	year numeric,
	age numeric,
	cap_svc bpchar(1),
	cob numeric,
	coins numeric,
	copay numeric,
	deduct numeric,
	dx3 bpchar(10),
	dx4 bpchar(10),
	empzip numeric,
	fachdid numeric,
	facprof bpchar(1),
	mhsacovg int2,
	msclmid numeric,
	netpay numeric,
	npi bpchar(10),
	ntwkprov bpchar(1),
	paidntwk bpchar(1),
	pay numeric,
	pddate date,
	plantyp numeric,
	procgrp numeric,
	procmod bpchar(2),
	provid numeric,
	qty numeric,
	svcscat int4,
	tsvcdat date,
	units numeric,
	mdc int2,
	region int2,
	stdplac numeric,
	stdprov numeric,
	datatyp numeric,
	agegrp int2,
	eeclass int2,
	eestatu int2,
	egeoloc int2,
	eidflag int2,
	emprel int2,
	enrflag int2,
	phyflag int2,
	rx int2,
	sex int2,
	hlthplan int2,
	indstry bpchar(5)
) 
LOCATION ( 
'gpfdist://192.168.58.179:8081/truven/2019/mdcro*'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_mdcro_v2
limit 1000;

insert into truven.mdcro (SEQNUM,VERSION,DX1,DX2,DXVER,PROC1,PROCTYP,EFAMID,ENROLID,REVCODE,SVCDATE,DOBYR,YEAR,
AGE,CAP_SVC,COB,COINS,COPAY,DEDUCT,DX3,DX4,EMPZIP,FACHDID,FACPROF,MHSACOVG,MSCLMID,NETPAY,NPI,NTWKPROV,PAIDNTWK,PAY,PDDATE,
PLANTYP,PROCGRP,PROCMOD,PROVID,QTY, SVCSCAT,TSVCDAT,UNITS,MDC,REGION,STDPLAC,STDPROV,DATATYP,AGEGRP,
EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY)
select SEQNUM,VERSION,DX1,DX2,DXVER,PROC1,PROCTYP,EFAMID,ENROLID,REVCODE,SVCDATE,DOBYR,YEAR,
AGE,CAP_SVC,COB,COINS,COPAY,DEDUCT,DX3,DX4,EMPZIP,FACHDID,FACPROF,MHSACOVG,MSCLMID,NETPAY,NPI,NTWKPROV,PAIDNTWK,PAY,PDDATE,
PLANTYP,PROCGRP,PROCMOD,PROVID,QTY, SVCSCAT,TSVCDAT,UNITS,MDC,REGION,STDPLAC,STDPROV,DATATYP,AGEGRP,
EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY
from ext_mdcro_v2;

-- Verify

select count(*), min(year), max(year) from truven.mdcro;



-- Fix storage options
create table truven.mdcro_2019
WITH (appendonly=true, orientation=column, compresstype=zlib)
as (select * from truven.mdcro where year=2019)
distributed randomly;

delete from truven.mdcro where year=2019;

drop table truven.mdcro;
alter table truven.mdcro_new rename to mdcro;

