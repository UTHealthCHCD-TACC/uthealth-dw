/*
v1 Fields:

SEQNUM,VERSION,EFAMID,ENROLID,MEMDAYS,YEAR,AGE,DOBYR,AGEGRP,EMPREL,PHYFLAG,RX,SEX,HLTHPLAN,ENRMON,
DATTYP1,DATTYP2,DATTYP3,DATTYP4,DATTYP5,DATTYP6,DATTYP7,DATTYP8,DATTYP9,DATTYP10,DATTYP11,DATTYP12,
ENRIND1,ENRIND2,ENRIND3,ENRIND4,ENRIND5,ENRIND6,ENRIND7,ENRIND8,ENRIND9,ENRIND10,ENRIND11,ENRIND12,
MEMDAY1,MEMDAY2,MEMDAY3,MEMDAY4,MEMDAY5,MEMDAY6,MEMDAY7,MEMDAY8,MEMDAY9,MEMDAY10,MEMDAY11,MEMDAY12,
PLNKEY1,PLNKEY2,PLNKEY3,PLNKEY4,PLNKEY5,PLNKEY6,PLNKEY7,PLNKEY8,PLNKEY9,PLNKEY10,PLNKEY11,PLNKEY12,
PLNTYP1,PLNTYP2,PLNTYP3,PLNTYP4,PLNTYP5,PLNTYP6,PLNTYP7,PLNTYP8,PLNTYP9,PLNTYP10,PLNTYP11,PLNTYP12,
EECLASS,EESTATU,EGEOLOC,EMPZIP,INDSTRY,MHSACOVG,REGION,WGTKEY

v2 Fields: (missing all PLNKEY* fields)

SEQNUM,VERSION,EFAMID,ENROLID,MEMDAYS,YEAR,AGE,DOBYR,AGEGRP,EMPREL,PHYFLAG,RX,SEX,HLTHPLAN,ENRMON,
DATTYP1,DATTYP2,DATTYP3,DATTYP4,DATTYP5,DATTYP6,DATTYP7,DATTYP8,DATTYP9,DATTYP10,DATTYP11,DATTYP12,
ENRIND1,ENRIND2,ENRIND3,ENRIND4,ENRIND5,ENRIND6,ENRIND7,ENRIND8,ENRIND9,ENRIND10,ENRIND11,ENRIND12,
MEMDAY1,MEMDAY2,MEMDAY3,MEMDAY4,MEMDAY5,MEMDAY6,MEMDAY7,MEMDAY8,MEMDAY9,MEMDAY10,MEMDAY11,MEMDAY12,
PLNTYP1,PLNTYP2,PLNTYP3,PLNTYP4,PLNTYP5,PLNTYP6,PLNTYP7,PLNTYP8,PLNTYP9,PLNTYP10,PLNTYP11,PLNTYP12,
EECLASS,EESTATU,EGEOLOC,EMPZIP,INDSTRY,MHSACOVG,REGION,MSWGTKEY

*/

drop table truven.ccaea;
CREATE TABLE truven.ccaea (
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

drop external table ext_ccaea_v1;
CREATE EXTERNAL TABLE ext_ccaea_v1 (
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
from ext_ccaea_v1
limit 1000;

truncate table truven.ccaea;

insert into truven.ccaea (SEQNUM,VERSION,DX1,DX2,PROC1,PROCTYP,EFAMID,ENROLID,REVCODE,SVCDATE,DOBYR,YEAR,
AGE,CAP_SVC,COB,COINS,COPAY,DEDUCT,DX3,DX4,EMPZIP,FACHDID,FACPROF,MHSACOVG,MSCLMID,NETPAY,NTWKPROV,
PAIDNTWK,PAY,PDDATE,PLANTYP,PROCGRP,PROCMOD,PROVID,QTY,SVCSCAT,TSVCDAT,MDC,REGION,STDPLAC,STDPROV,DATATYP,PLANKEY,
WGTKEY,AGEGRP,EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY)
select SEQNUM,VERSION,DX1,DX2,PROC1,PROCTYP,EFAMID,ENROLID,REVCODE,SVCDATE,DOBYR,YEAR,
AGE,CAP_SVC,COB,COINS,COPAY,DEDUCT,DX3,DX4,EMPZIP,FACHDID,FACPROF,MHSACOVG,MSCLMID,NETPAY,NTWKPROV,
PAIDNTWK,PAY,PDDATE,PLANTYP,PROCGRP,PROCMOD,PROVID,QTY,SVCSCAT,TSVCDAT,MDC,REGION,STDPLAC,STDPROV,DATATYP,PLANKEY,
WGTKEY,AGEGRP,EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY
from ext_ccaea_v1;
limit 1000;

drop external table ext_ccaea_v2;
CREATE EXTERNAL TABLE ext_ccaea_v2 (
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
'gpfdist://c252-140:8801/*'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_ccaea_v2
limit 1000;

insert into truven.ccaea (SEQNUM,VERSION,DX1,DX2,DXVER,PROC1,PROCTYP,EFAMID,ENROLID,REVCODE,SVCDATE,DOBYR,YEAR,
AGE,CAP_SVC,COB,COINS,COPAY,DEDUCT,DX3,DX4,EMPZIP,FACHDID,FACPROF,MHSACOVG,MSCLMID,NETPAY,NPI,NTWKPROV,PAIDNTWK,PAY,PDDATE,
PLANTYP,PROCGRP,PROCMOD,PROVID,QTY, SVCSCAT,TSVCDAT,UNITS,MDC,REGION,STDPLAC,STDPROV,DATATYP,AGEGRP,
EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY)
select SEQNUM,VERSION,DX1,DX2,DXVER,PROC1,PROCTYP,EFAMID,ENROLID,REVCODE,SVCDATE,DOBYR,YEAR,
AGE,CAP_SVC,COB,COINS,COPAY,DEDUCT,DX3,DX4,EMPZIP,FACHDID,FACPROF,MHSACOVG,MSCLMID,NETPAY,NPI,NTWKPROV,PAIDNTWK,PAY,PDDATE,
PLANTYP,PROCGRP,PROCMOD,PROVID,QTY, SVCSCAT,TSVCDAT,UNITS,MDC,REGION,STDPLAC,STDPROV,DATATYP,AGEGRP,
EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY
from ext_ccaea_v2;

-- Verify

select count(*) from truven.ccaea;



