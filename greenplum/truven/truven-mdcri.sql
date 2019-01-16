/*
v1 Fields:

SEQNUM,VERSION,EFAMID,ENROLID,DOBYR,YEAR,ADMDATE,AGE,CASEID,DAYS,DISDATE,DRG,
EMPZIP,HOSPNET,HOSPPAY,MHSACOVG,PDX,PHYSID,PHYSNET,PHYSPAY,PLANTYP,PPROC,
TOTCOB,TOTCOINS,TOTCOPAY,TOTDED,TOTNET,TOTPAY,ADMTYP,MDC,DSTATUS,REGION,DATATYP,PLANKEY,WGTKEY,
DX1,DX2,DX3,DX4,DX5,DX6,DX7,DX8,DX9,DX10,DX11,DX12,DX13,DX14,DX15,
PROC1,PROC2,PROC3,PROC4,PROC5,PROC6,PROC7,PROC8,PROC9,PROC10,PROC11,PROC12,PROC13,PROC14,PROC15,
AGEGRP,EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,RX,SEX,STATE,HLTHPLAN,INDSTRY


v2 Fields: (Drop PLANKEY and WGTKEY, Add DXVER)

SEQNUM,VERSION,EFAMID,ENROLID,DOBYR,YEAR,ADMDATE,AGE,CASEID,DAYS,DISDATE,DRG,DXVER,
EMPZIP,HOSPNET,HOSPPAY,MHSACOVG,PDX,PHYSID,PHYSNET,PHYSPAY,PLANTYP,PPROC,
TOTCOB,TOTCOINS,TOTCOPAY,TOTDED,TOTNET,TOTPAY,ADMTYP,MDC,DSTATUS,REGION,DATATYP,
DX1,DX2,DX3,DX4,DX5,DX6,DX7,DX8,DX9,DX10,DX11,DX12,DX13,DX14,DX15,
PROC1,PROC2,PROC3,PROC4,PROC5,PROC6,PROC7,PROC8,PROC9,PROC10,PROC11,PROC12,PROC13,PROC14,PROC15,
AGEGRP,EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,RX,SEX,STATE,HLTHPLAN,INDSTRY


*/

drop table truven.mdcri;
CREATE TABLE truven.mdcri (
	seqnum numeric NULL,
	version int2 NULL,
	efamid numeric NULL,
	enrolid numeric NULL,
	dobyr numeric NULL,
	year numeric NULL,
	admdate date null,
	age numeric NULL,
	caseid numeric null,
	days numeric null,
	disdate date null,
	drg numeric null,
	dxver bpchar(10) null,
		
	empzip numeric null,
	hospnet numeric null,
	hosppay numeric null,
	mhsacovg int2 NULL,
	pdx bpchar(10) null,
	physid numeric null,
	physnet numeric null,
	physpay numeric null,
	plantyp numeric null,
	pproc bpchar(10) null,
	
	totcob numeric NULL,
	totcoins numeric NULL,
	totcopay numeric NULL,
	totded numeric NULL,
	totnet numeric null,
	totpay numeric null,
	admtyp int2 null,
	mdc bpchar(10) null,
	dstatus int2 null,
	region int2 null,
	datatyp numeric null,
	plankey numeric null,
	wgtkey numeric null,
	
	dx1 bpchar(10) null,dx2 bpchar(10) null,dx3 bpchar(10) null,dx4 bpchar(10) null,dx5 bpchar(10) null,dx6 bpchar(10) null,dx7 bpchar(10) null,dx8 bpchar(10) null,
	dx9 bpchar(10) null,dx10 bpchar(10) null,dx11 bpchar(10) null,dx12 bpchar(10) null,dx13 bpchar(10) null,dx14 bpchar(10) null,dx15 bpchar(10) null,
	proc1 bpchar(10) null,proc2 bpchar(10) null,proc3 bpchar(10) null,proc4 bpchar(10) null,proc5 bpchar(10) null,proc6 bpchar(10) null,proc7 bpchar(10) null,proc8 bpchar(10) null,
	proc9 bpchar(10) null,proc10 bpchar(10) null,proc11 bpchar(10) null,proc12 bpchar(10) null,proc13 bpchar(10) null,proc14 bpchar(10) null,proc15 bpchar(10) null,
	
	agegrp int2 null,
	eeclass int2 null,
	eestatu int2 null,
	egeoloc int2 null,
	eidflag int2 null,
	emprel int2 null,
	enrflag int2 null,
	phyflag int2 null,
	rx int2 null,
	sex int2 null,
	state int2 null,
	hlthplan int2 null,
	indstry bpchar(5) null
)
DISTRIBUTED RANDOMLY;

drop external table ext_mdcri_v1;
CREATE EXTERNAL TABLE ext_mdcri_v1 (
	seqnum numeric ,
	version int2 ,
	efamid numeric ,
	enrolid numeric ,
	dobyr numeric ,
	year numeric ,
	admdate date ,
	age numeric ,
	caseid numeric ,
	days numeric ,
	disdate date ,
	drg numeric ,
		
	empzip numeric ,
	hospnet numeric ,
	hosppay numeric ,
	mhsacovg int2 ,
	pdx bpchar(10) ,
	physid numeric ,
	physnet numeric ,
	physpay numeric ,
	plantyp numeric ,
	pproc bpchar(10) ,
	
	totcob numeric ,
	totcoins numeric ,
	totcopay numeric ,
	totded numeric ,
	totnet numeric ,
	totpay numeric ,
	admtyp int2 ,
	mdc bpchar(10) ,
	dstatus int2 ,
	region int2 ,
	datatyp numeric ,
	plankey numeric ,
	wgtkey numeric ,
	
	dx1 bpchar(10) ,dx2 bpchar(10) ,dx3 bpchar(10) ,dx4 bpchar(10) ,dx5 bpchar(10) ,dx6 bpchar(10) ,dx7 bpchar(10) ,dx8 bpchar(10) ,
	dx9 bpchar(10) ,dx10 bpchar(10) ,dx11 bpchar(10) ,dx12 bpchar(10) ,dx13 bpchar(10) ,dx14 bpchar(10) ,dx15 bpchar(10) ,
	proc1 bpchar(10) ,proc2 bpchar(10) ,proc3 bpchar(10) ,proc4 bpchar(10) ,proc5 bpchar(10) ,proc6 bpchar(10) ,proc7 bpchar(10) ,proc8 bpchar(10) ,
	proc9 bpchar(10) ,proc10 bpchar(10) ,proc11 bpchar(10) ,proc12 bpchar(10) ,proc13 bpchar(10) ,proc14 bpchar(10) ,proc15 bpchar(10) ,
	
	agegrp int2 ,
	eeclass int2 ,
	eestatu int2 ,
	egeoloc int2 ,
	eidflag int2 ,
	emprel int2 ,
	enrflag int2 ,
	phyflag int2 ,
	rx int2 ,
	sex int2 ,
	state int2 ,
	hlthplan int2 ,
	indstry bpchar(5) 
) 
LOCATION ( 
'gpfdist://c252-140:8801/*'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_mdcri_v1
limit 1000;

--truncate table truven.mdcri;

insert into truven.mdcri (SEQNUM,VERSION,EFAMID,ENROLID,DOBYR,YEAR,ADMDATE,AGE,CASEID,DAYS,DISDATE,DRG,
EMPZIP,HOSPNET,HOSPPAY,MHSACOVG,PDX,PHYSID,PHYSNET,PHYSPAY,PLANTYP,PPROC,
TOTCOB,TOTCOINS,TOTCOPAY,TOTDED,TOTNET,TOTPAY,ADMTYP,MDC,DSTATUS,REGION,DATATYP,PLANKEY,WGTKEY,
DX1,DX2,DX3,DX4,DX5,DX6,DX7,DX8,DX9,DX10,DX11,DX12,DX13,DX14,DX15,
PROC1,PROC2,PROC3,PROC4,PROC5,PROC6,PROC7,PROC8,PROC9,PROC10,PROC11,PROC12,PROC13,PROC14,PROC15,
AGEGRP,EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,RX,SEX,STATE,HLTHPLAN,INDSTRY)
select SEQNUM,VERSION,EFAMID,ENROLID,DOBYR,YEAR,ADMDATE,AGE,CASEID,DAYS,DISDATE,DRG,
EMPZIP,HOSPNET,HOSPPAY,MHSACOVG,PDX,PHYSID,PHYSNET,PHYSPAY,PLANTYP,PPROC,
TOTCOB,TOTCOINS,TOTCOPAY,TOTDED,TOTNET,TOTPAY,ADMTYP,MDC,DSTATUS,REGION,DATATYP,PLANKEY,WGTKEY,
DX1,DX2,DX3,DX4,DX5,DX6,DX7,DX8,DX9,DX10,DX11,DX12,DX13,DX14,DX15,
PROC1,PROC2,PROC3,PROC4,PROC5,PROC6,PROC7,PROC8,PROC9,PROC10,PROC11,PROC12,PROC13,PROC14,PROC15,
AGEGRP,EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,RX,SEX,STATE,HLTHPLAN,INDSTRY
from ext_mdcri_v1;

drop external table ext_mdcri_v2;
CREATE EXTERNAL TABLE ext_mdcri_v2 (
	seqnum numeric ,
	version int2 ,
	efamid numeric ,
	enrolid numeric ,
	dobyr numeric ,
	year numeric ,
	admdate date ,
	age numeric ,
	caseid numeric ,
	days numeric ,
	disdate date ,
	drg numeric ,
	dxver bpchar(10) ,
		
	empzip numeric ,
	hospnet numeric ,
	hosppay numeric ,
	mhsacovg int2 ,
	pdx bpchar(10) ,
	physid numeric ,
	physnet numeric ,
	physpay numeric ,
	plantyp numeric ,
	pproc bpchar(10) ,
	
	totcob numeric ,
	totcoins numeric ,
	totcopay numeric ,
	totded numeric ,
	totnet numeric ,
	totpay numeric ,
	admtyp int2 ,
	mdc bpchar(10) ,
	dstatus int2 ,
	region int2 ,
	datatyp numeric ,
	
	dx1 bpchar(10) ,dx2 bpchar(10) ,dx3 bpchar(10) ,dx4 bpchar(10) ,dx5 bpchar(10) ,dx6 bpchar(10) ,dx7 bpchar(10) ,dx8 bpchar(10) ,
	dx9 bpchar(10) ,dx10 bpchar(10) ,dx11 bpchar(10) ,dx12 bpchar(10) ,dx13 bpchar(10) ,dx14 bpchar(10) ,dx15 bpchar(10) ,
	proc1 bpchar(10) ,proc2 bpchar(10) ,proc3 bpchar(10) ,proc4 bpchar(10) ,proc5 bpchar(10) ,proc6 bpchar(10) ,proc7 bpchar(10) ,proc8 bpchar(10) ,
	proc9 bpchar(10) ,proc10 bpchar(10) ,proc11 bpchar(10) ,proc12 bpchar(10) ,proc13 bpchar(10) ,proc14 bpchar(10) ,proc15 bpchar(10) ,
	
	agegrp int2 ,
	eeclass int2 ,
	eestatu int2 ,
	egeoloc int2 ,
	eidflag int2 ,
	emprel int2 ,
	enrflag int2 ,
	phyflag int2 ,
	rx int2 ,
	sex int2 ,
	state int2 ,
	hlthplan int2 ,
	indstry bpchar(5) 
) 
LOCATION ( 
'gpfdist://c252-140:8801/*'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_mdcri_v2
limit 1000;

insert into truven.mdcri (SEQNUM,VERSION,EFAMID,ENROLID,DOBYR,YEAR,ADMDATE,AGE,CASEID,DAYS,DISDATE,DRG,DXVER,
EMPZIP,HOSPNET,HOSPPAY,MHSACOVG,PDX,PHYSID,PHYSNET,PHYSPAY,PLANTYP,PPROC,
TOTCOB,TOTCOINS,TOTCOPAY,TOTDED,TOTNET,TOTPAY,ADMTYP,MDC,DSTATUS,REGION,DATATYP,
DX1,DX2,DX3,DX4,DX5,DX6,DX7,DX8,DX9,DX10,DX11,DX12,DX13,DX14,DX15,
PROC1,PROC2,PROC3,PROC4,PROC5,PROC6,PROC7,PROC8,PROC9,PROC10,PROC11,PROC12,PROC13,PROC14,PROC15,
AGEGRP,EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,RX,SEX,STATE,HLTHPLAN,INDSTRY)
select SEQNUM,VERSION,EFAMID,ENROLID,DOBYR,YEAR,ADMDATE,AGE,CASEID,DAYS,DISDATE,DRG,DXVER,
EMPZIP,HOSPNET,HOSPPAY,MHSACOVG,PDX,PHYSID,PHYSNET,PHYSPAY,PLANTYP,PPROC,
TOTCOB,TOTCOINS,TOTCOPAY,TOTDED,TOTNET,TOTPAY,ADMTYP,MDC,DSTATUS,REGION,DATATYP,
DX1,DX2,DX3,DX4,DX5,DX6,DX7,DX8,DX9,DX10,DX11,DX12,DX13,DX14,DX15,
PROC1,PROC2,PROC3,PROC4,PROC5,PROC6,PROC7,PROC8,PROC9,PROC10,PROC11,PROC12,PROC13,PROC14,PROC15,
AGEGRP,EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,RX,SEX,STATE,HLTHPLAN,INDSTRY
from ext_mdcri_v2;

-- Verify

select count(*) from truven.mdcri;



-- Fix storage options
create table truven.mdcri_new 
WITH (appendonly=true, orientation=column)
as (select * from truven.mdcri)
distributed randomly;

drop table truven.mdcri;
alter table truven.mdcri_new rename to mdcri;

