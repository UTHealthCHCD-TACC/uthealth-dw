/* ******************************************************************************************************
 *  Collection of queries for exploring database settings and performance/resource usage
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wallingTACC  || 10/27/2021 || Merging mdcr and MDCR into single script.  find/replace to switch between the two as fields are equal
 * ******************************************************************************************************
 */

/*
 This stupid file has 3 different versions of the headers
 
v1 Fields:

SEQNUM,VERSION,DX1,DX2,PROC1,PROCTYP,DOBYR,YEAR,ADMDATE,AGE,
CAP_SVC,CASEID,COB,COINS,COPAY,DEDUCT,DISDATE,DRG,DX3,DX4,FACHDID,FACPROF,MHSACOVG,MSCLMID,
NETPAY,NTWKPROV,PAIDNTWK,PAY,PDDATE,PDX,PPROC,PROCMOD,PROVID,QTY,REVCODE,
SVCDATE,SVCSCAT,TSVCDAT,ADMTYP,MDC,DSTATUS,STDPLAC,STDPROV,WGTKEY,
EFAMID,ENROLID,EMPZIP,PLANTYP,REGION,DATATYP,PLANKEY,AGEGRP,
EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY

v2 Fields: (CASEID and DISDATE change position)

SEQNUM,VERSION,DX1,DX2,PROC1,PROCTYP,CASEID,DISDATE,DOBYR,YEAR,ADMDATE,AGE,
CAP_SVC,COB,COINS,COPAY,DEDUCT,DRG,DX3,DX4,FACHDID,FACPROF,MHSACOVG,MSCLMID,
NETPAY,NTWKPROV,PAIDNTWK,PAY,PDDATE,PDX,PPROC,PROCMOD,PROVID,QTY,REVCODE,
SVCDATE,SVCSCAT,TSVCDAT,ADMTYP,MDC,DSTATUS,STDPLAC,STDPROV,WGTKEY,
EFAMID,ENROLID,EMPZIP,PLANTYP,REGION,DATATYP,PLANKEY,AGEGRP,
EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY

v3 Fields: (Drop PLANKEY and WGTKEY, Add DXVER, UNITS and NPI) 2015/16/18

SEQNUM,VERSION,DX1,DX2,DXVER,PROC1,PROCTYP,CASEID,DISDATE,DOBYR,YEAR,ADMDATE,AGE,
CAP_SVC,COB,COINS,COPAY,DEDUCT,DRG,DX3,DX4,FACHDID,FACPROF,MHSACOVG,MSCLMID,
NETPAY,NPI,NTWKPROV,PAIDNTWK,PAY,PDDATE,PDX,PPROC,PROCMOD,PROVID,QTY,REVCODE,
SVCDATE,SVCSCAT,TSVCDAT,UNITS,ADMTYP,MDC,DSTATUS,STDPLAC,STDPROV,
EFAMID,ENROLID,EMPZIP,PLANTYP,REGION,DATATYP,AGEGRP,
EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY

v4 Fields: (CASEID & DISDATE moved) 2017

SEQNUM,VERSION,DX1,DX2,DXVER,PROC1,PROCTYP,DOBYR,YEAR,ADMDATE,AGE,
CAP_SVC,CASEID,COB,COINS,COPAY,DEDUCT,DISDATE,DRG,DX3,DX4,FACHDID,FACPROF,MHSACOVG,MSCLMID,
NETPAY,NPI,NTWKPROV,PAIDNTWK,PAY,PDDATE,PDX,PPROC,PROCMOD,PROVID,QTY,REVCODE,
SVCDATE,SVCSCAT,TSVCDAT,UNITS,ADMTYP,MDC,DSTATUS,STDPLAC,STDPROV,
EFAMID,ENROLID,EMPZIP,PLANTYP,REGION,DATATYP,AGEGRP,
EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY


vDMS:

SEQNUM,VERSION,DX1,DX2,PROC1,PROCTYP,DOBYR,YEAR,ADMDATE,AGE,CAP_SVC,CASEID,COB,COINS,
COPAY,DEDUCT,DISDATE,DRG,DX3,DX4,FACHDID,FACPROF,MHSACOVG,NETPAY,NTWKPROV,PAIDNTWK,PAY,
PDDATE,PDX,PPROC,PROCMOD,PROVID,QTY,REVCODE,SVCDATE,SVCSCAT,TSVCDAT,ADMTYP,MDC,DSTATUS,
STDPLAC,STDPROV,WGTKEY,EFAMID,ENROLID,PLANTYP,REGION,MSA,DATATYP,PLANKEY,AGEGRP,EECLASS,
EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY

v2018

SEQNUM,VERSION,DX1,DX2,DXVER,PROC1,PROCTYP,CASEID,DISDATE,DOBYR,YEAR,ADMDATE,AGE,
CAP_SVC,COB,COINS,COPAY,DEDUCT,DRG,DX3,DX4,FACHDID,FACPROF,MHSACOVG,MSCLMID,
NETPAY,NPI,NTWKPROV,PAIDNTWK,PAY,PDDATE,PDX,PPROC,PROCMOD,PROVID,QTY,REVCODE,
SVCDATE,SVCSCAT,TSVCDAT,UNITS,ADMTYP,MDC,DSTATUS,STDPLAC,STDPROV,
EFAMID,ENROLID,EMPZIP,PLANTYP,REGION,DATATYP,AGEGRP,
EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY

v5 Added MEDADV in 2020
SEQNUM,VERSION,DX1,DX2,DXVER,PROC1,PROCTYP,CASEID,DISDATE,DOBYR,YEAR,ADMDATE,AGE,
CAP_SVC,COB,COINS,COPAY,DEDUCT,DRG,DX3,DX4,FACHDID,FACPROF,MHSACOVG,MSCLMID,
NETPAY,NPI,NTWKPROV,PAIDNTWK,PAY,PDDATE,PDX,PPROC,PROCMOD,PROVID,QTY,REVCODE,
SVCDATE,SVCSCAT,TSVCDAT,UNITS,ADMTYP,MDC,DSTATUS,STDPLAC,STDPROV,
EFAMID,ENROLID,EMPZIP,PLANTYP,REGION,DATATYP,MEDADV,AGEGRP,
EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY

v6 Moved MEDADV to end
SEQNUM,VERSION,DX1,DX2,DXVER,PROC1,PROCTYP,CASEID,DISDATE,DOBYR,YEAR,ADMDATE,AGE,
CAP_SVC,COB,COINS,COPAY,DEDUCT,DRG,DX3,DX4,FACHDID,FACPROF,MHSACOVG,MSCLMID,
NETPAY,NPI,NTWKPROV,PAIDNTWK,PAY,PDDATE,PDX,PPROC,PROCMOD,PROVID,QTY,REVCODE,
SVCDATE,SVCSCAT,TSVCDAT,UNITS,ADMTYP,MDC,DSTATUS,STDPLAC,STDPROV,
EFAMID,ENROLID,EMPZIP,PLANTYP,REGION,DATATYP,AGEGRP,
EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY,MEDADV

*/

create table truven.mdcrs (like truven.mdcrs)
WITH (appendonly=true, orientation=column, compresstype=zlib);

CREATE TABLE truven.mdcrs (
	seqnum numeric NULL,
	version int2 NULL,
	dx1 bpchar(10) NULL,
	dx2 bpchar(10) NULL,
	dxver bpchar(10) NULL,
	proc1 bpchar(10) NULL,
	proctyp bpchar(10) null,
	caseid numeric null,
	disdate date null,
	dobyr numeric NULL,
	year numeric NULL,
	admdate date null,
	age numeric NULL,
	
	cap_svc bpchar(1) NULL,
	cob numeric NULL,
	coins numeric NULL,
	copay numeric NULL,
	deduct numeric null,
	drg numeric null,
	dx3 bpchar(10) null,
	dx4 bpchar(10) null,
	fachdid numeric null,
	facprof bpchar(1) null,
	mhsacovg int2 NULL,
	msclmid numeric null,
	
	netpay numeric NULL,
	npi bpchar(10) null,
	ntwkprov bpchar(1) NULL,
	paidntwk bpchar(1) NULL,
	pay numeric NULL,
	pddate date NULL,
	pdx bpchar(10) null,
	pproc bpchar(10) NULL,
	procmod bpchar(2) NULL,
	provid numeric NULL,
	qty numeric null,
	revcode int2 NULL,
	
	svcdate date null,
	svcscat int4 null,
	tsvcdat date null,
	units numeric null,
	admtyp numeric null,
	mdc int2 null,
	dstatus numeric null,
	stdplac numeric null,
	stdprov numeric null,
	wgtkey numeric null,
	
	efamid numeric NULL,
	enrolid numeric NULL,
	empzip numeric null,
	plantyp numeric null,
	region int2 null,
	msa numeric null,
	datatyp numeric null,
	plankey numeric null,	
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
	hlthplan int2 null,
	indstry bpchar(5) null
)
DISTRIBUTED RANDOMLY;

/*
 * V6
 */

drop external table ext_mdcrs;
CREATE EXTERNAL TABLE ext_mdcrs (
	seqnum numeric ,
	version int2 ,
	dx1 bpchar(10) ,
	dx2 bpchar(10) ,
	dxver bpchar(10) ,
	proc1 bpchar(10) ,
	proctyp bpchar(10) ,
	caseid numeric ,
	disdate date ,
	dobyr numeric ,
	year numeric ,
	admdate date ,
	age numeric ,
	
	cap_svc bpchar(1) ,
	cob numeric ,
	coins numeric ,
	copay numeric ,
	deduct numeric ,
	drg numeric ,
	dx3 bpchar(10) ,
	dx4 bpchar(10) ,
	fachdid numeric ,
	facprof bpchar(1) ,
	mhsacovg int2 ,
	msclmid numeric ,
	
	netpay numeric ,
	npi bpchar(10) ,
	ntwkprov bpchar(1) ,
	paidntwk bpchar(1) ,
	pay numeric ,
	pddate date ,
	pdx bpchar(10) ,
	pproc bpchar(10) ,
	procmod bpchar(2) ,
	provid numeric ,
	qty numeric ,
	revcode int2 ,
	
	svcdate date ,
	svcscat int4 ,
	tsvcdat date ,
	units numeric ,
	admtyp numeric ,
	mdc int2 ,
	dstatus numeric ,
	stdplac numeric ,
	stdprov numeric ,

	efamid numeric ,
	enrolid numeric ,
	empzip numeric ,
	plantyp numeric ,
	region int2 ,
	datatyp numeric ,
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
	hlthplan int2 ,
	indstry bpchar(5),
	medadv int2
) 
LOCATION ( 
'gpfdist://greenplum01:8081/uthealth/truven/MDCRS*'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_mdcrs
limit 1000;

--alter table truven.mdcrs add column medadv int2;

insert into truven.mdcrs (SEQNUM,VERSION,DX1,DX2,DXVER,PROC1,PROCTYP,CASEID,DISDATE,DOBYR,YEAR,ADMDATE,AGE,
CAP_SVC,COB,COINS,COPAY,DEDUCT,DRG,DX3,DX4,FACHDID,FACPROF,MHSACOVG,MSCLMID,
NETPAY,NPI,NTWKPROV,PAIDNTWK,PAY,PDDATE,PDX,PPROC,PROCMOD,PROVID,QTY,REVCODE,
SVCDATE,SVCSCAT,TSVCDAT,UNITS,ADMTYP,MDC,DSTATUS,STDPLAC,STDPROV,
EFAMID,ENROLID,EMPZIP,PLANTYP,REGION,DATATYP,MEDADV,AGEGRP,
EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY)
select SEQNUM,VERSION,DX1,DX2,DXVER,PROC1,PROCTYP,CASEID,DISDATE,DOBYR,YEAR,ADMDATE,AGE,
CAP_SVC,COB,COINS,COPAY,DEDUCT,DRG,DX3,DX4,FACHDID,FACPROF,MHSACOVG,MSCLMID,
NETPAY,NPI,NTWKPROV,PAIDNTWK,PAY,PDDATE,PDX,PPROC,PROCMOD,PROVID,QTY,REVCODE,
SVCDATE,SVCSCAT,TSVCDAT,UNITS,ADMTYP,MDC,DSTATUS,STDPLAC,STDPROV,
EFAMID,ENROLID,EMPZIP,PLANTYP,REGION,DATATYP,MEDADV,AGEGRP,
EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY
from ext_mdcrs;

-- Verify

select count(*), min(year), max(year) from truven.mdcrs;

select date('2017');


-- Truncate and Replace

delete from truven.mdcrs where year>=2020;


