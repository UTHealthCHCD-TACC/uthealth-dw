/* ******************************************************************************************************
 *  Collection of queries for exploring database settings and performance/resource usage
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wallingTACC  || 10/27/2021 || Merging mdcr and mdcr into single script.  find/replace to switch between the two as fields are equal
 * ******************************************************************************************************
 */

/*
v1 Fields:

SEQNUM,VERSION,DX1,DX2,PROC1,FACHDID,EFAMID,ENROLID,DOBYR,YEAR,AGE,BILLTYP,CAP_SVC,CASEID,COB,COINS,COPAY,
DEDUCT,DX3,DX4,DX5,DX6,DX7,DX8,DX9,EMPZIP,MHSACOVG,MSCLMID,NETPAY,NTWKPROV,PAIDNTWK,PDDATE,PLANTYP,
PROC2,PROC3,PROC4,PROC5,PROC6,PROVID,SVCDATE,TSVCDAT,MDC,DSTATUS,REGION,STDPLAC,STDPROV,DATATYP,PLANKEY,WGTKEY,AGEGRP,
ECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY


v2 Fields: (Drop PLANKEY and WGTKEY, Add DXVER and NPI)

SEQNUM,VERSION,DX1,DX2,DXVER,PROC1,FACHDID,EFAMID,ENROLID,DOBYR,YEAR,AGE,BILLTYP,CAP_SVC,CASEID,COB,COINS,COPAY,
DEDUCT,DX3,DX4,DX5,DX6,DX7,DX8,DX9,EMPZIP,MHSACOVG,MSCLMID,NETPAY,NPI,NTWKPROV,PAIDNTWK,PDDATE,PLANTYP,
PROC2,PROC3,PROC4,PROC5,PROC6,PROVID,SVCDATE,TSVCDAT,MDC,DSTATUS,REGION,STDPLAC,STDPROV,DATATYP,AGEGRP,
EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY

v3 Fields: (Added POADX# fields)

SEQNUM,VERSION,DX1,DX2,DXVER,PROC1,FACHDID,EFAMID,ENROLID,DOBYR,YEAR,AGE,BILLTYP,CAP_SVC,CASEID,COB,COINS,COPAY,
DEDUCT,DX3,DX4,DX5,DX6,DX7,DX8,DX9,EMPZIP,MHSACOVG,MSCLMID,NETPAY,NPI,NTWKPROV,PAIDNTWK,PDDATE,PLANTYP,
POADX1,POADX2,POADX3,POADX4,POADX5,POADX6,POADX7,POADX8,POADX9,
PROC2,PROC3,PROC4,PROC5,PROC6,PROVID,SVCDATE,TSVCDAT,MDC,DSTATUS,REGION,STDPLAC,STDPROV,DATATYP,AGEGRP,
EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY

vDMS Fields: (Drop EMPZIP, MSCLMID, Add MSA

SEQNUM,VERSION,DX1,DX2,PROC1,FACHDID,EFAMID,ENROLID,DOBYR,YEAR,AGE,BILLTYP,CAP_SVC,CASEID,COB,COINS,COPAY,DEDUCT
,DX3,DX4,DX5,DX6,DX7,DX8,DX9,MHSACOVG,NETPAY,NTWKPROV,PAIDNTWK,PDDATE,PLANTYP,PROC2,PROC3,PROC4,PROC5,PROC6,PROVID
,SVCDATE,TSVCDAT,MDC,DSTATUS,REGION,MSA,STDPLAC,STDPROV,DATATYP,PLANKEY,WGTKEY,AGEGRP,EECLASS,EESTATU,EGEOLOC,EIDFLAG,
EMPREL,ENRFLAG,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY

v4 Fields: (Added MEDADV# fields)

SEQNUM,VERSION,DX1,DX2,DXVER,PROC1,FACHDID,EFAMID,ENROLID,DOBYR,YEAR,AGE,BILLTYP,CAP_SVC,CASEID,COB,COINS,COPAY,
DEDUCT,DX3,DX4,DX5,DX6,DX7,DX8,DX9,EMPZIP,MHSACOVG,MSCLMID,NETPAY,NPI,NTWKPROV,PAIDNTWK,PDDATE,PLANTYP,
POADX1,POADX2,POADX3,POADX4,POADX5,POADX6,POADX7,POADX8,POADX9,
PROC2,PROC3,PROC4,PROC5,PROC6,PROVID,SVCDATE,TSVCDAT,MDC,DSTATUS,REGION,STDPLAC,STDPROV,DATATYP,MEDADV,AGEGRP,
EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY

v5 fields: Moved MEDADV to end
*/

drop table truven.mdcrf;
CREATE TABLE truven.mdcrf (
	seqnum numeric NULL,
	version int2 NULL,
	dx1 bpchar(10) NULL,
	dx2 bpchar(10) NULL,
	dxver bpchar(10) NULL,
	proc1 bpchar(10) NULL,
	fachdid numeric NULL,
	efamid numeric NULL,
	enrolid numeric NULL,
	dobyr numeric NULL,
	year numeric NULL,
	age numeric NULL,
	billtyp bpchar(5) null,
	cap_svc bpchar(1) NULL,
	caseid numeric null,
	cob numeric NULL,
	coins numeric NULL,
	copay numeric NULL,
	
	deduct numeric NULL,
	dx3 bpchar(10) NULL,
	dx4 bpchar(10) NULL,
	dx5 bpchar(10) NULL,
	dx6 bpchar(10) NULL,
	dx7 bpchar(10) NULL,
	dx8 bpchar(10) NULL,
	dx9 bpchar(10) NULL,
	empzip numeric null,
	mhsacovg int2 NULL,
	msclmid numeric null,
	netpay numeric NULL,
	npi bpchar(10) null,
	ntwkprov bpchar(1) NULL,
	paidntwk bpchar(1) NULL,
	pddate date NULL,
	plantyp numeric NULL,
	
	proc2 bpchar(10) NULL,
	proc3 bpchar(10) NULL,
	proc4 bpchar(10) NULL,
	proc5 bpchar(10) NULL,
	proc6 bpchar(10) NULL,
	provid numeric NULL,
	svcdate date null,
	tsvcdat date null,
	mdc bpchar(10) null,
	dstatus int2 null,
	region int2 null,
	msa bpchar(7) ,
	stdplac numeric NULL,
	stdprov numeric NULL,
	datatyp numeric null,
	plankey numeric null,
	wgtkey numeric null,
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

alter TABLE truven.mdcrf
add column poadx1 bpchar(1),
add column poadx2 bpchar(1),
add column 	poadx3 bpchar(1),
add column 	poadx4 bpchar(1),
add column 	poadx5 bpchar(1),
add column 	poadx6 bpchar(1),
add column 	poadx7 bpchar(1),
add column 	poadx8 bpchar(1),
add column 	poadx9 bpchar(1);

alter table truven.mdcrf add column medadv int2;
 
/*
 * V5
 */

drop external table ext_mdcrf;
CREATE EXTERNAL TABLE ext_mdcrf (
	seqnum numeric ,
	version int2 ,
	dx1 bpchar(10) ,
	dx2 bpchar(10) ,
	dxver bpchar(10) ,
	proc1 bpchar(10) ,
	fachdid numeric ,
	efamid numeric ,
	enrolid numeric ,
	dobyr numeric ,
	year numeric ,
	age numeric ,
	billtyp bpchar(5) ,
	cap_svc bpchar(1) ,
	caseid numeric ,
	cob numeric ,
	coins numeric ,
	copay numeric ,
	
	deduct numeric ,
	dx3 bpchar(10) ,
	dx4 bpchar(10) ,
	dx5 bpchar(10) ,
	dx6 bpchar(10) ,
	dx7 bpchar(10) ,
	dx8 bpchar(10) ,
	dx9 bpchar(10) ,
	empzip numeric ,
	mhsacovg int2 ,
	msclmid numeric ,
	netpay numeric ,
	npi bpchar(10) ,
	ntwkprov bpchar(1) ,
	paidntwk bpchar(1) ,
	pddate date ,
	plantyp numeric ,
	
	poadx1 bpchar(1),
	poadx2 bpchar(1),
	poadx3 bpchar(1),
	poadx4 bpchar(1),
	poadx5 bpchar(1),
	poadx6 bpchar(1),
	poadx7 bpchar(1),
	poadx8 bpchar(1),
	poadx9 bpchar(1),
	
	proc2 bpchar(10) ,
	proc3 bpchar(10) ,
	proc4 bpchar(10) ,
	proc5 bpchar(10) ,
	proc6 bpchar(10) ,
	provid numeric ,
	svcdate date ,
	tsvcdat date ,
	mdc bpchar(10) ,
	dstatus int2 ,
	region int2 ,
	stdplac numeric ,
	stdprov numeric ,
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
'gpfdist://greenplum01:8081/uthealth/truven/MDCRF*'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_mdcrf
limit 1000;

insert into truven.mdcrf (SEQNUM,VERSION,DX1,DX2,PROC1,FACHDID,EFAMID,ENROLID,DOBYR,YEAR,AGE,BILLTYP,CAP_SVC,CASEID,COB,COINS,COPAY,
DEDUCT,DX3,DX4,DX5,DX6,DX7,DX8,DX9,EMPZIP,MHSACOVG,MSCLMID,NETPAY,NTWKPROV,PAIDNTWK,PDDATE,PLANTYP,
POADX1,POADX2,POADX3,POADX4,POADX5,POADX6,POADX7,POADX8,POADX9,
PROC2,PROC3,PROC4,PROC5,PROC6,PROVID,SVCDATE,TSVCDAT,MDC,DSTATUS,REGION,STDPLAC,STDPROV,DATATYP,MEDADV,AGEGRP,
EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY)
select SEQNUM,VERSION,DX1,DX2,PROC1,FACHDID,EFAMID,ENROLID,DOBYR,YEAR,AGE,BILLTYP,CAP_SVC,CASEID,COB,COINS,COPAY,
DEDUCT,DX3,DX4,DX5,DX6,DX7,DX8,DX9,EMPZIP,MHSACOVG,MSCLMID,NETPAY,NTWKPROV,PAIDNTWK,PDDATE,PLANTYP,
POADX1,POADX2,POADX3,POADX4,POADX5,POADX6,POADX7,POADX8,POADX9,
PROC2,PROC3,PROC4,PROC5,PROC6,PROVID,SVCDATE,TSVCDAT,MDC,DSTATUS,REGION,STDPLAC,STDPROV,DATATYP,MEDADV,AGEGRP,
EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY
from ext_mdcrf;


-- Verify

select count(*), min(year), max(year) from truven.mdcrf;


-- Truncate and Reload

delete from truven.mdcrf where year>=2020;


