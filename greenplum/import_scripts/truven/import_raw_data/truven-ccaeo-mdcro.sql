/* ******************************************************************************************************
 *  Collection of queries for exploring database settings and performance/resource usage
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wallingTACC  || 10/27/2021 || Merging mdcr and MDCR into single script.  find/replace to switch between the two as fields are equal
 * ******************************************************************************************************
 */

/*
v1 fields: 
SEQNUM,VERSION,DX1,DX2,PROC1,PROCTYP,EFAMID,ENROLID,REVCODE,SVCDATE,DOBYR,YEAR,
AGE,CAP_SVC,COB,COINS,COPAY,DEDUCT,DX3,DX4,EMPZIP,FACHDID,FACPROF,MHSACOVG,MSCLMID,NETPAY,NTWKPROV,
PAIDNTWK,PAY,PDDATE,PLANTYP,PROCGRP,PROCMOD,PROVID,QTY,SVCSCAT,TSVCDAT,MDC,REGION,STDPLAC,STDPROV,DATATYP,PLANKEY,
WGTKEY,AGEGRP,EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY

v2 fields:
SEQNUM,VERSION,DX1,DX2,DXVER,PROC1,PROCTYP,EFAMID,ENROLID,REVCODE,SVCDATE,DOBYR,YEAR,
AGE,CAP_SVC,COB,COINS,COPAY,DEDUCT,DX3,DX4,EMPZIP,FACHDID,FACPROF,MHSACOVG,MSCLMID,NETPAY,NPI,NTWKPROV,PAIDNTWK,PAY,PDDATE,
PLANTYP,PROCGRP,PROCMOD,PROVID,QTY, SVCSCAT,TSVCDAT,UNITS,MDC,REGION,STDPLAC,STDPROV,DATATYP,AGEGRP,
EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY

v3 fields: Added MEDADV
SEQNUM,VERSION,DX1,DX2,DXVER,PROC1,PROCTYP,EFAMID,ENROLID,REVCODE,SVCDATE,DOBYR,YEAR,
AGE,CAP_SVC,COB,COINS,COPAY,DEDUCT,DX3,DX4,EMPZIP,FACHDID,FACPROF,MHSACOVG,MSCLMID,NETPAY,NPI,NTWKPROV,PAIDNTWK,PAY,PDDATE,
PLANTYP,PROCGRP,PROCMOD,PROVID,QTY, SVCSCAT,TSVCDAT,UNITS,MDC,REGION,STDPLAC,STDPROV,DATATYP,MEDADV,AGEGRP,
EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY

v4 fields: Moved MEDADV to end
SEQNUM,VERSION,DX1,DX2,DXVER,PROC1,PROCTYP,EFAMID,ENROLID,REVCODE,SVCDATE,DOBYR,YEAR,
AGE,CAP_SVC,COB,COINS,COPAY,DEDUCT,DX3,DX4,EMPZIP,FACHDID,FACPROF,MHSACOVG,MSCLMID,NETPAY,NPI,NTWKPROV,PAIDNTWK,PAY,PDDATE,
PLANTYP,PROCGRP,PROCMOD,PROVID,QTY, SVCSCAT,TSVCDAT,UNITS,MDC,REGION,STDPLAC,STDPROV,DATATYP,AGEGRP,
EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY,MEDADV

*/

create schema truven;

create table truven.mdcro (like truven.mdcro)
WITH (appendonly=true, orientation=column, compresstype=zlib);

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

--New Columns
alter table truven.mdcro add column medadv int2;

/*
 * V4
 */
drop external table ext_mdcro;
CREATE EXTERNAL TABLE ext_mdcro (
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
	indstry bpchar(5),
	medadv int2
) 
LOCATION ( 
'gpfdist://greenplum01:8081/uthealth/truven/MDCRO*'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_mdcro
limit 1000;

--Load New

insert into truven.mdcro (SEQNUM,VERSION,DX1,DX2,DXVER,PROC1,PROCTYP,EFAMID,ENROLID,REVCODE,SVCDATE,DOBYR,YEAR,
AGE,CAP_SVC,COB,COINS,COPAY,DEDUCT,DX3,DX4,EMPZIP,FACHDID,FACPROF,MHSACOVG,MSCLMID,NETPAY,NPI,NTWKPROV,PAIDNTWK,PAY,PDDATE,
PLANTYP,PROCGRP,PROCMOD,PROVID,QTY, SVCSCAT,TSVCDAT,UNITS,MDC,REGION,STDPLAC,STDPROV,DATATYP,MEDADV,AGEGRP,
EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY)
select SEQNUM,VERSION,DX1,DX2,DXVER,PROC1,PROCTYP,EFAMID,ENROLID,REVCODE,SVCDATE,DOBYR,YEAR,
AGE,CAP_SVC,COB,COINS,COPAY,DEDUCT,DX3,DX4,EMPZIP,FACHDID,FACPROF,MHSACOVG,MSCLMID,NETPAY,NPI,NTWKPROV,PAIDNTWK,PAY,PDDATE,
PLANTYP,PROCGRP,PROCMOD,PROVID,QTY, SVCSCAT,TSVCDAT,UNITS,MDC,REGION,STDPLAC,STDPROV,DATATYP,MEDADV,AGEGRP,
EECLASS,EESTATU,EGEOLOC,EIDFLAG,EMPREL,ENRFLAG,PHYFLAG,RX,SEX,HLTHPLAN,INDSTRY
from ext_mdcro;


-- Truncate and Refresh

delete from truven.mdcro where YEAR>=2020;


-- Query

select year, count(*)
from truven.mdcro
group by 1
order by 1;

