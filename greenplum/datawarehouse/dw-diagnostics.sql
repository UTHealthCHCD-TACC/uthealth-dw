
--Main table
drop table data_warehouse.diagnostics;
create table data_warehouse.diagnostics (
id bigserial NOT NULL,
	"source" bpchar(2) NULL,
	mbr_id int8 NULL,
	claim_typ bpchar(3) NULL,
	claim_no varchar NULL,
	case_link_key varchar NULL,
	adm_dt date NULL,
	dc_dt date NULL,
	dc_stat varchar NULL,
	drg_cd varchar NULL,
	tot_chgs numeric NULL,
	tot_alwd numeric NULL,
	tot_paid numeric NULL,
	billed_amt numeric NULL,
	allowed_amt numeric NULL,
	paid_amt numeric NULL,
	ded_amt numeric NULL,
	copay_amt numeric NULL,
	coins_amt numeric NULL,
	cob_amt numeric NULL,
	adjud_date date NULL
) 
WITH (appendonly=true, orientation=column)
distributed randomly;

--Greenplum performance optimization for serial/sequence
alter sequence data_warehouse.medical_id_seq cache 200;

--Optum load: 
insert into data_warehouse.medical(source, mbr_id, claim_typ, claim_no, case_link_key, adm_dt, dc_dt, dc_stat, drg_cd,
tot_chgs, tot_alwd, tot_paid, billed_amt, allowed_amt, paid_amt, ded_amt, copay_amt, coins_amt, cob_amt, adjud_date)
select 'od', patid, loc_cd, clmid, conf_id, fst_dt, lst_dt, dstatus, drg,
charge, null, null, null, std_cost, null, null, copay, coins, null, paid_dt
from optum_dod.medical;

select count(*)
from data_warehouse.medical;


-- Diagnostics


/*
 * Truven 'medical' data is split between inpatient and outpatient data tables (ex. ccaei and ccaeo). 
 */
--Truven load Inpatient
insert into data_warehouse.medical(source, mbr_id, claim_typ, claim_no, case_link_key, adm_dt, dc_dt, dc_stat, drg_cd,
tot_chgs, tot_alwd, tot_paid, billed_amt, allowed_amt, paid_amt, ded_amt, copay_amt, coins_amt, cob_amt, adjud_date)
select 'to', enrolid, facprof, msclmid, caseid, admdate, disdate, dstatus, drg,
null, null, null, null, null, netpay, deduct, copay, coins, cob, pddate
from truven.ccaes;

update data_warehouse.medical
set source='ts' where source='to';

--Truven load Outpatient (Skipping for now)
insert into data_warehouse.medical(source, mbr_id, claim_typ, claim_no, case_link_key, adm_dt, dc_dt, dc_stat, drg_cd,
tot_chgs, tot_alwd, tot_paid, billed_amt, allowed_amt, paid_amt, ded_amt, copay_amt, coins_amt, cob_amt, adjud_date)
select 'to', enrolid, facprof, msclmid, null, null, null, null, null,
null, null, null, null, null, netpay, deduct, copay, coins, cob, pddate
from truven.ccaeo;


