
--Main table
drop table data_warehouse.claim_detail;
create table data_warehouse.claim_detail (
id bigserial NOT NULL,
	claim_id bigserial,
	seq_num_src,
	seq_num_derived,
	proc_id,
	proc_src,
	proc_mod_src,
	cost numeric,
	paid numeric,
	service_date date,
	paid_date date,
	billing_provider_id_src varchar,
	service_provider_id_src varchar
) 
WITH (appendonly=true, orientation=column)
distributed randomly;

--Greenplum performance optimization for serial/sequence
alter sequence data_warehouse.claim_detail_id_seq cache 400;

--Optum load: 
insert into data_warehouse.claim_detail(claim_id, seq_num_src, proc_src, proc_mod_src, cost, paid, service_date, paid_date, 
billing_provider_id_src, service_provider_id_src)
select 
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


--Verify
select source, count(*)
from data_warehouse.medical
group by 1;


