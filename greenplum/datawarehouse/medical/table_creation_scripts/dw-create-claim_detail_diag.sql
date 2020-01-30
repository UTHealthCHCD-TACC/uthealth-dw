

drop table if exists dw_qa.claim_detail_diag;

create table dw_qa.claim_detail_diag (
	id bigserial not null,
	uth_claim_id numeric,
	claim_sequence_number int4,
	diagnosis_cd varchar,
	diagnosis_position int4
) 
with (appendonly=true, orientation=column)
distributed by(id);

--Greenplum performance optimization for serial/sequence
alter sequence dw_qa.claim_detail_diag_id_seq cache 100;

--Optum load: 
insert into data_warehouse.claim_detail(claim_id, provider_id, seq_num, proc_id, proc, cost, paid, service_date, paid_date, 
billing_provider_id_src, service_provider_id_src)
select 
from optum_dod.medical;

select count(*)
from data_warehouse.claim_detail;


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




