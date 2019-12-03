

--Optum load: 
insert into data_warehouse.claim_detail(claim_header_id, provider_id, seq_num, proc_code, proc_mod, cost, paid, service_date, paid_date, 
billing_provider_id_src, service_provider_id_src)
select distinct ch.id, m.prov, cast(m.clmseq as int8), m.proc_cd, m.procmod, m.std_cost, m.coins, m.fst_dt, m.paid_dt,
m.bill_prov, m.service_prov
from data_warehouse.claim_header ch
join optum_dod_medical m on ch.claim_id_src=m.clmid and ch.member_id_src=m.patid;

select count(*)
from data_warehouse.claim_detail;



/*
 * Truven 'medical' data is split between inpatient and outpatient data tables (ex. ccaes and ccaeo). 
 */
--Truven load Services/Inpatient
insert into dev.claim_detail_v3(claim_header_id, provider_id, seq_num, proc_code, proc_mod, cost, paid, service_date, paid_date, 
billing_provider_id_src, service_provider_id_src)
select distinct ch.id, cast(s.fachdid as varchar), s.seqnum, s.proc1, s.procmod, null, s.netpay, s.svcdate, s.pddate, 
null, null
from data_warehouse.claim_header ch
join dev2016.truven_ccaes s on ch.claim_id_src=cast(s.caseid as varchar) and ch.member_id_src=cast(s.enrolid as varchar);

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
from data_warehouse.claim_header
group by 1;

select data_source, count(*)
from dev.claim_header_v1
group by 1;

select data_source, count(*)
from dev.claim_detail_v1
group by 1;

select *
from dev.claim_detail_v1
limit 10;

