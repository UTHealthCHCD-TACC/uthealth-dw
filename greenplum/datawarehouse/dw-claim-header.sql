
--Main table
drop table data_warehouse.claim_header;
create table data_warehouse.claim_header (
id bigserial NOT NULL,
	"source" bpchar(4),
	member_id_src int8,
	uth_member_id varchar, 
	claim_id_src varchar,
	uth_claim_id varchar,
	claim_type varchar,
	in_network boolean,
	admit_id_src varchar,
	admit_date date,
	discharge_date date,
	admit_type_src varchar,
	admit_channel_src varchar,
	total_cost numeric,
	total_paid numeric
) 
WITH (appendonly=true, orientation=column)
distributed randomly;

--Greenplum performance optimization for serial/sequence
alter sequence data_warehouse.claim_header_id_seq cache 400;

--Optum load: 
insert into data_warehouse.claim_header(source, member_id_src, claim_id_src, 
admit_id_src, admit_date, discharge_date, admit_type_src, admit_channel_src, 
total_cost, total_paid)


select 'OPTD', m.patid, m.clmid, 
max(conf.conf_id) as conf_id, 
min(conf.admit_date) as admit_date, 
min(conf.disch_date) as disch_date,
sum(m.charge) as total_cost, 
sum(m.copay) as total_paid, 
count(distinct conf.conf_id) as conf_cnt, 
count(*) as record_cnt
from optum_dod_medical m
left join optum_dod_confinement conf on m.conf_id=conf.conf_id
where clmid='187810755'
group by 1, 2, 3;

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


