/*
 * 
 */
drop table if exists dev.claim_detail_optum;
CREATE TABLE dev.claim_detail_optum (
	data_source bpchar(4) NULL,
	uth_claim_id int8 NULL,
	claim_sequence_number int4 NULL,
	claim_sequence_number_src text,
	uth_member_id int8 NULL,
	from_date_of_service date NULL,
	to_date_of_service date NULL,
	month_year_id numeric null, --int4 NULL,
	perf_provider_id numeric null, --int4 NULL,
	bill_provider_id numeric null, --int4 NULL,
	ref_provider_id numeric null, --int4 NULL,
	place_of_service int4 NULL,
	network_ind bool NULL,
	network_paid_ind bool NULL,
	admit_date date NULL,
	discharge_date date NULL,
	procedure_cd text NULL,
	procedure_type text NULL,
	proc_mod_1 bpchar(1) NULL,
	proc_mod_2 bpchar(1) NULL,
	revenue_code bpchar(4) NULL,
	charge_amount numeric(13,2) NULL,
	allowed_amount numeric(13,2) NULL,
	paid_amount numeric(13,2) NULL,
	copay numeric(13,2) NULL,
	deductible numeric(13,2) NULL,
	coins numeric(13,2) NULL,
	cob numeric(13,2) NULL,
	bill_type_inst bpchar(1) NULL,
	bill_type_class bpchar(1) NULL,
	bill_type_freq bpchar(1) NULL,
	units int4 NULL,
	drg_cd text NULL,
	drg_type text NULL
)
WITH (
	appendonly=true, orientation=column
)
DISTRIBUTED BY (uth_claim_id);

/*
 * This script assumes claim_header has already been loaded with mapped uth_*_ids
 */
analyze dev.claim_header_optum;
analyze dev.dim_uth_claim_id_optum;

--Optum load: 
insert into dev.claim_detail_optum(data_source,	uth_claim_id, uth_member_id,
    claim_sequence_number, claim_sequence_number_src,
	from_date_of_service, to_date_of_service, month_year_id,	
	perf_provider_id, bill_provider_id, ref_provider_id, place_of_service,
	network_ind, network_paid_ind,
	admit_date,	discharge_date,
	procedure_cd, procedure_type, proc_mod_1, proc_mod_2,
	revenue_code, charge_amount, allowed_amount, paid_amount, copay, deductible, coins, cob,
	bill_type_inst,	bill_type_class, bill_type_freq, units,
	drg_cd,	drg_type)
	
select 'optd', ch.uth_claim_id, ch.uth_member_id,
/*row_number() over (
		partition by uth_claim_id
		order by cast(m.clmseq as int8)) rownum,
*/ 
null, m.clmseq,
m.fst_dt, m.lst_dt, get_my_from_date(m.fst_dt),
m.prov, m.bill_prov, m.refer_prov, null, --place_of_service is an int, but optum is varchar -> m.pos,
null, null,
conf.admit_date, conf.disch_date,
m.proc_cd, null, substring(m.procmod, 1,1), substring(m.procmod, 2,1),
m.rvnu_cd, null, m.std_cost, null, m.copay, null, m.coins, null, --cob is an int, but optum is varchar -> m.cob,
null, null, null, null, --units gives integer out of range error -> m.units, 
m.drg, null
from dev.claim_header_optum ch
join optum_dod_medical m on ch.claim_id_src=m.clmid::text and ch.member_id_src=m.patid::text
left outer join optum_dod_confinement conf on m.conf_id=conf.conf_id;

--Set claim_seq_number to start at 1 for a given detail (more efficient then row_number()).
create temp table optum_claim_detail_sequence
as
select uth_claim_id, min(claim_seq_number_src) as min_seq, max(claim_seq_number_src) as max_seq
from dev.claim_detail_optum
group by 1;

update dev.claim_detail_optum a
set a.claim_sequence_number = a.claim_sequence_number_src::int8 - b.min_seq + 1
from optum_claim_detail_sequence b
where a.uth_claim_id=b.uth_claim_id;

/*
 * Scratch Space
 */
select get_my_from_date('2011-08-18'::date);

select count(*)
from dev.claim_detail_optum;

select *
from data_warehouse.claim_detail_v1
limit 10;



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

