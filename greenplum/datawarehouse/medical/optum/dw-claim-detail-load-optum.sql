/*
 * 
 */
drop table if exists dev.claim_detail_optum;
CREATE TABLE dev.claim_detail_optum_fix (
	data_source bpchar(4) NULL,
	uth_claim_id int8 NULL,
	claim_sequence_number int4 NULL,
	claim_sequence_number_src text,
	uth_member_id int8 NULL,
	from_date_of_service date NULL,
	to_date_of_service date NULL,
	month_year_id int4 null, --int4 NULL,
	perf_provider_id text null, --int4 NULL,
	bill_provider_id text null, --int4 NULL,
	ref_provider_id text null, --int4 NULL,
	place_of_service text NULL,
	network_ind bool NULL,
	network_paid_ind bool NULL,
	admit_date date NULL,
	discharge_date date NULL,
	procedure_cd text NULL,
	procedure_type text NULL,
	proc_mod_1 bpchar(1) NULL,
	proc_mod_2 bpchar(1) NULL,
	revenue_cd bpchar(4) NULL,
	charge_amount numeric(13,2) NULL,
	allowed_amount numeric(13,2) NULL,
	paid_amount numeric(13,2) NULL,
	copay numeric(13,2) NULL,
	deductible numeric(13,2) NULL,
	coins numeric(13,2) NULL,
	cob numeric(13,2) NULL,
	cob_type text null,
	bill_type_inst bpchar(1) NULL,
	bill_type_class bpchar(1) NULL,
	bill_type_freq bpchar(1) NULL,
	units int4 NULL,
	drg_cd text NULL
)
WITH (
	appendonly=true, orientation=column
)
DISTRIBUTED BY (uth_claim_id);

alter sequence dev.claim_detail_optum_fix_id_seq cache 200;

/*
 * This script assumes claim_header has already been loaded with mapped uth_*_ids
 */
analyze dev.claim_header_optum;
analyze dev.dim_uth_claim_id_optum;

--Optum load: 23 min for 2016
insert into dev.claim_detail_optum(data_source,	uth_claim_id, uth_member_id,
    claim_sequence_number, claim_sequence_number_src,
	from_date_of_service, to_date_of_service, month_year_id,	
	perf_provider_id, bill_provider_id, ref_provider_id, place_of_service,
	network_ind, network_paid_ind,
	admit_date,	discharge_date,
	procedure_cd, procedure_type, proc_mod_1, proc_mod_2,
	revenue_cd, charge_amount, allowed_amount, paid_amount, copay, deductible, coins, cob, cob_type,
	bill_type_inst,	bill_type_class, bill_type_freq, units,
	drg_cd)
select 'optz', ch.uth_claim_id, ch.uth_member_id,
trunc(m.clmseq::int4), m.clmseq,
m.fst_dt, m.lst_dt, get_my_from_date(m.fst_dt),
m.prov::text, m.bill_prov::text, m.refer_prov::text, m.pos,
null, null, --No mappings for network fields
conf.admit_date, conf.disch_date,
m.proc_cd, null, substring(m.procmod, 1,1), substring(m.procmod, 2,1),
m.rvnu_cd, null, m.std_cost, null, m.copay, null, m.coins, null, m.cob, --NOTE: cob is an int, but optum is varchar -> m.cob (Find where it is a numeric value, set other to zero), 	--NOTE: Left pad revenu_cd to 4 digits with leading zero
bt.inst_cd, bt.class_cd, null, m.units, --NOTE: bill_type_freq is null for optum
m.drg
from dev.claim_header_optum ch
join data_warehouse.dim_uth_claim_id uth on ch.uth_claim_id=uth.uth_claim_id
join optum_zip.medical m on ch.claim_id_src=m.clmid::text and ch.member_id_src=m.patid::text and uth.data_year=m.year
left outer join optum_zip.confinement conf on m.conf_id=conf.conf_id
left outer join reference_tables.ref_optum_bill_type_from_tos bt on m.tos_cd=bt.tos
where ch.data_source='optz' and m.year >= 2015 and m.year <= 2017;

/* NOTE: The following cd is not needed.  
 * However, it provides a more efficient mechanism (than row_number()) for resetting claim_sequence_number to start at 1
 * for cases where the source version is not 1, but uses sequential numbers (ex. 2345, 2346, 2347 -> 1, 2, 3)
 
--Set claim_seq_number to start at 1 for a given detail (more efficient then row_number()).
create temp table optum_claim_detail_sequence
as
select uth_claim_id, min(claim_sequence_number_src) as min_seq, max(claim_sequence_number_src) as max_seq
from dev.claim_detail_optum
group by 1;

update dev.claim_detail_optum a
set claim_sequence_number = claim_sequence_number_src::int8 - b.min_seq::int8 + 1
from optum_claim_detail_sequence b
where a.uth_claim_id=b.uth_claim_id;
*/

/*
 * Scratch Space
 */
insert into dev.claim_detail_optum_fix (data_source, uth_claim_id, uth_member_id,
    claim_sequence_number, claim_sequence_number_src,
	from_date_of_service, to_date_of_service, month_year_id,	
	perf_provider_id, bill_provider_id, ref_provider_id, place_of_service,
	network_ind, network_paid_ind,
	admit_date,	discharge_date,
	procedure_cd, procedure_type, proc_mod_1, proc_mod_2,
	revenue_cd, charge_amount, allowed_amount, paid_amount, copay, deductible, coins, cob, cob_type,
	bill_type_inst,	bill_type_class, bill_type_freq, units,
	drg_cd)
select distinct d.data_source, d.uth_claim_id, d.uth_member_id,
    claim_sequence_number, claim_sequence_number_src,
	d.from_date_of_service, d.to_date_of_service, month_year_id,	
	perf_provider_id, bill_provider_id, ref_provider_id, d.place_of_service,
	network_ind, network_paid_ind,
	admit_date,	discharge_date,
	procedure_cd, procedure_type, proc_mod_1, proc_mod_2,
	revenue_cd, charge_amount, allowed_amount, paid_amount, copay, deductible, coins, cob, cob_type,
	bill_type_inst,	bill_type_class, bill_type_freq, units,
	drg_cd
from dev.claim_detail_optum d
join dev.claim_header_optum h on d.uth_claim_id=h.uth_claim_id;

analyze dev.claim_header_optum;

select prov::int8
from dev2016.optum_zip_medical;

analyze dev.claim_detail_optum_fix;

select get_my_from_date('2011-08-18'::date);

select data_source, count(*), count(distinct uth_claim_id)
from dev.claim_detail_optum
group by 1;

select data_source, count(*), count(distinct uth_claim_id)
from dw_qa.claim_detail
group by 1;

insert into dw_qa.claim_detail(data_source, uth_claim_id, uth_member_id,
    claim_sequence_number, claim_sequence_number_src,
	from_date_of_service, to_date_of_service, month_year_id,	
	perf_provider_id, bill_provider_id, ref_provider_id, place_of_service,
	network_ind, network_paid_ind,
	admit_date,	discharge_date,
	procedure_cd, procedure_type, proc_mod_1, proc_mod_2,
	revenue_cd, charge_amount, allowed_amount, paid_amount, copay, deductible, coins, cob, cob_type,
	bill_type_inst,	bill_type_class, bill_type_freq, units,
	drg_cd)
select data_source, uth_claim_id, uth_member_id,
    claim_sequence_number, claim_sequence_number_src,
	from_date_of_service, to_date_of_service, month_year_id,	
	perf_provider_id, bill_provider_id, ref_provider_id, place_of_service,
	network_ind, network_paid_ind,
	admit_date,	discharge_date,
	procedure_cd, procedure_type, proc_mod_1, proc_mod_2,
	revenue_cd, charge_amount, allowed_amount, paid_amount, copay, deductible, coins, cob, cob_type,
	bill_type_inst,	bill_type_class, bill_type_freq, units,
	drg_cd
from dev.claim_detail_optum;


select *
from dev.claim_detail_optum
where uth_claim_id=162996771593
order by claim_sequence_number
limit 10;

select trunc(0002);

SELECT DISTINCT cob
FROM optum_zip_medical
where pos not in (select place_of_treatment_cd from reference_tables.ref_place_of_service);

select *
from reference_tables.ref_place_of_service;

SELECT DISTINCT tos_cd
FROM optum_zip_medical;


select case when a in 

