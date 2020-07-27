/*
 * 
 */
drop table if exists dev.claim_detail_by_claim;
CREATE TABLE dev.claim_detail_by_claim (
	data_source bpchar(4) NULL,
	year int2,
	uth_claim_id numeric NULL,
	claim_sequence_number int4 NULL,
	uth_member_id int8 NULL,
	from_date_of_service date NULL,
	to_date_of_service date NULL,
	month_year_id int4 NULL,
	perf_provider_id text NULL,
	bill_provider_id text NULL,
	ref_provider_id text NULL,
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
	bill_type_inst bpchar(1) NULL,
	bill_type_class bpchar(1) NULL,
	bill_type_freq bpchar(1) NULL,
	units int4 NULL,
	drg_cd text NULL,
	claim_id_src text NULL,
	member_id_src text NULL,
	table_id_src text NULL,
	claim_sequence_number_src text NULL,
	cob_type text NULL
)
WITH (
	appendonly=true, orientation=column
)
DISTRIBUTED BY (uth_member_id);

/*
 * Remove old records
 */


/*
 * This script assumes claim_header has already been loaded with mapped uth_*_ids
 */
vacuum analyze data_warehouse.claim_header;

vacuum analyze optum_dod.medical;

delete from data_warehouse.claim_detail where data_source in ('optz','optd')

--Optum load: 23 min for 2016
explain

insert into data_warehouse.claim_detail(
	data_source, year, uth_claim_id, uth_member_id,
    claim_sequence_number, claim_sequence_number_src,
	from_date_of_service, to_date_of_service, month_year_id,	
	perf_provider_id, bill_provider_id, ref_provider_id, place_of_service,
	network_ind, network_paid_ind,
	admit_date,	discharge_date,
	procedure_cd, procedure_type, proc_mod_1, proc_mod_2,
	revenue_cd, charge_amount, allowed_amount, paid_amount, copay, deductible, coins, cob, cob_type,
	bill_type_inst,	bill_type_class, bill_type_freq, units,
	drg_cd,
	claim_id_src, member_id_src, table_id_src)
select ch.data_source, ch.year, ch.uth_claim_id, ch.uth_member_id,
	trunc(m.clmseq::int4), m.clmseq,
	m.fst_dt, m.lst_dt, get_my_from_date(m.fst_dt),
	m.prov::text, m.bill_prov::text, m.refer_prov::text, m.pos,
	null, null, --No mappings for network fields
	conf.admit_date, conf.disch_date,
	m.proc_cd, null, substring(m.procmod, 1,1), substring(m.procmod, 2,1),
	m.rvnu_cd, null, m.std_cost, null, m.copay, null, m.coins, null, m.cob, --NOTE: cob is an int, but optum is varchar -> m.cob (Find where it is a numeric value, set other to zero), 	--NOTE: Left pad revenu_cd to 4 digits with leading zero
	bt.inst_code, bt.class_code, null, m.units, --NOTE: bill_type_freq is null for optum
	m.drg,
	uth.claim_id_src, uth.member_id_src, 'medical'
from data_warehouse.claim_header ch
	join data_warehouse.dim_uth_claim_id uth 
		on ch.uth_claim_id=uth.uth_claim_id
	join optum_dod.medical m 
		on ch.claim_id_src=m.clmid::text 
	   and ch.member_id_src=m.patid::text
	left outer join optum_dod.confinement conf 
		on m.conf_id=conf.conf_id
	left outer join reference_tables.ref_optum_bill_type_from_tos bt 
		on m.tos_cd=bt.tos
where ch.data_source='optd'
;


select count(*), count(distinct conf_id) 
from optum_dod.confinement 

select * from reference_tables.ref_optum_bill_type_from_tos robtft 





---optz
insert into data_warehouse.claim_detail(
	data_source, year, uth_claim_id, uth_member_id,
    claim_sequence_number, claim_sequence_number_src,
	from_date_of_service, to_date_of_service, month_year_id,	
	perf_provider_id, bill_provider_id, ref_provider_id, place_of_service,
	network_ind, network_paid_ind,
	admit_date,	discharge_date,
	procedure_cd, procedure_type, proc_mod_1, proc_mod_2,
	revenue_cd, charge_amount, allowed_amount, paid_amount, copay, deductible, coins, cob, cob_type,
	bill_type_inst,	bill_type_class, bill_type_freq, units,
	drg_cd,
	claim_id_src, member_id_src, table_id_src)
select ch.data_source, ch.year, ch.uth_claim_id, ch.uth_member_id,
	trunc(m.clmseq::int4), m.clmseq,
	m.fst_dt, m.lst_dt, get_my_from_date(m.fst_dt),
	m.prov::text, m.bill_prov::text, m.refer_prov::text, m.pos,
	null, null, --No mappings for network fields
	conf.admit_date, conf.disch_date,
	m.proc_cd, null, substring(m.procmod, 1,1), substring(m.procmod, 2,1),
	m.rvnu_cd, null, m.std_cost, null, m.copay, null, m.coins, null, m.cob, --NOTE: cob is an int, but optum is varchar -> m.cob (Find where it is a numeric value, set other to zero), 	--NOTE: Left pad revenu_cd to 4 digits with leading zero
	bt.inst_code, bt.class_code, null, m.units, --NOTE: bill_type_freq is null for optum
	m.drg,
	uth.claim_id_src, uth.member_id_src, 'medical'
from data_warehouse.claim_header ch
	join data_warehouse.dim_uth_claim_id uth 
		on ch.uth_claim_id = uth.uth_claim_id
	join optum_zip.medical m 
		on ch.claim_id_src=m.clmid::text 
	   and ch.member_id_src=m.patid::text
	left outer join optum_zip.confinement conf 
		on m.conf_id=conf.conf_id
	left outer join reference_tables.ref_optum_bill_type_from_tos bt 
		on m.tos_cd=bt.tos
where ch.data_source='optz'
;


select count(*), year, data_source
from data_warehouse.claim_detail cd 
group by year, data_source
order by year, data_source

/*
 * Scratch Space
 */

analyze dev.claim_header_optum;

select prov::int8
from dev2016.optum_zip_medical;

analyze dw_qa.claim_detail;

select get_my_from_date('2011-08-18'::date);


select data_source, count(*)
from data_warehouse.claim_detail
group by 1;

insert into data_warehouse.claim_detail(data_source, year, uth_claim_id, uth_member_id,
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

