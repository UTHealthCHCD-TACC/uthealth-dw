
--Optum dod load
drop table dev.wc_claim_detail_optd;

select * from dev.wc_claim_detail_optd

create table dev.wc_claim_detail_optd
with(appendonly=true,orientation=column)
as select * from data_warehouse.claim_detail limit 0
distributed by (member_id_src);

---medical for optd 2007 to 2020

drop table dev.wc_optd_medical;

create table dev.wc_optd_medical
with(appendonly=true,orientation=column)
as select patid::text as mem_id_src, * from optum_dod.medical
distributed by (mem_id_src);

vacuum analyze dev.wc_optd_medical;

---uth claims for optd only
drop table if exists dev.wc_optd_uth_claim;

create table dev.wc_optd_uth_claim
with(appendonly=true,orientation=column)
as select * from data_warehouse.dim_uth_claim_id where data_source = 'optd'
distributed by (member_id_src);


select count(*) , year 
from dev.wc_claim_detail_optd
group by year 
order by year 


vacuum analyze dev.wc_optd_uth_claim;

vacuum analyze dev.wc_optd_medical;

vacuum analyze dev.wc_claim_detail_optd;



--load to dev table
insert into dev.wc_claim_detail_optd (
	data_source, year, 
	year_adj,
	uth_claim_id, uth_member_id,
    claim_sequence_number, claim_sequence_number_src,
	from_date_of_service, to_date_of_service, month_year_id,	
	perf_provider_id, bill_provider_id, ref_provider_id, place_of_service,
	network_ind, network_paid_ind,
	admit_date,	discharge_date,
	cpt_hcpcs, procedure_type, proc_mod_1, proc_mod_2,
	revenue_cd, charge_amount, allowed_amount, paid_amount, 
	charge_amount_adj, allowed_amount_adj, paid_amount_adj,
	copay, deductible, coins, cob, cob_type,
	bill_type_inst,	bill_type_class, bill_type_freq, units,
	drg_cd,
	claim_id_src, member_id_src, table_id_src, fiscal_year, discharge_status)	
select uth.data_source, uth.data_year, 
	m.std_cost_yr::int,
	uth.uth_claim_id, uth.uth_member_id,
	trunc(m.clmseq::int4), m.clmseq,
	m.fst_dt, m.lst_dt, get_my_from_date(m.fst_dt),
	m.prov::text, m.bill_prov::text, m.refer_prov::text, m.pos,
	case when prov_par in ('C','P','T') then True else False end as net_ind, case when prov_par in ('C','P','T') then True else False end as net_ind_pd, 
	conf.admit_date, conf.disch_date,
	m.proc_cd, null, substring(m.procmod, 1,1), substring(m.procmod, 2,1),
	m.rvnu_cd, 
	m.charge, m.std_cost, null, 
	(m.charge * cf.cost_factor), (m.std_cost * cf.cost_factor), null,
	m.copay, null, m.coins, null, m.cob, --NOTE: cob is an int, but optum is varchar -> m.cob (Find where it is a numeric value, set other to zero), 	--NOTE: Left pad revenu_cd to 4 digits with leading zero
	bt.inst_code, bt.class_code, null, m.units, --NOTE: bill_type_freq is null for optum
	m.drg,
	uth.claim_id_src, uth.member_id_src, 'medical', m.year, m.dstatus 
from dev.wc_optd_medical m 
	join dev.wc_optd_uth_claim uth 
	   on uth.member_id_src=m.patid::text
	  and uth.claim_id_src=m.clmid::text 
	join reference_tables.ref_optum_cost_factor cf 
	   on cf.service_type = left(m.tos_cd, (position('.' in m.tos_cd)-1)) 
	  and cf.standard_price_year = m.std_cost_yr::int
	left outer join optum_dod.confinement conf 
	  on m.conf_id=conf.conf_id
	left outer join reference_tables.ref_optum_bill_type_from_tos bt 
	  on m.tos_cd=bt.tos
;


---run after load below
vacuum analyze dev.wc_claim_detail_optd;

--verify
select count(*) , year 
from dev.wc_claim_detail_optd
group by year 
order by year 


--delete from claim detail
delete from data_warehouse.claim_detail where data_source = 'optd';

--load new records
insert into data_warehouse.claim_detail 
select * from dev.wc_claim_detail_optd 
;

--final verify dw
select count(*), year 
from data_warehouse.claim_detail cd 
where data_source = 'optd'
group by year 
order by year;

--final verify original data
select count(*), year  
from optum_dod.medical m 
group by year 
order by year 

--final cleanup
drop table dev.wc_optd_medical;  drop table dev.wc_claim_detail_optd; drop table dev.wc_optd_uth_claim;


---**********************************************************************************************************
-------------------- optz -------------------------------------
---**********************************************************************************************************

create table dev.wc_claim_detail_optz
with(appendonly=true,orientation=column)
as select * from data_warehouse.claim_detail limit 0
distributed by (member_id_src);

---medical for optd 2007 to 2020

drop table dev.wc_optz_medical;

create table dev.wc_optz_medical
with(appendonly=true,orientation=column)
as select patid::text as mem_id_src, * from optum_zip.medical
distributed by (mem_id_src);

vacuum analyze dev.wc_optz_medical;

---uth claims for optd only
drop table if exists dev.wc_optz_uth_claim;

create table dev.wc_optz_uth_claim
with(appendonly=true,orientation=column)
as select * from data_warehouse.dim_uth_claim_id where data_source = 'optz'
distributed by (member_id_src);


vacuum analyze dev.wc_optz_uth_claim;


insert into dev.wc_claim_detail_optz(
	data_source, year, 	year_adj,
	uth_claim_id, uth_member_id,
    claim_sequence_number, claim_sequence_number_src,
	from_date_of_service, to_date_of_service, month_year_id,	
	perf_provider_id, bill_provider_id, ref_provider_id, place_of_service,
	network_ind, network_paid_ind,
	admit_date,	discharge_date,
	cpt_hcpcs, procedure_type, proc_mod_1, proc_mod_2,
	revenue_cd, charge_amount, allowed_amount, paid_amount, 
	charge_amount_adj, allowed_amount_adj, paid_amount_adj,
	copay, deductible, coins, cob, cob_type,
	bill_type_inst,	bill_type_class, bill_type_freq, units,
	drg_cd,
	claim_id_src, member_id_src, table_id_src, fiscal_year, discharge_status)
select uth.data_source, uth.data_year, 	m.std_cost_yr::int,
	uth.uth_claim_id, uth.uth_member_id,
	trunc(m.clmseq::int4), m.clmseq,
	m.fst_dt, m.lst_dt, get_my_from_date(m.fst_dt),
	m.prov::text, m.bill_prov::text, m.refer_prov::text, m.pos,
	case when prov_par in ('C','P','T') then True else False end as net_ind, case when prov_par in ('C','P','T') then True else False end as net_ind_pd, 
	conf.admit_date, conf.disch_date,
	m.proc_cd, null, substring(m.procmod, 1,1), substring(m.procmod, 2,1),
	m.rvnu_cd, m.charge, m.std_cost, null, 
	(m.charge * cf.cost_factor), (m.std_cost * cf.cost_factor), null,
	m.copay, null, m.coins, null, m.cob, 
	bt.inst_code, bt.class_code, null, m.units, --NOTE: bill_type_freq is null for optum
	m.drg,
	uth.claim_id_src, uth.member_id_src, 'medical', m.year, m.dstatus
from dev.wc_optz_medical m 
	join dev.wc_optz_uth_claim uth 
	   on uth.member_id_src=m.patid::text
	  and uth.claim_id_src=m.clmid::text 
	join reference_tables.ref_optum_cost_factor cf 
		on cf.service_type = left(m.tos_cd, (position('.' in m.tos_cd)-1)) 
	   and cf.standard_price_year = m.std_cost_yr::int
	left outer join optum_zip.confinement conf 
		on m.conf_id=conf.conf_id
	left outer join reference_tables.ref_optum_bill_type_from_tos bt 
		on m.tos_cd=bt.tos
;

vacuum analyze dev.wc_claim_detail_optz ;


--validate before load
select count(*), year 
from dev.wc_claim_detail_optz 
group by year 
order by year ;

--validate counts of raw data vs load above
select count(*), year 
from optum_zip.medical 
group by year 
order by year;

---delete from dw 
delete from data_warehouse.claim_detail where data_source = 'optz';


--load new data
insert into data_warehouse.claim_detail 
select * from dev.wc_claim_detail_optz
;


--verify dw 
select year, count(*)
from data_warehouse.claim_detail
where data_source = 'optz'
group by year
order by year;



analyze data_warehouse.claim_detail;


