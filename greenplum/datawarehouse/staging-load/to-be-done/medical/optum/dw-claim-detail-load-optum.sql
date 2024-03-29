
/* ******************************************************************************************************
 *  load claim detail for optum zip and optum dod
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 * ******************************************************************************************************
 *  wcc001  || 9/20/2021 || add comment block. migrate to dw_staging load
 * ******************************************************************************************************
 *  gmunoz  || 10/25/2021 || adding dev.fiscal_year_func() logic
 * ******************************************************************************************************
  * jwozny  || 11/05/2021 || added provider variables - note: need to add columns
 * ******************************************************************************************************
 *  jwozny  || 11/19/2021 || changed units to alt_units and removed substring on procmod
 * ***********************************************************************************
 *  jwozny  || 12/17/2021 || added  logic for procedure type
 * *************************************************************************************
 *  jwozny  || 01/04/2022 || added logic to get rid of 'NA' discharge values
 * 												  	added logic to fill in null last date of service values
 * *************************************************************************************
 * */


--------------***** Optum DoD *****

---uth claims for optd only distributed by member id
drop table if exists dw_staging.optd_uth_claim_id;

create table dw_staging.optd_uth_claim_id
with(appendonly=true,orientation=column)
as select * from data_warehouse.dim_uth_claim_id where data_source = 'optd'
distributed by (member_id_src);

analyze dw_staging.optd_uth_claim_id;


--load OPTD to staging claim_detail table 43mins  12/14/21
insert into dw_staging.claim_detail (
		data_source, year, uth_member_id, uth_claim_id, claim_sequence_number,
       from_date_of_service, to_date_of_service, month_year_id, place_of_service,
       network_ind, network_paid_ind,
       admit_date, discharge_date, discharge_status,
       cpt_hcpcs_cd, procedure_type, proc_mod_1, proc_mod_2, drg_cd,
       revenue_cd, charge_amount, allowed_amount, paid_amount,
       copay, deductible, coins, cob,
       bill_type_inst, bill_type_class, bill_type_freq,
       units, fiscal_year, cost_factor_year, table_id_src, claim_sequence_number_src,
       bill_provider, ref_provider, other_provider, perf_rn_provider, perf_at_provider, perf_op_provider
)
select 'optd', extract(year from a.fst_dt) as year, b.uth_member_id, b.uth_claim_id, null,
       a.fst_dt, 
        case --- lst_dt: fill in missing lst_dt for claims where lst_dt should be same as lst_dt
					when a.lst_dt is null and a.tos_ext like 'PROF%' or a.tos_ext like 'ANC%' or a.tos_ext like 'FAC_OP%'
						then a.fst_dt 
					else a.lst_dt
					end as lst_dt, 
       get_my_from_date(a.fst_dt) as month_year, a.pos,
       case when prov_par in ('C','P','T') then true else false end as network_ind, 
       case when prov_par in ('C','P','T') then true else false end as network_paid_ind,
       d.admit_date, d.disch_date,
			 case 
		 		when d.dstatus = 'NA'
		 			then null
		 		else lpad(trim(d.dstatus), 2, '0')
		 		end as discharge_st,
		 	a.proc_cd,
		 	case
		 		when substring(proc_cd, 1, 1) ~ '[0-9]'
		 			then 'CPT'
		 		when substring(proc_cd, 1, 1) ~ '[a-zA-Z]'
		 			then 'HCPCS'
		 		else null
		 		end as procedure_type,
	   		substring(a.procmod,1,2) as proc_mod_1, null as proc_mod_2, a.drg,
       a.rvnu_cd, (a.charge * c.cost_factor) as charge_amount, (a.std_cost * c.cost_factor) as allowed_amount, null as paid_amount,
       a.copay, a.deduct, a.coins, null,
       substring(a.bill_type,1,1), substring(a.bill_type,2,1), substring(a.bill_type,3,1),
       a.alt_units::int,
       dev.fiscal_year_func(a.fst_dt),
       c.standard_price_year, 'medical', a.clmseq,
       a.bill_prov as bill_provider, a.refer_prov as ref_provider,
       a.service_prov as other_provider, a.prov as perf_rn_provider, null as perf_at_provider, null as perf_op_provider
from optum_dod.medical a
	join dw_staging.optd_uth_claim_id b
	   on b.member_id_src = a.member_id_src
	  and b.claim_id_src = a.clmid
	join reference_tables.ref_optum_cost_factor c
	   on c.service_type = left(a.tos_cd, (position('.' in a.tos_cd)-1))
	  and c.standard_price_year = a.std_cost_yr::int
	left outer join optum_dod.confinement d
	  on d.member_id_src = a.member_id_src
	 and d.conf_id = a.conf_id
;



-------------------------

---**********************************************************************************************************
-------------------- optz -------------------------------------
---**********************************************************************************************************



---uth claims for optd only
drop table if exists dw_staging.optz_uth_claim_id;

create table dw_staging.optz_uth_claim_id
with(appendonly=true,orientation=column)
as select * from data_warehouse.dim_uth_claim_id where data_source = 'optz'
distributed by (member_id_src);

analyze dw_staging.optz_uth_claim_id;



---------------optz insert
insert into dw_staging.claim_detail (
		data_source, year, uth_member_id, uth_claim_id, claim_sequence_number,
       from_date_of_service, to_date_of_service, month_year_id, place_of_service,
       network_ind, 
       network_paid_ind,
       admit_date, discharge_date, discharge_status,
       cpt_hcpcs_cd, procedure_type, proc_mod_1, proc_mod_2, drg_cd,
       revenue_cd, charge_amount, allowed_amount, paid_amount,
       copay, deductible, coins, cob,
       bill_type_inst, bill_type_class, bill_type_freq,
       units, fiscal_year, cost_factor_year, table_id_src, claim_sequence_number_src,
       bill_provider, ref_provider, other_provider, perf_rn_provider, perf_at_provider, perf_op_provider
)
select 'optz', extract(year from a.fst_dt) as year, b.uth_member_id, b.uth_claim_id, null,
       a.fst_dt, 
       case --- lst_dt: fill in missing lst_dt for claims where lst_dt should be same as lst_dt
					when a.lst_dt is null and a.tos_ext like 'PROF%' or a.tos_ext like 'ANC%' or a.tos_ext like 'FAC_OP%'
						then a.fst_dt 
					else a.lst_dt
				end as lst_dt, 
       get_my_from_date(a.fst_dt) as month_year, a.pos,
       case when prov_par in ('C','P','T') then true else false end as network_ind, 
       case when prov_par in ('C','P','T') then true else false end as network_paid_ind,
       d.admit_date, d.disch_date,
			 case
			 		when d.dstatus = 'NA'
			 			then null
			 		else lpad(trim(d.dstatus), 2, '0')
		 		end as discharge_st,
			 	a.proc_cd,
			 	case
			 		when substring(proc_cd, 1, 1) ~ '[0-9]'
			 			then 'CPT'
			 		when substring(proc_cd, 1, 1) ~ '[a-zA-Z]'
			 			then 'HCPCS'
			 		else null
		 		end as procedure_type,
	   	 substring(a.procmod,1,2) as proc_mod_1, null as proc_mod_2, a.drg,
       a.rvnu_cd, (a.charge * c.cost_factor) as charge_amount, (a.std_cost * c.cost_factor) as allowed_amount, null as paid_amount,
       a.copay, a.deduct, a.coins, null,
       substring(a.bill_type,1,1), substring(a.bill_type,2,1), substring(a.bill_type,3,1),
       a.alt_units::int4, dev.fiscal_year_func(a.fst_dt), c.standard_price_year, 'medical', a.clmseq,
       a.bill_prov as bill_provider, a.refer_prov as ref_provider,
       a.service_prov as other_provider, a.prov as perf_rn_provider, null as perf_at_provider, null as perf_op_provider
from optum_zip.medical a
	join dw_staging.optz_uth_claim_id b
	   on b.member_id_src = a.member_id_src
	  and b.claim_id_src = a.clmid
	join reference_tables.ref_optum_cost_factor c
		on c.service_type = left(a.tos_cd, (position('.' in a.tos_cd)-1))
	   and c.standard_price_year = a.std_cost_yr::int
	left outer join optum_zip.confinement d
	  on d.member_id_src = a.member_id_src
	 and d.conf_id = a.conf_id
;






--va
analyze dw_staging.claim_detail;


--------------- END SCRIPT -------

select count(*), data_source, year
from dw_staging.claim_detail cd
where data_source like 'opt%'
group by 2,3 order by 2,3;


select count(*), year 
from optum_dod.medical 
group by 2 order by 2 
;

select count(*), year 
from optum_zip.medical 
group by 2 order by 2
;


select count(*), data_year 
from dw_staging.optd_uth_claim_id
group by 2 order by 2
;