
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
 * */


--------------- BEGIN SCRIPT -------

---create copy of data warehouse table in dw_staging 
drop table if exists dw_staging.claim_detail;

create table dw_staging.claim_detail 
with (appendonly=true, orientation=column) as 
select data_source, year, uth_member_id, uth_claim_id, claim_sequence_number, 
       from_date_of_service, to_date_of_service, month_year_id, place_of_service, 
       network_ind, network_paid_ind, 
       admit_date, discharge_date, discharge_status,
       cpt_hcpcs as cpt_hcpcs_cd, procedure_type, proc_mod_1, proc_mod_2, drg_cd, 
       revenue_cd, charge_amount, allowed_amount, paid_amount, 
       copay, deductible, coins, cob, 
       bill_type_inst, bill_type_class, bill_type_freq, 
       units, fiscal_year, cost_factor_year, table_id_src, claim_sequence_number_src 
from data_warehouse.claim_detail 
where data_source not in ('optd','optz')
distributed by (uth_member_id) 
;

vacuum analyze dw_staging.claim_detail;


--------------***** Optum DoD ***** 

---uth claims for optd only distributed by member id 
drop table if exists dw_staging.optd_uth_claim_id;

create table dw_staging.optd_uth_claim_id
with(appendonly=true,orientation=column)
as select * from data_warehouse.dim_uth_claim_id where data_source = 'optd'
distributed by (member_id_src);

vacuum analyze dw_staging.optd_uth_claim_id;



--load OPTD to staging claim_detail table
insert into dw_staging.claim_detail (
		data_source, year, uth_member_id, uth_claim_id, claim_sequence_number, 
       from_date_of_service, to_date_of_service, month_year_id, place_of_service, 
       network_ind, network_paid_ind, 
       admit_date, discharge_date, discharge_status,
       cpt_hcpcs_cd, procedure_type, proc_mod_1, proc_mod_2, drg_cd, 
       revenue_cd, charge_amount, allowed_amount, paid_amount, 
       copay, deductible, coins, cob, 
       bill_type_inst, bill_type_class, bill_type_freq, 
       units, fiscal_year, cost_factor_year, table_id_src, claim_sequence_number_src 
)
select 'optd', extract(year from a.fst_dt) as year, b.uth_member_id, b.uth_claim_id, null, 
       a.fst_dt, a.lst_dt, get_my_from_date(a.fst_dt) as month_year, a.pos, 
       null, null, 
       d.admit_date, d.disch_date, lpad(trim(d.dstatus),2,'0'), 
       a.proc_cd,null, substring(a.procmod, 1,1), substring(a.procmod, 2,1), a.drg, 
       a.rvnu_cd, (a.charge * c.cost_factor) as charge_amount, (a.std_cost * c.cost_factor) as allowed_amount, null as paid_amount,
       a.copay, a.deduct, a.coins, null, 
       substring(a.bill_type,1,1), substring(a.bill_type,2,1), substring(a.bill_type,3,1), 
       a.units, 
       dev.fiscal_year_func(a.fst_dt), 
       c.standard_price_year, 'medical', a.clmseq
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

vacuum analyze dw_staging.optz_uth_claim_id;



---------------optz insert 
insert into dw_staging.claim_detail (
		data_source, year, uth_member_id, uth_claim_id, claim_sequence_number, 
       from_date_of_service, to_date_of_service, month_year_id, place_of_service, 
       network_ind, network_paid_ind, 
       admit_date, discharge_date, discharge_status,
       cpt_hcpcs_cd, procedure_type, proc_mod_1, proc_mod_2, drg_cd, 
       revenue_cd, charge_amount, allowed_amount, paid_amount, 
       copay, deductible, coins, cob, 
       bill_type_inst, bill_type_class, bill_type_freq, 
       units, fiscal_year, cost_factor_year, table_id_src, claim_sequence_number_src 
)
select 'optz', extract(year from a.fst_dt) as year, b.uth_member_id, b.uth_claim_id, null, 
       a.fst_dt, a.lst_dt, get_my_from_date(a.fst_dt) as month_year, a.pos, 
       null, null, 
       d.admit_date, d.disch_date, lpad(trim(d.dstatus),2,'0'), 
       a.proc_cd,null, substring(a.procmod, 1,1), substring(a.procmod, 2,1), a.drg, 
       a.rvnu_cd, (a.charge * c.cost_factor) as charge_amount, (a.std_cost * c.cost_factor) as allowed_amount, null as paid_amount,
       a.copay, a.deduct, a.coins, null, 
       substring(a.bill_type,1,1), substring(a.bill_type,2,1), substring(a.bill_type,3,1), 
       a.units, dev.fiscal_year_func(a.fst_dt), c.standard_price_year, 'medical', a.clmseq     
from optum_zip.medical a 
	join dw_staging.optz_uth_claim_id b
	   on b.member_id_src = a.member_id_src
	  and b.claim_id_src = a.clmid
	join reference_tables.ref_optum_cost_factor c
		on c.service_type = left(a.tos_cd, (position('.' in a.tos_cd)-1)) 
	   and c.standard_price_year = a.std_cost_yr::int
	left outer join reference_tables.ref_optum_bill_type_from_tos e
		on e.tos = a.tos_cd
	left outer join optum_zip.confinement d 
	  on d.member_id_src = a.member_id_src  
	 and d.conf_id = a.conf_id 
;

--va 
vacuum analyze dw_staging.claim_detail;


---cleanup
drop table if exists dw_staging.optd_uth_claim_id;  drop table if exists dw_staging.optz_uth_claim_id;

--------------- END SCRIPT -------