
/* ******************************************************************************************************
 *  load claim detail for medicare texas
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 * ******************************************************************************************************
 *  wcc001  || 10/01/2021 || add comment block. migrate to dw_staging load 
 * 					         logical groupings: [inpatient, hha, hospice, snf], [outpatient], [bcarrier, dme]
 * ****************************************************************************************************** 
 *  jw001   || 10/05/2021 || remove substrings to change hcpcs_1st_mdfr_cd's to 2 characters like raw table
 * 													 change from_date_of_service to rev_cntr_dt in hha, hospice, outpatient
 * ****************************************************************************************************** 
 *  jw002   || 11/03/2021 || add provider variables 
 * ***************************************************************************************************
 *  jw003   || 11/22/2021 || changed procedure type to case when based on the hcpc_cd 
 * ***************************************************************************************************
=======
 *  gmunoz  || 10/25/2021 || adding dev.fiscal_year_func() logic
 * ****************************************************************************************************** 
 * */


do $$ 

begin 

----inpatient
insert into dw_staging.claim_detail ( data_source, year, uth_member_id, uth_claim_id, claim_sequence_number, from_date_of_service, to_date_of_service,
                                      month_year_id, place_of_service, network_ind, network_paid_ind, admit_date, discharge_date, discharge_status, 
                                      cpt_hcpcs_cd, procedure_type, proc_mod_1, proc_mod_2, drg_cd, revenue_cd, charge_amount, allowed_amount, 
                                      paid_amount, copay, deductible, coins, cob, bill_type_inst, bill_type_class, bill_type_freq, 
                                      units, fiscal_year, table_id_src, claim_sequence_number_src,
                                      bill_provider, ref_provider, other_provider, perf_rn_provider, perf_at_provider, perf_op_provider
                                     )								   							   
select 'mcrt',  extract(year from a.clm_thru_dt::date), c.uth_member_id, c.uth_claim_id, a.clm_line_num::numeric, b.clm_from_dt::date, a.clm_thru_dt::date,
	    d.month_year_id, b.clm_fac_type_cd, true, true, b.clm_admsn_dt::date, b.nch_bene_dschrg_dt::date, b.ptnt_dschrg_stus_cd, 
	    a.hcpcs_cd, case when substring(hcpcs_cd,1,1) ~ '[0-9]' then 'CPT'
	   									 when substring(hcpcs_cd,1,1) ~ '[a-zA-Z]' then 'HCPCS' else null end as procedure_type,
	   	a.hcpcs_1st_mdfr_cd, a.hcpcs_2nd_mdfr_cd, b.clm_drg_cd, a.rev_cntr, a.rev_cntr_ncvrd_chrg_amt::numeric, null, 
	    null, null, null, null, null,  substring(b.clm_fac_type_cd,1,1),  substring(b.clm_srvc_clsfctn_type_cd,1,1),  substring(b.clm_freq_cd,1,1), 
	    a.rev_cntr_unit_cnt::int, 
      dev.fiscal_year_func(a.clm_thru_dt::date),
      'inpatient_revenue_center', a.clm_line_num,
	    null as bill_provider, null as ref_provider, null as other_provider, 
	    null as perf_rn_provider, null as perf_at_provider, null as perf_op_provider
from medicare_texas.inpatient_revenue_center_k a 
     join medicare_texas.inpatient_base_claims_k b
       on b.bene_id = a.bene_id 
      and b.clm_id = a.clm_id  
     join data_warehouse.dim_uth_claim_id c
        on c.member_id_src = a.bene_id 
	   and c.data_source = 'mcrt'
	   and c.claim_id_src = a.clm_id
   join reference_tables.ref_month_year d
	    on d.month_int = extract(month from b.clm_from_dt::date) 
	   and d.year_int = extract(year from b.clm_from_dt::date) 
;

raise notice 'inpatient';

--hha
insert into dw_staging.claim_detail ( data_source, year, uth_member_id, uth_claim_id, claim_sequence_number, from_date_of_service, to_date_of_service,
                                      month_year_id, place_of_service, network_ind, network_paid_ind, admit_date, discharge_date, discharge_status, 
                                      cpt_hcpcs_cd, procedure_type, 
                                      proc_mod_1, proc_mod_2, drg_cd, revenue_cd, charge_amount, allowed_amount, 
                                      paid_amount, copay, deductible, coins, cob, 
                                      bill_type_inst, bill_type_class, bill_type_freq, 
                                      units, 
                                      fiscal_year, table_id_src, claim_sequence_number_src,
                                      bill_provider, ref_provider, other_provider, perf_rn_provider, perf_at_provider, perf_op_provider
                                     )	                                                                         
select 'mcrt', extract(year from a.clm_thru_dt::date), c.uth_member_id, c.uth_claim_id, a.clm_line_num::numeric, a.rev_cntr_dt::date, a.clm_thru_dt::date,
	    d.month_year_id, b.clm_fac_type_cd, true, true, b.clm_admsn_dt::date, b.nch_bene_dschrg_dt::date, b.ptnt_dschrg_stus_cd, 
	    a.hcpcs_cd, case when substring(hcpcs_cd,1,1) ~ '[0-9]' then 'CPT'
	   									 when substring(hcpcs_cd,1,1) ~ '[a-zA-Z]' then 'HCPCS' else null end as procedure_type,
	    a.hcpcs_1st_mdfr_cd, a.hcpcs_2nd_mdfr_cd, null, a.rev_cntr, a.rev_cntr_ncvrd_chrg_amt::numeric, null, 
	    null, null, null, null, null,  
	    substring(b.clm_fac_type_cd,1,1),  substring(b.clm_srvc_clsfctn_type_cd,1,1),  substring(b.clm_freq_cd,1,1), 
	    a.rev_cntr_unit_cnt::int, dev.fiscal_year_func(a.clm_thru_dt::date), 'hha_revenue_center', a.clm_line_num,
	    null as bill_provider, null as ref_provider, null as other_provider, 
	    a.rndrng_physn_npi as perf_rn_provider, null as perf_at_provider, null as perf_op_provider
from medicare_texas.hha_revenue_center_k a 
     join medicare_texas.hha_base_claims_k b
     on a.bene_id = b.bene_id
    and a.clm_id = b.clm_id 
  join data_warehouse.dim_uth_claim_id c 
    on c.member_id_src = a.bene_id
   and c.data_source = 'mcrt' 
   and c.claim_id_src = a.clm_id
 join reference_tables.ref_month_year d 
    on d.month_int = extract(month from b.clm_from_dt::date)
   and d.year_int = extract(year from b.clm_from_dt::date)
;

raise notice 'hha';


--hospice
insert into dw_staging.claim_detail ( data_source, year, uth_member_id, uth_claim_id, claim_sequence_number, from_date_of_service, to_date_of_service,
                                      month_year_id, place_of_service, network_ind, network_paid_ind, admit_date, discharge_date, discharge_status, 
                                      cpt_hcpcs_cd, procedure_type, proc_mod_1, proc_mod_2, drg_cd, revenue_cd, charge_amount, allowed_amount, 
                                      paid_amount, copay, deductible, coins, cob, bill_type_inst, bill_type_class, bill_type_freq, 
                                      units, fiscal_year, table_id_src, claim_sequence_number_src,
                                      bill_provider, ref_provider, other_provider, perf_rn_provider, perf_at_provider, perf_op_provider
                                     )								   							   
select 'mcrt',  extract(year from a.clm_thru_dt::date), c.uth_member_id, c.uth_claim_id, a.clm_line_num::numeric, a.rev_cntr_dt::date, a.clm_thru_dt::date,
	    d.month_year_id, b.clm_fac_type_cd, true, true, null, b.nch_bene_dschrg_dt::date, b.ptnt_dschrg_stus_cd, 
	   a.hcpcs_cd, case when substring(hcpcs_cd,1,1) ~ '[0-9]' then 'CPT'
	   								 when substring(hcpcs_cd,1,1) ~ '[a-zA-Z]' then 'HCPCS' else null end as procedure_type,
	    a.hcpcs_1st_mdfr_cd, a.hcpcs_2nd_mdfr_cd, null, a.rev_cntr, a.rev_cntr_ncvrd_chrg_amt::numeric, null, 
	    null, null, null, null, null,  substring(b.clm_fac_type_cd,1,1),  substring(b.clm_srvc_clsfctn_type_cd,1,1),  substring(b.clm_freq_cd,1,1), 
	    a.rev_cntr_unit_cnt::int, 
      dev.fiscal_year_func(a.clm_thru_dt::date),
      'hospice_revenue_center', a.clm_line_num,
	    null as bill_provider, null as ref_provider, null as other_provider, 
	    a.rndrng_physn_npi as perf_rn_provider, null as perf_at_provider, null as perf_op_provider
from medicare_texas.hospice_revenue_center_k a 
     join medicare_texas.hospice_base_claims_k b
     on a.bene_id = b.bene_id
    and a.clm_id = b.clm_id 
  join data_warehouse.dim_uth_claim_id c 
    on c.member_id_src = a.bene_id
   and c.data_source = 'mcrt' 
   and c.claim_id_src = a.clm_id
 join reference_tables.ref_month_year d 
    on d.month_int = extract(month from b.clm_from_dt::date)
   and d.year_int = extract(year from b.clm_from_dt::date)
;

raise notice 'hospice';

--snf
insert into dw_staging.claim_detail ( data_source, year, uth_member_id, uth_claim_id, claim_sequence_number, from_date_of_service, to_date_of_service,
                                      month_year_id, place_of_service, network_ind, network_paid_ind, admit_date, discharge_date, discharge_status, 
                                      cpt_hcpcs_cd, procedure_type, proc_mod_1, proc_mod_2, drg_cd, revenue_cd, charge_amount, allowed_amount, 
                                      paid_amount, copay, deductible, coins, cob, bill_type_inst, bill_type_class, bill_type_freq, 
                                      units, fiscal_year, table_id_src, claim_sequence_number_src,
                                      bill_provider, ref_provider, other_provider, perf_rn_provider, perf_at_provider, perf_op_provider
                                     )								   							   
select 'mcrt',  extract(year from a.clm_thru_dt::date), c.uth_member_id, c.uth_claim_id, a.clm_line_num::numeric, b.clm_from_dt::date, a.clm_thru_dt::date,
	    d.month_year_id, b.clm_fac_type_cd, true, true, b.clm_admsn_dt::date, b.nch_bene_dschrg_dt::date, b.ptnt_dschrg_stus_cd, 
	    a.hcpcs_cd, case when substring(hcpcs_cd,1,1) ~ '[0-9]' then 'CPT'
	   									 when substring(hcpcs_cd,1,1) ~ '[a-zA-Z]' then 'HCPCS' else null end as procedure_type, 
	    a.hcpcs_1st_mdfr_cd, a.hcpcs_2nd_mdfr_cd, null, a.rev_cntr, a.rev_cntr_ncvrd_chrg_amt::numeric, null, 
	    null, null, null, null, null,  substring(b.clm_fac_type_cd,1,1),  substring(b.clm_srvc_clsfctn_type_cd,1,1),  substring(b.clm_freq_cd,1,1), 
	    a.rev_cntr_unit_cnt::int,
      dev.fiscal_year_func(a.clm_thru_dt::date),
      'snf_revenue_center', a.clm_line_num,
	    null as bill_provider, null as ref_provider, null as other_provider, 
	    null as perf_rn_provider, null as perf_at_provider, null as perf_op_provider
from medicare_texas.snf_revenue_center_k a 
     join medicare_texas.snf_base_claims_k b
     on a.bene_id = b.bene_id
    and a.clm_id = b.clm_id 
  join data_warehouse.dim_uth_claim_id c 
    on c.member_id_src = a.bene_id
   and c.data_source = 'mcrt' 
   and c.claim_id_src = a.clm_id
 join reference_tables.ref_month_year d 
    on d.month_int = extract(month from b.clm_from_dt::date)
   and d.year_int = extract(year from b.clm_from_dt::date)
;

raise notice 'snf';

---////
---outpatient
insert into dw_staging.claim_detail ( data_source, year, uth_member_id, uth_claim_id, claim_sequence_number, from_date_of_service, to_date_of_service,
                                      month_year_id, place_of_service, network_ind, network_paid_ind, admit_date, discharge_date, discharge_status, 
                                      cpt_hcpcs_cd, procedure_type, proc_mod_1, proc_mod_2, drg_cd, revenue_cd, charge_amount, allowed_amount, 
                                      paid_amount, copay, deductible, coins, cob, bill_type_inst, bill_type_class, bill_type_freq, 
                                      units, fiscal_year, table_id_src, claim_sequence_number_src,
                                      bill_provider, ref_provider, other_provider, perf_rn_provider, perf_at_provider, perf_op_provider
                                     )								   							   
select 'mcrt',  extract(year from a.clm_thru_dt::date), c.uth_member_id, c.uth_claim_id, a.clm_line_num::numeric, a.rev_cntr_dt::date, a.clm_thru_dt::date,
	    d.month_year_id, b.clm_fac_type_cd, true, true, null, null, null, 
	   a.hcpcs_cd, case when substring(hcpcs_cd,1,1) ~ '[0-9]' then 'CPT'
	   									 when substring(hcpcs_cd,1,1) ~ '[a-zA-Z]' then 'HCPCS' else null end as procedure_type,
	    a.hcpcs_1st_mdfr_cd, a.hcpcs_2nd_mdfr_cd, null, a.rev_cntr, a.rev_cntr_ncvrd_chrg_amt::numeric, null, 
	    null, null, null, null, null,  substring(b.clm_fac_type_cd,1,1),  substring(b.clm_srvc_clsfctn_type_cd,1,1),  substring(b.clm_freq_cd,1,1), 
	    a.rev_cntr_unit_cnt::int, 
      dev.fiscal_year_func(a.clm_thru_dt::date),
      'outpatient_revenue_center', a.clm_line_num,
	    null as bill_provider, null as ref_provider, null as other_provider, 
	    a.rndrng_physn_npi as perf_rn_provider, null as perf_at_provider, null as perf_op_provider
from medicare_texas.outpatient_revenue_center_k a 
     join medicare_texas.outpatient_base_claims_k b
       on b.bene_id = a.bene_id 
      and b.clm_id = a.clm_id 
     join data_warehouse.dim_uth_claim_id c
	    on c.member_id_src = a.bene_id
	   and c.data_source = 'mcrt'
	   and c.claim_id_src = a.clm_id
   join reference_tables.ref_month_year d
	    on d.month_int = extract(month from b.clm_from_dt::date) 
	   and d.year_int = extract(year from b.clm_from_dt::date) 
;


raise notice 'outpatient';

--bcarrier 
insert into dw_staging.claim_detail ( data_source, year, uth_member_id, uth_claim_id, claim_sequence_number, from_date_of_service, to_date_of_service,
                                      month_year_id, place_of_service, network_ind, network_paid_ind, admit_date, discharge_date, discharge_status, 
                                      cpt_hcpcs_cd, procedure_type, proc_mod_1, proc_mod_2, drg_cd, revenue_cd, charge_amount, allowed_amount, 
                                      paid_amount, copay, deductible, coins, cob, bill_type_inst, bill_type_class, bill_type_freq, 
                                      units, fiscal_year, table_id_src, claim_sequence_number_src,
                                      bill_provider, ref_provider, other_provider, perf_rn_provider, perf_at_provider, perf_op_provider
                                     )								   							   
select 'mcrt',  extract(year from a.clm_thru_dt::date), c.uth_member_id, c.uth_claim_id, a.line_num::numeric, b.clm_from_dt::date, a.clm_thru_dt::date,
	    d.month_year_id, a.line_place_of_srvc_cd, true, true, null, null, null, 
	    a.hcpcs_cd, case when substring(hcpcs_cd,1,1) ~ '[0-9]' then 'CPT'
	   									 when substring(hcpcs_cd,1,1) ~ '[a-zA-Z]' then 'HCPCS' else null end as procedure_type,
	    a.hcpcs_1st_mdfr_cd, a.hcpcs_2nd_mdfr_cd, null, null as reveneue_cd, a.line_alowd_chrg_amt::numeric, null, 
	    a.line_bene_pmt_amt::numeric, null, a.line_service_deductible::numeric, a.line_coinsrnc_amt::numeric, null, null, null, null, 
	    a.line_srvc_cnt::numeric, 
      dev.fiscal_year_func(a.clm_thru_dt::date),
      'bcarrier_line', a.line_num,
	    a.org_npi_num as bill_provider, null as ref_provider, null as other_provider, 
	    a.prf_physn_npi as perf_rn_provider, null as perf_at_provider, null as perf_op_provider
from medicare_texas.bcarrier_line_k a 
     join medicare_texas.bcarrier_claims_k b
       on b.bene_id = a.bene_id 
      and b.clm_id = a.clm_id 
     join data_warehouse.dim_uth_claim_id c 
        on c.member_id_src = a.bene_id 
       and c.data_source = 'mcrt'
	   and c.claim_id_src = a.clm_id
   join reference_tables.ref_month_year d
	    on d.month_int = extract(month from b.clm_from_dt::date) 
	   and d.year_int = extract(year from b.clm_from_dt::date) 
;


raise notice 'bcarrier';

--dme
insert into dw_staging.claim_detail ( data_source, year, uth_member_id, uth_claim_id, claim_sequence_number, from_date_of_service, to_date_of_service,
                                      month_year_id, place_of_service, network_ind, network_paid_ind, admit_date, discharge_date, discharge_status, 
                                      cpt_hcpcs_cd, procedure_type, proc_mod_1, proc_mod_2, drg_cd, revenue_cd, charge_amount, allowed_amount, 
                                      paid_amount, copay, deductible, coins, cob, bill_type_inst, bill_type_class, bill_type_freq, 
                                      units, fiscal_year, table_id_src, claim_sequence_number_src,
                                      bill_provider, ref_provider, other_provider, perf_rn_provider, perf_at_provider, perf_op_provider
                                     )								   							   
select 'mcrt',  extract(year from a.clm_thru_dt::date), c.uth_member_id, c.uth_claim_id, a.line_num::numeric, b.clm_from_dt::date, a.clm_thru_dt::date,
	    d.month_year_id, a.line_place_of_srvc_cd, true, true, null, null, null, 
	    a.hcpcs_cd, case when substring(hcpcs_cd,1,1) ~ '[0-9]' then 'CPT'
	   									 when substring(hcpcs_cd,1,1) ~ '[a-zA-Z]' then 'HCPCS' else null end as procedure_type, 
	    a.hcpcs_1st_mdfr_cd, a.hcpcs_2nd_mdfr_cd, null, null as reveneue_cd, a.line_alowd_chrg_amt::numeric, null, 
	    a.line_bene_pmt_amt::numeric, null, a.line_service_deductible::numeric, a.line_coinsrnc_amt::numeric, null, null, null, null, 
	    a.line_srvc_cnt::numeric,
      dev.fiscal_year_func(a.clm_thru_dt::date),
      'dme_line', a.line_num,
	    a.prvdr_npi as bill_provider, null as ref_provider, null as other_provider, 
	    null as perf_rn_provider, null as perf_at_provider, null as perf_op_provider
from medicare_texas.dme_line_k a
  join medicare_texas.dme_claims_k b
     on a.clm_id = b.clm_id 
    and a.bene_id = b.bene_id
  join data_warehouse.dim_uth_claim_id c 
    on  c.member_id_src = a.bene_id
   and c.data_source = 'mcrt' 
   and c.claim_id_src = a.clm_id
 join reference_tables.ref_month_year d 
    on d.month_int = extract(month from b.clm_from_dt::date)
   and d.year_int = extract(year from b.clm_from_dt::date)
;

raise notice 'dme';

---finalize 
analyze dw_staging.claim_detail;

raise notice 'done';

end $$
;

