
/* ******************************************************************************************************
 *  load claim detail for medicare texas
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 * ******************************************************************************************************
 *  wcc001  || 10/01/2021 || add comment block. migrate to dw_staging load 
 * 					         logical groupings: [inpatient, hha, hospics, snf], [outpatient], [bcarrier, dme]
 * ****************************************************************************************************** 
 * */


--------------- BEGIN SCRIPT -------

/*
 * run this step in medicare national first 
 * 
---create copy of data warehouse table in dw_staging 
drop table if exists dw_staging.claim_detail;

create table dw_staging.claim_detail 
with (appendonly=true, orientation=column, compresstype=zlib, compresslevel=5) as 
select *
from data_warehouse.claim_detail 
where data_source not in ('mcrt','mcrt')
distributed by (uth_member_id) 
;

vacuum analyze dw_staging.claim_detail;
*/

----inpatient
insert into dw_staging.claim_detail ( data_source, year, uth_member_id, uth_claim_id, claim_sequence_number, from_date_of_service, to_date_of_service,
                                      month_year_id, place_of_service, network_ind, network_paid_ind, admit_date, discharge_date, discharge_status, 
                                      cpt_hcpcs_cd, procedure_type, proc_mod_1, proc_mod_2, drg_cd, revenue_cd, charge_amount, allowed_amount, 
                                      paid_amount, copay, deductible, coins, cob, bill_type_inst, bill_type_class, bill_type_freq, 
                                      units, fiscal_year, table_id_src, claim_sequence_number_src 
                                     )								   							   
select 'mcrt',  extract(year from a.clm_thru_dt::date), c.uth_member_id, c.uth_claim_id, a.clm_line_num::numeric, b.clm_from_dt::date, a.clm_thru_dt::date,
	    d.month_year_id, b.clm_fac_type_cd, true, true, b.clm_admsn_dt::date, b.nch_bene_dschrg_dt::date, b.ptnt_dschrg_stus_cd, 
	    a.hcpcs_cd, a.nch_clm_type_cd,  substring(a.hcpcs_1st_mdfr_cd,1,1),  substring(a.hcpcs_2nd_mdfr_cd,1,1), b.clm_drg_cd, a.rev_cntr, a.rev_cntr_ncvrd_chrg_amt::numeric, null, 
	    null, null, null, null, null,  substring(b.clm_fac_type_cd,1,1),  substring(b.clm_srvc_clsfctn_type_cd,1,1),  substring(b.clm_freq_cd,1,1), 
	    a.rev_cntr_unit_cnt::int, b.year::int2, 'inpatient_revenue_center', a.clm_line_num 
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


--hha
insert into dw_staging.claim_detail ( data_source, year, uth_member_id, uth_claim_id, claim_sequence_number, from_date_of_service, to_date_of_service,
                                      month_year_id, place_of_service, network_ind, network_paid_ind, admit_date, discharge_date, discharge_status, 
                                      cpt_hcpcs_cd, procedure_type, proc_mod_1, proc_mod_2, drg_cd, revenue_cd, charge_amount, allowed_amount, 
                                      paid_amount, copay, deductible, coins, cob, bill_type_inst, bill_type_class, bill_type_freq, 
                                      units, fiscal_year, table_id_src, claim_sequence_number_src 
                                     )								   							   
select 'mcrt', extract(year from a.clm_thru_dt::date), c.uth_member_id, c.uth_claim_id, a.clm_line_num::numeric, b.clm_from_dt::date, a.clm_thru_dt::date,
	    d.month_year_id, b.clm_fac_type_cd, true, true, b.clm_admsn_dt::date, b.nch_bene_dschrg_dt::date, b.ptnt_dschrg_stus_cd, 
	    a.hcpcs_cd, a.nch_clm_type_cd,  substring(a.hcpcs_1st_mdfr_cd,1,1),  substring(a.hcpcs_2nd_mdfr_cd,1,1), null, a.rev_cntr, a.rev_cntr_ncvrd_chrg_amt::numeric, null, 
	    null, null, null, null, null,  substring(b.clm_fac_type_cd,1,1),  substring(b.clm_srvc_clsfctn_type_cd,1,1),  substring(b.clm_freq_cd,1,1), 
	    a.rev_cntr_unit_cnt::int, b.year::int2, 'hha_revenue_center', a.clm_line_num 
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


--hospice
insert into dw_staging.claim_detail ( data_source, year, uth_member_id, uth_claim_id, claim_sequence_number, from_date_of_service, to_date_of_service,
                                      month_year_id, place_of_service, network_ind, network_paid_ind, admit_date, discharge_date, discharge_status, 
                                      cpt_hcpcs_cd, procedure_type, proc_mod_1, proc_mod_2, drg_cd, revenue_cd, charge_amount, allowed_amount, 
                                      paid_amount, copay, deductible, coins, cob, bill_type_inst, bill_type_class, bill_type_freq, 
                                      units, fiscal_year, table_id_src, claim_sequence_number_src 
                                     )								   							   
select 'mcrt',  extract(year from a.clm_thru_dt::date), c.uth_member_id, c.uth_claim_id, a.clm_line_num::numeric, b.clm_from_dt::date, a.clm_thru_dt::date,
	    d.month_year_id, b.clm_fac_type_cd, true, true, null, b.nch_bene_dschrg_dt::date, b.ptnt_dschrg_stus_cd, 
	    a.hcpcs_cd, a.nch_clm_type_cd,  substring(a.hcpcs_1st_mdfr_cd,1,1),  substring(a.hcpcs_2nd_mdfr_cd,1,1), null, a.rev_cntr, a.rev_cntr_ncvrd_chrg_amt::numeric, null, 
	    null, null, null, null, null,  substring(b.clm_fac_type_cd,1,1),  substring(b.clm_srvc_clsfctn_type_cd,1,1),  substring(b.clm_freq_cd,1,1), 
	    a.rev_cntr_unit_cnt::int, b.year::int2, 'hospice_revenue_center', a.clm_line_num 
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

--snf
insert into dw_staging.claim_detail ( data_source, year, uth_member_id, uth_claim_id, claim_sequence_number, from_date_of_service, to_date_of_service,
                                      month_year_id, place_of_service, network_ind, network_paid_ind, admit_date, discharge_date, discharge_status, 
                                      cpt_hcpcs_cd, procedure_type, proc_mod_1, proc_mod_2, drg_cd, revenue_cd, charge_amount, allowed_amount, 
                                      paid_amount, copay, deductible, coins, cob, bill_type_inst, bill_type_class, bill_type_freq, 
                                      units, fiscal_year, table_id_src, claim_sequence_number_src 
                                     )								   							   
select 'mcrt',  extract(year from a.clm_thru_dt::date), c.uth_member_id, c.uth_claim_id, a.clm_line_num::numeric, b.clm_from_dt::date, a.clm_thru_dt::date,
	    d.month_year_id, b.clm_fac_type_cd, true, true, b.clm_admsn_dt::date, b.nch_bene_dschrg_dt::date, b.ptnt_dschrg_stus_cd, 
	    a.hcpcs_cd, a.nch_clm_type_cd,  substring(a.hcpcs_1st_mdfr_cd,1,1),  substring(a.hcpcs_2nd_mdfr_cd,1,1), null, a.rev_cntr, a.rev_cntr_ncvrd_chrg_amt::numeric, null, 
	    null, null, null, null, null,  substring(b.clm_fac_type_cd,1,1),  substring(b.clm_srvc_clsfctn_type_cd,1,1),  substring(b.clm_freq_cd,1,1), 
	    a.rev_cntr_unit_cnt::int, b.year::int2, 'snf_revenue_center', a.clm_line_num 
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

---////
---outpatient
insert into dw_staging.claim_detail ( data_source, year, uth_member_id, uth_claim_id, claim_sequence_number, from_date_of_service, to_date_of_service,
                                      month_year_id, place_of_service, network_ind, network_paid_ind, admit_date, discharge_date, discharge_status, 
                                      cpt_hcpcs_cd, procedure_type, proc_mod_1, proc_mod_2, drg_cd, revenue_cd, charge_amount, allowed_amount, 
                                      paid_amount, copay, deductible, coins, cob, bill_type_inst, bill_type_class, bill_type_freq, 
                                      units, fiscal_year, table_id_src, claim_sequence_number_src 
                                     )								   							   
select 'mcrt',  extract(year from a.clm_thru_dt::date), c.uth_member_id, c.uth_claim_id, a.clm_line_num::numeric, b.clm_from_dt::date, a.clm_thru_dt::date,
	    d.month_year_id, b.clm_fac_type_cd, true, true, null, null, null, 
	    a.hcpcs_cd, a.nch_clm_type_cd,  substring(a.hcpcs_1st_mdfr_cd,1,1),  substring(a.hcpcs_2nd_mdfr_cd,1,1), null, a.rev_cntr, a.rev_cntr_ncvrd_chrg_amt::numeric, null, 
	    null, null, null, null, null,  substring(b.clm_fac_type_cd,1,1),  substring(b.clm_srvc_clsfctn_type_cd,1,1),  substring(b.clm_freq_cd,1,1), 
	    a.rev_cntr_unit_cnt::int, b.year::int2, 'outpatient_revenue_center', a.clm_line_num 
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



--bcarrier 
insert into dw_staging.claim_detail ( data_source, year, uth_member_id, uth_claim_id, claim_sequence_number, from_date_of_service, to_date_of_service,
                                      month_year_id, place_of_service, network_ind, network_paid_ind, admit_date, discharge_date, discharge_status, 
                                      cpt_hcpcs_cd, procedure_type, proc_mod_1, proc_mod_2, drg_cd, revenue_cd, charge_amount, allowed_amount, 
                                      paid_amount, copay, deductible, coins, cob, bill_type_inst, bill_type_class, bill_type_freq, 
                                      units, fiscal_year, table_id_src, claim_sequence_number_src 
                                     )								   							   
select 'mcrt',  extract(year from a.clm_thru_dt::date), c.uth_member_id, c.uth_claim_id, a.line_num::numeric, b.clm_from_dt::date, a.clm_thru_dt::date,
	    d.month_year_id, a.line_place_of_srvc_cd, true, true, null, null, null, 
	    a.hcpcs_cd, a.nch_clm_type_cd,  substring(a.hcpcs_1st_mdfr_cd,1,1),  substring(a.hcpcs_2nd_mdfr_cd,1,1), null, null as reveneue_cd, a.line_alowd_chrg_amt::numeric, null, 
	    a.line_bene_pmt_amt::numeric, null, a.line_service_deductible::numeric, a.line_coinsrnc_amt::numeric, null, null, null, null, 
	    a.line_srvc_cnt::numeric, b.year::int2, 'bcarrier_line', a.line_num    	      
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


--dme
insert into dw_staging.claim_detail ( data_source, year, uth_member_id, uth_claim_id, claim_sequence_number, from_date_of_service, to_date_of_service,
                                      month_year_id, place_of_service, network_ind, network_paid_ind, admit_date, discharge_date, discharge_status, 
                                      cpt_hcpcs_cd, procedure_type, proc_mod_1, proc_mod_2, drg_cd, revenue_cd, charge_amount, allowed_amount, 
                                      paid_amount, copay, deductible, coins, cob, bill_type_inst, bill_type_class, bill_type_freq, 
                                      units, fiscal_year, table_id_src, claim_sequence_number_src 
                                     )								   							   
select 'mcrt',  extract(year from a.clm_thru_dt::date), c.uth_member_id, c.uth_claim_id, a.line_num::numeric, b.clm_from_dt::date, a.clm_thru_dt::date,
	    d.month_year_id, a.line_place_of_srvc_cd, true, true, null, null, null, 
	    a.hcpcs_cd, a.nch_clm_type_cd,  substring(a.hcpcs_1st_mdfr_cd,1,1),  substring(a.hcpcs_2nd_mdfr_cd,1,1), null, null as reveneue_cd, a.line_alowd_chrg_amt::numeric, null, 
	    a.line_bene_pmt_amt::numeric, null, a.line_service_deductible::numeric, a.line_coinsrnc_amt::numeric, null, null, null, null, 
	    a.line_srvc_cnt::numeric, b.year::int2, 'dme_line', a.line_num    	    
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


---finalize 
vacuum analyze dw_staging.claim_detail;


select data_source, year, count(*) 
from dw_staging.claim_detail 
group by data_source , "year" 
order by data_source , year 
; 


------ END SCRIPT 