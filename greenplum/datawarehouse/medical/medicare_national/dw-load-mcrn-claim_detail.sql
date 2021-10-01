
/* ******************************************************************************************************
 *  load claim detail for medicare national
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 * ******************************************************************************************************
 *  wcc001  || 10/01/2021 || add comment block. migrate to dw_staging load 
 * ****************************************************************************************************** 
 * */


--------------- BEGIN SCRIPT -------

---create copy of data warehouse table in dw_staging 
drop table if exists dw_staging.claim_detail;

create table dw_staging.claim_detail 
with (appendonly=true, orientation=column, compresstype=zlib, compresslevel=5) as 
select *
from data_warehouse.claim_detail 
where data_source not in ('mcrn','mcrt')
distributed by (uth_member_id) 
;

vacuum analyze dw_staging.claim_detail;


----inpatient
insert into dw_staging.claim_detail ( data_source, year, uth_member_id, uth_claim_id, claim_sequence_number, from_date_of_service, to_date_of_service,
                                      month_year_id, place_of_service, network_ind, network_paid_ind, admit_date, discharge_date, discharge_status, 
                                      cpt_hcpcs_cd, procedure_type, proc_mod_1, proc_mod_2, drg_cd, revenue_cd, charge_amount, allowed_amount, 
                                      paid_amount, copay, deductible, coins, cob, bill_type_inst, bill_type_class, bill_type_freq, 
                                      units, fiscal_year, table_id_src, claim_sequence_number_src 
                                     )


								   							   
	select 'mcrn', c.data_year, c.uth_member_id, c.uth_claim_id, a.clm_line_num::numeric, b.clm_from_dt, a.clm_thru_dt::date
	       d.month_year_id, 
	       
	       b.prvdr_num, null, null, b.clm_fac_type_cd, true, true, 
	       b.clm_admsn_dt::date, b.nch_bene_dschrg_dt::date, a.hcpcs_cd,a.nch_clm_type_cd,  substring(a.hcpcs_1st_mdfr_cd,1,1),  substring(a.hcpcs_2nd_mdfr_cd,1,1), a.rev_cntr,
	       a.rev_cntr_tot_chrg_amt::numeric, null, null, null, null, null, null, 
	       substring(b.clm_fac_type_cd,1,1), substring(b.clm_srvc_clsfctn_type_cd,1,1), substring(b.clm_freq_cd,1,1), a.rev_cntr_unit_cnt::numeric, b.clm_drg_cd,
	       b.clm_id, b.bene_id, 'inpatient_revenue_center_k'
from medicare_national.inpatient_revenue_center_k a 
     join medicare_national.inpatient_base_claims_k b
       on b.bene_id = a.bene_id 
      and b.clm_id = a.clm_id  
     join data_warehouse.dim_uth_claim_id c
        on c.member_id_src = a.bene_id 
	   and c.data_source = 'mcrn'
	   and c.claim_id_src = a.clm_id
   join reference_tables.ref_month_year d
	    on d.month_int = extract(month from b.clm_from_dt::date) 
	   and d.year_int = extract(year from b.clm_from_dt::date) 
;


---outpatient
insert into dw_staging.claim_detail (  data_source, year, uth_claim_id, claim_sequence_number, uth_member_id, from_date_of_service, to_date_of_service,
								   month_year_id, perf_provider_id, bill_provider_id, ref_provider_id, place_of_service, network_ind, network_paid_ind,
								   admit_date, discharge_date, procedure_cd, procedure_type, proc_mod_1, proc_mod_2, revenue_cd,
								   charge_amount, allowed_amount, paid_amount, deductible, copay, coins, cob,
								   bill_type_inst, bill_type_class, bill_type_freq, units, drg_cd,
								   claim_id_src, member_id_src, table_id_src )											  						   
	select 'mcrn', c.data_year,c.uth_claim_id, a.clm_line_num::numeric, c.uth_member_id, b.clm_from_dt::date, b.clm_thru_dt::date,
	       d.month_year_id, b.prvdr_num, null, null, b.clm_fac_type_cd, true, true, 
	       null, null, a.hcpcs_cd,a.nch_clm_type_cd,  substring(a.hcpcs_1st_mdfr_cd,1,1),  substring(a.hcpcs_2nd_mdfr_cd,1,1), a.rev_cntr,
	       a.rev_cntr_tot_chrg_amt::numeric, null, null, null, null, null, null, 
	       substring(b.clm_fac_type_cd,1,1), substring(b.clm_srvc_clsfctn_type_cd,1,1), substring(b.clm_freq_cd,1,1), a.rev_cntr_unit_cnt::numeric, null,
	       b.clm_id, b.bene_id, 'outpatient_revenue_center_k'
from medicare_national.outpatient_revenue_center_k a 
     join medicare_national.outpatient_base_claims_k b
       on b.bene_id = a.bene_id 
      and b.clm_id = a.clm_id 
     join data_warehouse.dim_uth_claim_id c
	    on c.member_id_src = a.bene_id
	   and c.data_source = 'mcrn'
	   and c.claim_id_src = a.clm_id
   join reference_tables.ref_month_year d
	    on d.month_int = extract(month from b.clm_from_dt::date) 
	   and d.year_int = extract(year from b.clm_from_dt::date) 
;



--bcarrier   27m 2014
insert into dw_staging.claim_detail (  data_source, year, uth_claim_id, claim_sequence_number, uth_member_id, from_date_of_service, to_date_of_service,
								   month_year_id, perf_provider_id, bill_provider_id, ref_provider_id, place_of_service, network_ind, network_paid_ind,
								   admit_date, discharge_date, procedure_cd, procedure_type, proc_mod_1, proc_mod_2, revenue_cd,
								   charge_amount, allowed_amount, paid_amount, deductible, copay, coins, cob,
								   bill_type_inst, bill_type_class, bill_type_freq, units, drg_cd,
								   claim_id_src, member_id_src, table_id_src )									  								   
select 'mcrn', c.data_year,c.uth_claim_id, a.line_num::numeric, c.uth_member_id, b.clm_from_dt::date, b.clm_thru_dt::date,
	       d.month_year_id, a.org_npi_num, null, null, a.line_place_of_srvc_cd, true, true, 
	       null, null, a.hcpcs_cd, a.nch_clm_type_cd,  substring(a.hcpcs_1st_mdfr_cd,1,1),  substring(a.hcpcs_2nd_mdfr_cd,1,1), null,
	       a.carr_line_cl_chrg_amt::numeric, null, a.line_bene_pmt_amt::numeric , a.line_service_deductible::numeric, null, a.line_coinsrnc_amt::numeric, null, 
	       null,null,null, a.line_srvc_cnt::numeric, null,
	       b.clm_id, b.bene_id, 'bcarrier_claims_k'	 	      	      
from medicare_national.bcarrier_line_k a 
     join medicare_national.bcarrier_claims_k b
       on b.bene_id = a.bene_id 
      and b.clm_id = a.clm_id 
     join data_warehouse.dim_uth_claim_id c 
        on c.member_id_src = a.bene_id 
       and c.data_source = 'mcrn'
	   and c.claim_id_src = a.clm_id
   join reference_tables.ref_month_year d
	    on d.month_int = extract(month from b.clm_from_dt::date) 
	   and d.year_int = extract(year from b.clm_from_dt::date) 
;


--dme
insert into dw_staging.claim_detail (  data_source, year, uth_claim_id, claim_sequence_number, uth_member_id, from_date_of_service, to_date_of_service,
								   month_year_id, perf_provider_id, bill_provider_id, ref_provider_id, place_of_service, network_ind, network_paid_ind,
								   admit_date, discharge_date, procedure_cd, procedure_type, proc_mod_1, proc_mod_2, revenue_cd,
								   charge_amount, allowed_amount, paid_amount, deductible, copay, coins, cob,
								   bill_type_inst, bill_type_class, bill_type_freq, units, drg_cd,
								   claim_id_src, member_id_src, table_id_src )												   
	select 'mcrn',c.data_year,c.uth_claim_id, a.line_num::numeric, c.uth_member_id, b.clm_from_dt::date, b.clm_thru_dt::date,
	       d.month_year_id, a.prvdr_num, null, null, a.line_place_of_srvc_cd, true, true, 
	       null, null, a.hcpcs_cd, a.nch_clm_type_cd,  substring(a.hcpcs_1st_mdfr_cd,1,1),  substring(a.hcpcs_2nd_mdfr_cd,1,1), null,
	       a.line_sbmtd_chrg_amt::numeric, a.line_prmry_alowd_chrg_amt::numeric, a.line_bene_prmry_pyr_pd_amt::numeric, a.line_service_deductible::numeric, null, a.line_coinsrnc_amt::numeric, null,
	       null, null, null, a.dmerc_line_mtus_cnt::numeric, null,
	       a.clm_id, b.bene_id, 'dme_claims_k'
from medicare_national.dme_line_k a
  join medicare_national.dme_claims_k b
     on a.clm_id = b.clm_id 
    and a.bene_id = b.bene_id
  join data_warehouse.dim_uth_claim_id c 
    on  c.member_id_src = a.bene_id
   and c.data_source = 'mcrn' 
   and c.claim_id_src = a.clm_id
 join reference_tables.ref_month_year d 
    on d.month_int = extract(month from b.clm_from_dt::date)
   and d.year_int = extract(year from b.clm_from_dt::date)
;


--hha
insert into dw_staging.claim_detail (  data_source, year, uth_claim_id, claim_sequence_number, uth_member_id, from_date_of_service, to_date_of_service,
								   month_year_id, perf_provider_id, bill_provider_id, ref_provider_id, place_of_service, network_ind, network_paid_ind,
								   admit_date, discharge_date, procedure_cd, procedure_type, proc_mod_1, proc_mod_2, revenue_cd,
								   charge_amount, allowed_amount, paid_amount, deductible, copay, coins, cob,
								   bill_type_inst, bill_type_class, bill_type_freq, units, drg_cd,
								   claim_id_src, member_id_src, table_id_src )		
	select 'mcrn', c.data_year, c.uth_claim_id, a.clm_line_num::numeric, c.uth_member_id, b.clm_from_dt::date, b.clm_thru_dt::date,
	       d.month_year_id, b.prvdr_num, null, null, b.clm_fac_type_cd, true, true, 
	       null, null, a.hcpcs_cd,a.nch_clm_type_cd,  substring(a.hcpcs_1st_mdfr_cd,1,1),  substring(a.hcpcs_2nd_mdfr_cd,1,1), a.rev_cntr,
	       a.rev_cntr_tot_chrg_amt::numeric, null, null, null, null, null, null, 
	       substring(b.clm_fac_type_cd,1,1), substring(b.clm_srvc_clsfctn_type_cd,1,1), substring(b.clm_freq_cd,1,1), a.rev_cntr_unit_cnt::numeric, null,
	       b.clm_id, b.bene_id, 'hha_revenue_center_k'
from medicare_national.hha_revenue_center_k a 
     join medicare_national.hha_base_claims_k b
     on a.bene_id = b.bene_id
    and a.clm_id = b.clm_id 
  join data_warehouse.dim_uth_claim_id c 
    on c.member_id_src = a.bene_id
   and c.data_source = 'mcrn' 
   and c.claim_id_src = a.clm_id
 join reference_tables.ref_month_year d 
    on d.month_int = extract(month from b.clm_from_dt::date)
   and d.year_int = extract(year from b.clm_from_dt::date)
;


select * 
from dw_staging.claim_detail cd 
where data_source = 'mcrn' 
and table_id_src = 'inpatient_revenue_center_k';


--hospice
insert into dw_staging.claim_detail (  data_source, year, uth_claim_id, claim_sequence_number, uth_member_id, from_date_of_service, to_date_of_service,
								   month_year_id, perf_provider_id, bill_provider_id, ref_provider_id, place_of_service, network_ind, network_paid_ind,
								   admit_date, discharge_date, procedure_cd, procedure_type, proc_mod_1, proc_mod_2, revenue_cd,
								   charge_amount, allowed_amount, paid_amount, deductible, copay, coins, cob,
								   bill_type_inst, bill_type_class, bill_type_freq, units, drg_cd,
								   claim_id_src, member_id_src, table_id_src )		
	select 'mcrn', c.data_year,c.uth_claim_id, a.clm_line_num::numeric, c.uth_member_id, b.clm_from_dt::date, b.clm_thru_dt::date,
	       d.month_year_id, b.prvdr_num, null, null, b.clm_fac_type_cd, true, true, 
	       null, null, a.hcpcs_cd,a.nch_clm_type_cd,  substring(a.hcpcs_1st_mdfr_cd,1,1),  substring(a.hcpcs_2nd_mdfr_cd,1,1), a.rev_cntr,
	       a.rev_cntr_tot_chrg_amt::numeric, null, null, null, null, null, null, 
	       substring(b.clm_fac_type_cd,1,1), substring(b.clm_srvc_clsfctn_type_cd,1,1), substring(b.clm_freq_cd,1,1), a.rev_cntr_unit_cnt::numeric, null,
	       b.clm_id, b.bene_id, 'hospice_base_claims_k'
from medicare_national.hospice_revenue_center_k a 
     join medicare_national.hospice_base_claims_k b
     on a.bene_id = b.bene_id
    and a.clm_id = b.clm_id 
  join data_warehouse.dim_uth_claim_id c 
    on c.member_id_src = a.bene_id
   and c.data_source = 'mcrn' 
   and c.claim_id_src = a.clm_id
 join reference_tables.ref_month_year d 
    on d.month_int = extract(month from b.clm_from_dt::date)
   and d.year_int = extract(year from b.clm_from_dt::date)
;

--snf
insert into dw_staging.claim_detail (  data_source, year, uth_claim_id, claim_sequence_number, uth_member_id, from_date_of_service, to_date_of_service,
								   month_year_id, perf_provider_id, bill_provider_id, ref_provider_id, place_of_service, network_ind, network_paid_ind,
								   admit_date, discharge_date, procedure_cd, procedure_type, proc_mod_1, proc_mod_2, revenue_cd,
								   charge_amount, allowed_amount, paid_amount, deductible, copay, coins, cob,
								   bill_type_inst, bill_type_class, bill_type_freq, units, drg_cd,
								   claim_id_src, member_id_src, table_id_src )		
	select 'mcrn', c.data_year,c.uth_claim_id, a.clm_line_num::numeric, c.uth_member_id, b.clm_from_dt::date, b.clm_thru_dt::date,
	       d.month_year_id, b.prvdr_num, null, null, b.clm_fac_type_cd, true, true, 
	       null, null, a.hcpcs_cd,a.nch_clm_type_cd,  substring(a.hcpcs_1st_mdfr_cd,1,1),  substring(a.hcpcs_2nd_mdfr_cd,1,1), a.rev_cntr,
	       a.rev_cntr_tot_chrg_amt::numeric, null, null, null, null, null, null, 
	       substring(b.clm_fac_type_cd,1,1), substring(b.clm_srvc_clsfctn_type_cd,1,1), substring(b.clm_freq_cd,1,1), a.rev_cntr_unit_cnt::numeric, null,
	       b.clm_id, b.bene_id, 'snf_base_claims_k'
from medicare_national.snf_revenue_center_k a 
     join medicare_national.snf_base_claims_k b
     on a.bene_id = b.bene_id
    and a.clm_id = b.clm_id 
  join data_warehouse.dim_uth_claim_id c 
    on c.member_id_src = a.bene_id
   and c.data_source = 'mcrn' 
   and c.claim_id_src = a.clm_id
 join reference_tables.ref_month_year d 
    on d.month_int = extract(month from b.clm_from_dt::date)
   and d.year_int = extract(year from b.clm_from_dt::date)
;



vacuum analyze dw_staging.claim_detail;

-------************add discharge status and dates for all claims 1/14/2021 WC ************


---outpatient has no discharge date
update dw_staging.claim_detail a set discharge_status = b.ptnt_dschrg_stus_cd
from medicare_national.outpatient_base_claims_k b 
where b.bene_id = a.member_id_src 
and b.clm_id = a.claim_id_src 
and a.data_source = 'mcrn'
;


---inpatient discharge date is coded in main insert
update dw_staging.claim_detail a set discharge_status = b.ptnt_dschrg_stus_cd 
from medicare_national.inpatient_base_claims_k b 
where b.bene_id = a.member_id_src 
and b.clm_id = a.claim_id_src 
and a.data_source = 'mcrn'
;

---for snf, hha, and hospice, add both discharge date and status

--hha
update dw_staging.claim_detail a set discharge_status = b.ptnt_dschrg_stus_cd, 
                                         discharge_date = b.nch_bene_dschrg_dt::date, 
                                         admit_date = b.clm_admsn_dt::date 
from medicare_national.hha_base_claims_k b 
where b.bene_id = a.member_id_src 
and b.clm_id = a.claim_id_src 
and a.data_source = 'mcrn'
;

--hospice
update dw_staging.claim_detail a set discharge_status = b.ptnt_dschrg_stus_cd, 
                                         discharge_date = b.nch_bene_dschrg_dt::date, 
                                         admit_date =  b.clm_hospc_start_dt_id::date
from medicare_national.hospice_base_claims_k b 
where b.bene_id = a.member_id_src 
and b.clm_id = a.claim_id_src 
and a.data_source = 'mcrn'
;

--snf
update dw_staging.claim_detail a set discharge_status = b.ptnt_dschrg_stus_cd, 
                                         discharge_date = b.nch_bene_dschrg_dt::date, 
                                         admit_date = b.clm_admsn_dt::date 
from medicare_national.snf_base_claims_k b 
where b.bene_id = a.member_id_src 
and b.clm_id = a.claim_id_src 
and a.data_source = 'mcrn'
;

vacuum analyze dw_staging.claim_detail;

---validate discharge changes
select * from dw_staging.claim_detail where data_source = 'mcrn' and discharge_date is not null;

select * from dw_staging.claim_detail cd where data_source = 'mcrn' and bill_type_inst is not null and discharge_status is null;

----- ************ end discharge status updates ****************
