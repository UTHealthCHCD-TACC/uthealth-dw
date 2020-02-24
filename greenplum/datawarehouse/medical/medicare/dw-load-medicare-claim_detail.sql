

----inpatient
insert into dw_qa.claim_detail (  data_source, uth_claim_id, claim_sequence_number, uth_member_id, from_date_of_service, to_date_of_service,
								   month_year_id, perf_provider_id, bill_provider_id, ref_provider_id, place_of_service, network_ind, network_paid_ind,
								   admit_date, discharge_date, procedure_cd, procedure_type, proc_mod_1, proc_mod_2, revenue_cd,
								   charge_amount, allowed_amount, paid_amount, deductible, copay, coins, cob,
								   bill_type_inst, bill_type_class, bill_type_freq, units, drg_cd,
								   claim_id_src, member_id_src, table_id_src )									   							   
	select 'mdcr', c.uth_claim_id, a.clm_line_num::numeric, c.uth_member_id, b.clm_from_dt::date, b.clm_thru_dt::date,
	       d.month_year_id, b.prvdr_num, null, null, b.clm_fac_type_cd, true, true, 
	       b.clm_admsn_dt::date, b.nch_bene_dschrg_dt::date, a.hcpcs_cd,a.nch_clm_type_cd,  substring(a.hcpcs_1st_mdfr_cd,1,1),  substring(a.hcpcs_2nd_mdfr_cd,1,1), a.rev_cntr,
	       a.rev_cntr_tot_chrg_amt::numeric, null, null, null, null, null, null, 
	       substring(b.clm_fac_type_cd,1,1), substring(b.clm_srvc_clsfctn_type_cd,1,1), substring(b.clm_freq_cd,1,1), a.rev_cntr_unit_cnt::numeric, b.clm_drg_cd,
	       b.clm_id, b.bene_id, 'inpatient_revenue_center_k'
from medicare.inpatient_revenue_center_k a 
     join medicare.inpatient_base_claims_k b
       on b.clm_id = a.clm_id 
      and b.bene_id = a.bene_id 
     join data_warehouse.dim_uth_claim_id c
	    on c.data_source = 'mdcr'
	   and c.claim_id_src = a.clm_id
	   and c.member_id_src = a.bene_id 
	   and c.data_year = extract(year from clm_from_dt::date)
   join reference_tables.ref_month_year d
	    on d.month_int = extract(month from b.clm_from_dt::date) 
	   and d.year_int = extract(year from b.clm_from_dt::date) 
where extract(year from b.clm_from_dt::date) between 2015 and 2017
;



---outpatient
insert into dw_qa.claim_detail (  data_source, uth_claim_id, claim_sequence_number, uth_member_id, from_date_of_service, to_date_of_service,
								   month_year_id, perf_provider_id, bill_provider_id, ref_provider_id, place_of_service, network_ind, network_paid_ind,
								   admit_date, discharge_date, procedure_cd, procedure_type, proc_mod_1, proc_mod_2, revenue_cd,
								   charge_amount, allowed_amount, paid_amount, deductible, copay, coins, cob,
								   bill_type_inst, bill_type_class, bill_type_freq, units, drg_cd,
								   claim_id_src, member_id_src, table_id_src )											  						   
	select 'mdcr', c.uth_claim_id, a.clm_line_num::numeric, c.uth_member_id, b.clm_from_dt::date, b.clm_thru_dt::date,
	       d.month_year_id, b.prvdr_num, null, null, b.clm_fac_type_cd, true, true, 
	       null, null, a.hcpcs_cd,a.nch_clm_type_cd,  substring(a.hcpcs_1st_mdfr_cd,1,1),  substring(a.hcpcs_2nd_mdfr_cd,1,1), a.rev_cntr,
	       a.rev_cntr_tot_chrg_amt::numeric, null, null, null, null, null, null, 
	       substring(b.clm_fac_type_cd,1,1), substring(b.clm_srvc_clsfctn_type_cd,1,1), substring(b.clm_freq_cd,1,1), a.rev_cntr_unit_cnt::numeric, null,
	       b.clm_id, b.bene_id, 'outpatient_revenue_center_k'
from medicare.outpatient_revenue_center_k a 
     join medicare.outpatient_base_claims_k b
       on b.clm_id = a.clm_id 
      and b.bene_id = a.bene_id 
     join data_warehouse.dim_uth_claim_id c
	    on c.data_source = 'mdcr'
	   and c.claim_id_src = a.clm_id
	   and c.member_id_src = a.bene_id 
	   and c.data_year = extract(year from clm_from_dt::date)
   join reference_tables.ref_month_year d
	    on d.month_int = extract(month from b.clm_from_dt::date) 
	   and d.year_int = extract(year from b.clm_from_dt::date) 
where extract(year from b.clm_from_dt::date) between 2015 and 2017
;



--bcarrier 
select 'mdcr', c.uth_claim_id, a.clm_line_num::numeric, c.uth_member_id, b.clm_from_dt::date, b.clm_thru_dt::date,
	       d.month_year_id, b.prvdr_num, null, null, b.clm_fac_type_cd, true, true, 
	       null, null, a.hcpcs_cd,a.nch_clm_type_cd,  substring(a.hcpcs_1st_mdfr_cd,1,1),  substring(a.hcpcs_2nd_mdfr_cd,1,1), a.rev_cntr,
	       a.rev_cntr_tot_chrg_amt::numeric, null, null, null, null, null, null, 
	       substring(b.clm_fac_type_cd,1,1), substring(b.clm_srvc_clsfctn_type_cd,1,1), substring(b.clm_freq_cd,1,1), a.rev_cntr_unit_cnt::numeric, null,
	       b.clm_id, b.bene_id, 'bcarrier_claims_k'
	       
	       select a.*
from medicare.bcarrier_line_k a 
     join medicare.bcarrier_claims_k b
       on b.clm_id = a.clm_id 
      and b.bene_id = a.bene_id 
     join data_warehouse.dim_uth_claim_id c
	    on c.data_source = 'mdcr'
	   and c.claim_id_src = a.clm_id
	   and c.member_id_src = a.bene_id 
	   and c.data_year = extract(year from clm_from_dt::date)
   join reference_tables.ref_month_year d
	    on d.month_int = extract(month from b.clm_from_dt::date) 
	   and d.year_int = extract(year from b.clm_from_dt::date) 
where extract(year from b.clm_from_dt::date) between 2015 and 2017
;


select pid, usename, application_name, backend_start, xact_start, waiting, state, query
from pg_catalog.pg_stat_activity ;


select pg_terminate_backend(116384);

select version();




select * from medicare.inpatient_revenue_center_k



select distinct proctyp from truven.ccaeo;
--dme

--hha

--hospice

--snf





