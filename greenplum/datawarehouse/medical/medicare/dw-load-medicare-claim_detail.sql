delete from data_warehouse.claim_detail where data_source = 'mdcr';

vacuum analyze data_warehouse.claim_detail;


----inpatient
insert into data_warehouse.claim_detail (  data_source, year, uth_claim_id, claim_sequence_number, uth_member_id, from_date_of_service, to_date_of_service,
								   month_year_id, perf_provider_id, bill_provider_id, ref_provider_id, place_of_service, network_ind, network_paid_ind,
								   admit_date, discharge_date, procedure_cd, procedure_type, proc_mod_1, proc_mod_2, revenue_cd,
								   charge_amount, allowed_amount, paid_amount, deductible, copay, coins, cob,
								   bill_type_inst, bill_type_class, bill_type_freq, units, drg_cd,
								   claim_id_src, member_id_src, table_id_src )									   							   
	select 'mdcr', c.data_year, c.uth_claim_id, a.clm_line_num::numeric, c.uth_member_id, b.clm_from_dt::date, b.clm_thru_dt::date,
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
   join reference_tables.ref_month_year d
	    on d.month_int = extract(month from b.clm_from_dt::date) 
	   and d.year_int = extract(year from b.clm_from_dt::date) 
;


select count(*), year 
from data_warehouse.claim_detail 
where table_id_src = 'inpatient_revenue_center_k'
group by year 


select count(*), count(distinct clm_id), extract(year from clm_from_dt::date) 
from medicare.inpatient_base_claims_k s
group by extract(year from clm_from_dt::date) 


select count(*), count(distinct uth_claim_id), year 
from data_warehouse.claim_header 
where data_source = 'mdcr'
group by year ; 


---outpatient
insert into data_warehouse.claim_detail (  data_source, year, uth_claim_id, claim_sequence_number, uth_member_id, from_date_of_service, to_date_of_service,
								   month_year_id, perf_provider_id, bill_provider_id, ref_provider_id, place_of_service, network_ind, network_paid_ind,
								   admit_date, discharge_date, procedure_cd, procedure_type, proc_mod_1, proc_mod_2, revenue_cd,
								   charge_amount, allowed_amount, paid_amount, deductible, copay, coins, cob,
								   bill_type_inst, bill_type_class, bill_type_freq, units, drg_cd,
								   claim_id_src, member_id_src, table_id_src )											  						   
	select 'mdcr', c.data_year,c.uth_claim_id, a.clm_line_num::numeric, c.uth_member_id, b.clm_from_dt::date, b.clm_thru_dt::date,
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
   join reference_tables.ref_month_year d
	    on d.month_int = extract(month from b.clm_from_dt::date) 
	   and d.year_int = extract(year from b.clm_from_dt::date) 
;



--bcarrier   27m 2014
insert into data_warehouse.claim_detail (  data_source, year, uth_claim_id, claim_sequence_number, uth_member_id, from_date_of_service, to_date_of_service,
								   month_year_id, perf_provider_id, bill_provider_id, ref_provider_id, place_of_service, network_ind, network_paid_ind,
								   admit_date, discharge_date, procedure_cd, procedure_type, proc_mod_1, proc_mod_2, revenue_cd,
								   charge_amount, allowed_amount, paid_amount, deductible, copay, coins, cob,
								   bill_type_inst, bill_type_class, bill_type_freq, units, drg_cd,
								   claim_id_src, member_id_src, table_id_src )									  								   
select 'mdcr', c.data_year,c.uth_claim_id, a.line_num::numeric, c.uth_member_id, b.clm_from_dt::date, b.clm_thru_dt::date,
	       d.month_year_id, a.org_npi_num, null, null, a.line_place_of_srvc_cd, true, true, 
	       null, null, a.hcpcs_cd, a.nch_clm_type_cd,  substring(a.hcpcs_1st_mdfr_cd,1,1),  substring(a.hcpcs_2nd_mdfr_cd,1,1), null,
	       a.carr_line_cl_chrg_amt::numeric, null, a.line_bene_pmt_amt::numeric , a.line_service_deductible::numeric, null, a.line_coinsrnc_amt::numeric, null, 
	       null,null,null, a.line_srvc_cnt::numeric, null,
	       b.clm_id, b.bene_id, 'bcarrier_claims_k'	 	      	      
from medicare.bcarrier_line_k a 
     join medicare.bcarrier_claims_k b
       on b.clm_id = a.clm_id 
      and b.bene_id = a.bene_id 
     join data_warehouse.dim_uth_claim_id c 
        on c.data_source = 'mdcr'
	   and c.claim_id_src = a.clm_id
	   and c.member_id_src = a.bene_id 
   join reference_tables.ref_month_year d
	    on d.month_int = extract(month from b.clm_from_dt::date) 
	   and d.year_int = extract(year from b.clm_from_dt::date) 
where a.year = '2015'
;

drop table dev.wc_medicare_bcarrier_claim

create table dev.wc_medicare_bcarrier_claim
with(appendonly=true, orientation=column)
as select  a.line_num::numeric, b.clm_from_dt::date, b.clm_thru_dt::date,
	       a.org_npi_num, a.line_place_of_srvc_cd, 
	       a.hcpcs_cd, a.nch_clm_type_cd,  substring(a.hcpcs_1st_mdfr_cd,1,1) as procmod1,  substring(a.hcpcs_2nd_mdfr_cd,1,1) as procmod2,
	       a.carr_line_cl_chrg_amt::numeric, a.line_bene_pmt_amt::numeric , a.line_service_deductible::numeric, a.line_coinsrnc_amt::numeric, 
	       a.line_srvc_cnt::numeric,
	       b.clm_id, b.bene_id , b.year 
from medicare.bcarrier_line_k a
     join medicare.bcarrier_claims_k b
       on b.clm_id = a.clm_id 
      and b.bene_id = a.bene_id 
distributed by (clm_id)
;


create table dev.wc_medicare_uth
with(appendonly=true, orientation=column)
as select * 
from data_warehouse.dim_uth_claim_id 
where data_source = 'mdcr'
distributed by (claim_id_src)
;


select count(*), year  from data_warehouse.claim_detail cd where table_id_src = 'bcarrier_claims_k' group by year 

insert into data_warehouse.claim_detail (  data_source, year, uth_claim_id, claim_sequence_number, uth_member_id, from_date_of_service, to_date_of_service,
								   month_year_id, perf_provider_id, bill_provider_id, ref_provider_id, place_of_service, network_ind, network_paid_ind,
								   admit_date, discharge_date, procedure_cd, procedure_type, proc_mod_1, proc_mod_2, revenue_cd,
								   charge_amount, allowed_amount, paid_amount, deductible, copay, coins, cob,
								   bill_type_inst, bill_type_class, bill_type_freq, units, drg_cd,
								   claim_id_src, member_id_src, table_id_src )									  								   
select 'mdcr', c.data_year,c.uth_claim_id, a.line_num, c.uth_member_id, a.clm_from_dt, a.clm_thru_dt,
	       d.month_year_id, a.org_npi_num, null, null, a.line_place_of_srvc_cd, true, true, 
	       null, null, a.hcpcs_cd, a.nch_clm_type_cd,  a.procmod1,  a.procmod2, null,
	       a.carr_line_cl_chrg_amt, null, a.line_bene_pmt_amt , a.line_service_deductible, null, a.line_coinsrnc_amt, null, 
	       null,null,null, a.line_srvc_cnt::numeric, null,
	       a.clm_id, a.bene_id, 'bcarrier_claims_k'	 	      	      
from dev.wc_medicare_bcarrier_claim a
     join dev.wc_medicare_uth c 
        on c.data_source = 'mdcr'
	   and c.claim_id_src = a.clm_id
	   and c.member_id_src = a.bene_id 
   join reference_tables.ref_month_year d
	    on d.month_int = extract(month from a.clm_from_dt) 
	   and d.year_int = extract(year from a.clm_from_dt) 
where a.year = '2017'
;

------------------------------

delete from data_warehouse.claim_detail where table_id_src = 'dme_claims_k'

--dme
insert into data_warehouse.claim_detail (  data_source, year, uth_claim_id, claim_sequence_number, uth_member_id, from_date_of_service, to_date_of_service,
								   month_year_id, perf_provider_id, bill_provider_id, ref_provider_id, place_of_service, network_ind, network_paid_ind,
								   admit_date, discharge_date, procedure_cd, procedure_type, proc_mod_1, proc_mod_2, revenue_cd,
								   charge_amount, allowed_amount, paid_amount, deductible, copay, coins, cob,
								   bill_type_inst, bill_type_class, bill_type_freq, units, drg_cd,
								   claim_id_src, member_id_src, table_id_src )												   
	select 'mdcr',c.data_year,c.uth_claim_id, a.line_num::numeric, c.uth_member_id, b.clm_from_dt::date, b.clm_thru_dt::date,
	       d.month_year_id, a.prvdr_num, null, null, a.line_place_of_srvc_cd, true, true, 
	       null, null, a.hcpcs_cd, a.nch_clm_type_cd,  substring(a.hcpcs_1st_mdfr_cd,1,1),  substring(a.hcpcs_2nd_mdfr_cd,1,1), null,
	       a.line_sbmtd_chrg_amt::numeric, a.line_prmry_alowd_chrg_amt::numeric, a.line_bene_prmry_pyr_pd_amt::numeric, a.line_service_deductible::numeric, null, a.line_coinsrnc_amt::numeric, null,
	       null, null, null, a.dmerc_line_mtus_cnt::numeric, null,
	       a.clm_id, b.bene_id, 'dme_claims_k'
from medicare.dme_line_k a
  join medicare.dme_claims_k b
     on a.bene_id = b.bene_id
    and a.clm_id = b.clm_id 
  join data_warehouse.dim_uth_claim_id c 
    on c.data_source = 'mdcr' 
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id
 join reference_tables.ref_month_year d 
    on d.month_int = extract(month from b.clm_from_dt::date)
   and d.year_int = extract(year from b.clm_from_dt::date)
;


--hha
insert into data_warehouse.claim_detail (  data_source, year, uth_claim_id, claim_sequence_number, uth_member_id, from_date_of_service, to_date_of_service,
								   month_year_id, perf_provider_id, bill_provider_id, ref_provider_id, place_of_service, network_ind, network_paid_ind,
								   admit_date, discharge_date, procedure_cd, procedure_type, proc_mod_1, proc_mod_2, revenue_cd,
								   charge_amount, allowed_amount, paid_amount, deductible, copay, coins, cob,
								   bill_type_inst, bill_type_class, bill_type_freq, units, drg_cd,
								   claim_id_src, member_id_src, table_id_src )		
	select 'mdcr', c.data_year, c.uth_claim_id, a.clm_line_num::numeric, c.uth_member_id, b.clm_from_dt::date, b.clm_thru_dt::date,
	       d.month_year_id, b.prvdr_num, null, null, b.clm_fac_type_cd, true, true, 
	       null, null, a.hcpcs_cd,a.nch_clm_type_cd,  substring(a.hcpcs_1st_mdfr_cd,1,1),  substring(a.hcpcs_2nd_mdfr_cd,1,1), a.rev_cntr,
	       a.rev_cntr_tot_chrg_amt::numeric, null, null, null, null, null, null, 
	       substring(b.clm_fac_type_cd,1,1), substring(b.clm_srvc_clsfctn_type_cd,1,1), substring(b.clm_freq_cd,1,1), a.rev_cntr_unit_cnt::numeric, null,
	       b.clm_id, b.bene_id, 'hha_revenue_center_k'
from medicare.hha_revenue_center_k a 
     join medicare.hha_base_claims_k b
     on a.bene_id = b.bene_id
    and a.clm_id = b.clm_id 
  join data_warehouse.dim_uth_claim_id c 
    on c.data_source = 'mdcr' 
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id
 join reference_tables.ref_month_year d 
    on d.month_int = extract(month from b.clm_from_dt::date)
   and d.year_int = extract(year from b.clm_from_dt::date)
;



--hospice
insert into data_warehouse.claim_detail (  data_source, year, uth_claim_id, claim_sequence_number, uth_member_id, from_date_of_service, to_date_of_service,
								   month_year_id, perf_provider_id, bill_provider_id, ref_provider_id, place_of_service, network_ind, network_paid_ind,
								   admit_date, discharge_date, procedure_cd, procedure_type, proc_mod_1, proc_mod_2, revenue_cd,
								   charge_amount, allowed_amount, paid_amount, deductible, copay, coins, cob,
								   bill_type_inst, bill_type_class, bill_type_freq, units, drg_cd,
								   claim_id_src, member_id_src, table_id_src )		
	select 'mdcr', c.data_year,c.uth_claim_id, a.clm_line_num::numeric, c.uth_member_id, b.clm_from_dt::date, b.clm_thru_dt::date,
	       d.month_year_id, b.prvdr_num, null, null, b.clm_fac_type_cd, true, true, 
	       null, null, a.hcpcs_cd,a.nch_clm_type_cd,  substring(a.hcpcs_1st_mdfr_cd,1,1),  substring(a.hcpcs_2nd_mdfr_cd,1,1), a.rev_cntr,
	       a.rev_cntr_tot_chrg_amt::numeric, null, null, null, null, null, null, 
	       substring(b.clm_fac_type_cd,1,1), substring(b.clm_srvc_clsfctn_type_cd,1,1), substring(b.clm_freq_cd,1,1), a.rev_cntr_unit_cnt::numeric, null,
	       b.clm_id, b.bene_id, 'hospice_base_claims_k'
from medicare.hospice_revenue_center_k a 
     join medicare.hospice_base_claims_k b
     on a.bene_id = b.bene_id
    and a.clm_id = b.clm_id 
  join data_warehouse.dim_uth_claim_id c 
    on c.data_source = 'mdcr' 
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id
 join reference_tables.ref_month_year d 
    on d.month_int = extract(month from b.clm_from_dt::date)
   and d.year_int = extract(year from b.clm_from_dt::date)
;

--snf
insert into data_warehouse.claim_detail (  data_source, year, uth_claim_id, claim_sequence_number, uth_member_id, from_date_of_service, to_date_of_service,
								   month_year_id, perf_provider_id, bill_provider_id, ref_provider_id, place_of_service, network_ind, network_paid_ind,
								   admit_date, discharge_date, procedure_cd, procedure_type, proc_mod_1, proc_mod_2, revenue_cd,
								   charge_amount, allowed_amount, paid_amount, deductible, copay, coins, cob,
								   bill_type_inst, bill_type_class, bill_type_freq, units, drg_cd,
								   claim_id_src, member_id_src, table_id_src )		
	select 'mdcr', c.data_year,c.uth_claim_id, a.clm_line_num::numeric, c.uth_member_id, b.clm_from_dt::date, b.clm_thru_dt::date,
	       d.month_year_id, b.prvdr_num, null, null, b.clm_fac_type_cd, true, true, 
	       null, null, a.hcpcs_cd,a.nch_clm_type_cd,  substring(a.hcpcs_1st_mdfr_cd,1,1),  substring(a.hcpcs_2nd_mdfr_cd,1,1), a.rev_cntr,
	       a.rev_cntr_tot_chrg_amt::numeric, null, null, null, null, null, null, 
	       substring(b.clm_fac_type_cd,1,1), substring(b.clm_srvc_clsfctn_type_cd,1,1), substring(b.clm_freq_cd,1,1), a.rev_cntr_unit_cnt::numeric, null,
	       b.clm_id, b.bene_id, 'snf_base_claims_k'
from medicare.snf_revenue_center_k a 
     join medicare.snf_base_claims_k b
     on a.bene_id = b.bene_id
    and a.clm_id = b.clm_id 
  join data_warehouse.dim_uth_claim_id c 
    on c.data_source = 'mdcr' 
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id
 join reference_tables.ref_month_year d 
    on d.month_int = extract(month from b.clm_from_dt::date)
   and d.year_int = extract(year from b.clm_from_dt::date)
;



vacuum analyze data_warehouse.claim_detail;


