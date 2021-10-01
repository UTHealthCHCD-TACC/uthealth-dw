delete from data_warehouse.claim_detail where data_source = 'mcrt';

vacuum analyze data_warehouse.claim_detail;


----inpatient
insert into data_warehouse.claim_detail (  data_source, year, uth_claim_id, claim_sequence_number, uth_member_id, from_date_of_service, to_date_of_service,
								   month_year_id, perf_provider_id, bill_provider_id, ref_provider_id, place_of_service, network_ind, network_paid_ind,
								   admit_date, discharge_date, procedure_cd, procedure_type, proc_mod_1, proc_mod_2, revenue_cd,
								   charge_amount, allowed_amount, paid_amount, deductible, copay, coins, cob,
								   bill_type_inst, bill_type_class, bill_type_freq, units, drg_cd,
								   claim_id_src, member_id_src, table_id_src, data_year )									   							   
	select 'mcrt', c.data_year, c.uth_claim_id, a.clm_line_num::numeric, c.uth_member_id, b.clm_from_dt::date, b.clm_thru_dt::date,
	       d.month_year_id, b.prvdr_num, null, null, b.clm_fac_type_cd, true, true, 
	       b.clm_admsn_dt::date, b.nch_bene_dschrg_dt::date, a.hcpcs_cd,a.nch_clm_type_cd,  substring(a.hcpcs_1st_mdfr_cd,1,1),  substring(a.hcpcs_2nd_mdfr_cd,1,1), a.rev_cntr,
	       a.rev_cntr_tot_chrg_amt::numeric, null, null, null, null, null, null, 
	       substring(b.clm_fac_type_cd,1,1), substring(b.clm_srvc_clsfctn_type_cd,1,1), substring(b.clm_freq_cd,1,1), a.rev_cntr_unit_cnt::numeric, b.clm_drg_cd,
	       b.clm_id, b.bene_id, 'inpatient_revenue_center_k', c.data_year 
from uthealth/medicare_national.inpatient_revenue_center_k a 
     join uthealth/medicare_national.inpatient_base_claims_k b
       on b.clm_id = a.clm_id 
      and b.bene_id = a.bene_id 
     join data_warehouse.dim_uth_claim_id c
	    on c.data_source = 'mcrt'
	   and c.claim_id_src = a.clm_id
	   and c.member_id_src = a.bene_id 
   join reference_tables.ref_month_year d
	    on d.month_int = extract(month from b.clm_from_dt::date) 
	   and d.year_int = extract(year from b.clm_from_dt::date) 
;

select distinct ptnt_dschrg_stus_cd 
from uthealth/medicare_national.inpatient_base_claims_k a 

select   a.line_coinsrnc_amt , a.line_service_deductible 
from uthealth/medicare_national.bcarrier_line_k a

---outpatient
insert into data_warehouse.claim_detail (  data_source, year, uth_claim_id, claim_sequence_number, uth_member_id, from_date_of_service, to_date_of_service,
								   month_year_id, perf_provider_id, bill_provider_id, ref_provider_id, place_of_service, network_ind, network_paid_ind,
								   admit_date, discharge_date, procedure_cd, procedure_type, proc_mod_1, proc_mod_2, revenue_cd,
								   charge_amount, allowed_amount, paid_amount, deductible, copay, coins, cob,
								   bill_type_inst, bill_type_class, bill_type_freq, units, drg_cd,
								   claim_id_src, member_id_src, table_id_src, data_year )											  						   
	select 'mcrt', c.data_year,c.uth_claim_id, a.clm_line_num::numeric, c.uth_member_id, b.clm_from_dt::date, b.clm_thru_dt::date,
	       d.month_year_id, b.prvdr_num, null, null, b.clm_fac_type_cd, true, true, 
	       null, null, a.hcpcs_cd,a.nch_clm_type_cd,  substring(a.hcpcs_1st_mdfr_cd,1,1),  substring(a.hcpcs_2nd_mdfr_cd,1,1), a.rev_cntr,
	       a.rev_cntr_tot_chrg_amt::numeric, null, null, null, null, null, null, 
	       substring(b.clm_fac_type_cd,1,1), substring(b.clm_srvc_clsfctn_type_cd,1,1), substring(b.clm_freq_cd,1,1), a.rev_cntr_unit_cnt::numeric, null,
	       b.clm_id, b.bene_id, 'outpatient_revenue_center_k', c.data_year 
from uthealth/medicare_national.outpatient_revenue_center_k a 
     join uthealth/medicare_national.outpatient_base_claims_k b
       on b.clm_id = a.clm_id 
      and b.bene_id = a.bene_id 
     join data_warehouse.dim_uth_claim_id c
	    on c.data_source = 'mcrt'
	   and c.claim_id_src = a.clm_id
	   and c.member_id_src = a.bene_id 
   join reference_tables.ref_month_year d
	    on d.month_int = extract(month from b.clm_from_dt::date) 
	   and d.year_int = extract(year from b.clm_from_dt::date) 
;

-------************add discharge status and dates for all claims 12/17/2020 WC ************
alter table data_warehouse.claim_detail add column discharge_status char(2);


---outpatient has no discharge date
update data_warehouse.claim_detail a set discharge_status = b.ptnt_dschrg_stus_cd
from uthealth/medicare_national.outpatient_base_claims_k b 
where b.bene_id = a.member_id_src 
and b.clm_id = a.claim_id_src 
and a.data_source = 'mcrt'
;

select a.
from uthealth/medicare_national.outpatient_revenue_center_k a


---inpatient discharge date is coded in main insert
update data_warehouse.claim_detail a set discharge_status = b.ptnt_dschrg_stus_cd 
from uthealth/medicare_national.inpatient_base_claims_k b 
where b.bene_id = a.member_id_src 
and b.clm_id = a.claim_id_src 
and a.data_source = 'mcrt'
;

---for snf, hha, and hospice, add both discharge date and status

--hha
update data_warehouse.claim_detail a set discharge_status = b.ptnt_dschrg_stus_cd, 
                                         discharge_date = b.nch_bene_dschrg_dt::date, 
                                         admit_date = b.clm_admsn_dt::date 
from uthealth/medicare_national.hha_base_claims_k b 
where b.bene_id = a.member_id_src 
and b.clm_id = a.claim_id_src 
and a.data_source = 'mcrt'
;

--hospice
update data_warehouse.claim_detail a set discharge_status = b.ptnt_dschrg_stus_cd, 
                                         discharge_date = b.nch_bene_dschrg_dt::date, 
                                        -- admit_date =  b.clm_hospc_start_dt_id::date
from uthealth/medicare_national.hospice_base_claims_k b 
where b.bene_id = a.member_id_src 
and b.clm_id = a.claim_id_src 
and a.data_source = 'mcrt'
;

--snf
update data_warehouse.claim_detail a set discharge_status = b.ptnt_dschrg_stus_cd, 
                                         discharge_date = b.nch_bene_dschrg_dt::date, 
                                         admit_date = b.clm_admsn_dt::date 
from uthealth/medicare_national.snf_base_claims_k b 
where b.bene_id = a.member_id_src 
and b.clm_id = a.claim_id_src 
and a.data_source = 'mcrt'
;

vacuum analyze data_warehouse.claim_detail;

---validate discharge changes
select * from data_warehouse.claim_detail where data_source = 'mcrt' and discharge_date is not null;

select * from data_warehouse.claim_detail cd where data_source = 'mcrt' and bill_type_inst is not null and discharge_status is null;

----- ************ end discharge status updates ****************

---bcarrier setup
create table dev.wc_bcarrier_claim_tx
with (appendonly=true, orientation=column)
as 
select * from uthealth/medicare_national.bcarrier_claims_k
distributed by (clm_id);

create table dev.wc_bcarrier_line_tx
with (appendonly=true, orientation=column)
as 
select * from uthealth/medicare_national.bcarrier_line_k
distributed by (clm_id);


create table dev.wc_uth_claim_bcarrier_tx 
with (appendonly=true, orientation=column)
as 
select * from data_warehouse.dim_uth_claim_id where data_source = 'mcrt'
distributed by (claim_id_src);


delete from data_warehouse.claim_detail where data_source = 'mcrt' and table_id_src = 'bcarrier_claims_k';


--bcarrier load 1m 5s
insert into data_warehouse.claim_detail (  data_source, year, uth_claim_id, claim_sequence_number, uth_member_id, from_date_of_service, to_date_of_service,
								   month_year_id, perf_provider_id, bill_provider_id, ref_provider_id, place_of_service, network_ind, network_paid_ind,
								   admit_date, discharge_date, procedure_cd, procedure_type, proc_mod_1, proc_mod_2, revenue_cd,
								   charge_amount, allowed_amount, paid_amount, deductible, copay, coins, cob,
								   bill_type_inst, bill_type_class, bill_type_freq, units, drg_cd,
								   claim_id_src, member_id_src, table_id_src, data_year )									  								   
select 'mcrt', c.data_year,c.uth_claim_id, a.line_num::numeric, c.uth_member_id, b.clm_from_dt::date, b.clm_thru_dt::date,
	       d.month_year_id, a.org_npi_num, null, null, a.line_place_of_srvc_cd, true, true, 
	       null, null, a.hcpcs_cd, a.nch_clm_type_cd,  substring(a.hcpcs_1st_mdfr_cd,1,1),  substring(a.hcpcs_2nd_mdfr_cd,1,1), null,
	       a.carr_line_cl_chrg_amt::numeric, null, a.line_bene_pmt_amt::numeric , a.line_bene_ptb_ddctbl_amt::numeric, null, a.line_coinsrnc_amt::numeric, null, 
	       null,null,null, a.line_srvc_cnt::numeric, null,
	       b.clm_id, b.bene_id, 'bcarrier_claims_k',	c.data_year  	      	      
from dev.wc_bcarrier_line_tx a --from uthealth/medicare_national.bcarrier_line_k a 
     join dev.wc_bcarrier_claim_tx b --join uthealth/medicare_national.bcarrier_claims_k b
       on b.clm_id = a.clm_id 
      and b.bene_id = a.bene_id 
     join dev.wc_uth_claim_bcarrier_tx c -- data_warehouse.dim_uth_claim_id c 
        on c.claim_id_src = a.clm_id
	   and c.member_id_src = a.bene_id 
   join reference_tables.ref_month_year d
	    on d.month_int = extract(month from b.clm_from_dt::date) 
	   and d.year_int = extract(year from b.clm_from_dt::date) 
;






--cleanup from bcarrier
drop table dev.wc_bcarrier_claim_tx;
drop table dev.wc_bcarrier_line_tx;
drop table dev.wc_uth_claim_bcarrier_tx; 


--dme
insert into data_warehouse.claim_detail (  data_source, year, uth_claim_id, claim_sequence_number, uth_member_id, from_date_of_service, to_date_of_service,
								   month_year_id, perf_provider_id, bill_provider_id, ref_provider_id, place_of_service, network_ind, network_paid_ind,
								   admit_date, discharge_date, procedure_cd, procedure_type, proc_mod_1, proc_mod_2, revenue_cd,
								   charge_amount, allowed_amount, paid_amount, deductible, copay, coins, cob,
								   bill_type_inst, bill_type_class, bill_type_freq, units, drg_cd,
								   claim_id_src, member_id_src, table_id_src, data_year )												   
	select 'mcrt',c.data_year,c.uth_claim_id, a.line_num::numeric, c.uth_member_id, b.clm_from_dt::date, b.clm_thru_dt::date,
	       d.month_year_id, a.prvdr_num, null, null, a.line_place_of_srvc_cd, true, true, 
	       null, null, a.hcpcs_cd, a.nch_clm_type_cd,  substring(a.hcpcs_1st_mdfr_cd,1,1),  substring(a.hcpcs_2nd_mdfr_cd,1,1), null,
	       a.line_sbmtd_chrg_amt::numeric, a.line_prmry_alowd_chrg_amt::numeric, a.line_bene_prmry_pyr_pd_amt::numeric, a.line_service_deductible::numeric, null, a.line_coinsrnc_amt::numeric, null,
	       null, null, null, a.dmerc_line_mtus_cnt::numeric, null,
	       a.clm_id, b.bene_id, 'dme_claims_k', c.data_year 
from uthealth/medicare_national.dme_line_k a
  join uthealth/medicare_national.dme_claims_k b
     on a.bene_id = b.bene_id
    and a.clm_id = b.clm_id 
  join data_warehouse.dim_uth_claim_id c 
    on c.data_source = 'mcrt' 
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
								   claim_id_src, member_id_src, table_id_src, data_year )		
	select 'mcrt', c.data_year, c.uth_claim_id, a.clm_line_num::numeric, c.uth_member_id, b.clm_from_dt::date, b.clm_thru_dt::date,
	       d.month_year_id, b.prvdr_num, null, null, b.clm_fac_type_cd, true, true, 
	       null, null, a.hcpcs_cd,a.nch_clm_type_cd,  substring(a.hcpcs_1st_mdfr_cd,1,1),  substring(a.hcpcs_2nd_mdfr_cd,1,1), a.rev_cntr,
	       a.rev_cntr_tot_chrg_amt::numeric, null, null, null, null, null, null, 
	       substring(b.clm_fac_type_cd,1,1), substring(b.clm_srvc_clsfctn_type_cd,1,1), substring(b.clm_freq_cd,1,1), a.rev_cntr_unit_cnt::numeric, null,
	       b.clm_id, b.bene_id, 'hha_revenue_center_k', c.data_year 
from uthealth/medicare_national.hha_revenue_center_k a 
     join uthealth/medicare_national.hha_base_claims_k b
     on a.bene_id = b.bene_id
    and a.clm_id = b.clm_id 
  join data_warehouse.dim_uth_claim_id c 
    on c.data_source = 'mcrt' 
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
								   claim_id_src, member_id_src, table_id_src, data_year )		
	select 'mcrt', c.data_year,c.uth_claim_id, a.clm_line_num::numeric, c.uth_member_id, b.clm_from_dt::date, b.clm_thru_dt::date,
	       d.month_year_id, b.prvdr_num, null, null, b.clm_fac_type_cd, true, true, 
	       null, null, a.hcpcs_cd,a.nch_clm_type_cd,  substring(a.hcpcs_1st_mdfr_cd,1,1),  substring(a.hcpcs_2nd_mdfr_cd,1,1), a.rev_cntr,
	       a.rev_cntr_tot_chrg_amt::numeric, null, null, null, null, null, null, 
	       substring(b.clm_fac_type_cd,1,1), substring(b.clm_srvc_clsfctn_type_cd,1,1), substring(b.clm_freq_cd,1,1), a.rev_cntr_unit_cnt::numeric, null,
	       b.clm_id, b.bene_id, 'hospice_base_claims_k', c.data_year 
from uthealth/medicare_national.hospice_revenue_center_k a 
     join uthealth/medicare_national.hospice_base_claims_k b
     on a.bene_id = b.bene_id
    and a.clm_id = b.clm_id 
  join data_warehouse.dim_uth_claim_id c 
    on c.data_source = 'mcrt' 
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
								   claim_id_src, member_id_src, table_id_src, data_year )		
	select 'mcrt', c.data_year,c.uth_claim_id, a.clm_line_num::numeric, c.uth_member_id, b.clm_from_dt::date, b.clm_thru_dt::date,
	       d.month_year_id, b.prvdr_num, null, null, b.clm_fac_type_cd, true, true, 
	       null, null, a.hcpcs_cd,a.nch_clm_type_cd,  substring(a.hcpcs_1st_mdfr_cd,1,1),  substring(a.hcpcs_2nd_mdfr_cd,1,1), a.rev_cntr,
	       a.rev_cntr_tot_chrg_amt::numeric, null, null, null, null, null, null, 
	       substring(b.clm_fac_type_cd,1,1), substring(b.clm_srvc_clsfctn_type_cd,1,1), substring(b.clm_freq_cd,1,1), a.rev_cntr_unit_cnt::numeric, null,
	       b.clm_id, b.bene_id, 'snf_base_claims_k', c.data_year 
from uthealth/medicare_national.snf_revenue_center_k a 
     join uthealth/medicare_national.snf_base_claims_k b
     on a.bene_id = b.bene_id
    and a.clm_id = b.clm_id 
  join data_warehouse.dim_uth_claim_id c 
    on c.data_source = 'mcrt' 
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id
 join reference_tables.ref_month_year d 
    on d.month_int = extract(month from b.clm_from_dt::date)
   and d.year_int = extract(year from b.clm_from_dt::date)
;


--cleanup
vacuum analyze data_warehouse.claim_detail;

--validate
select count(*), table_id_src, data_year 
from data_warehouse.claim_detail
where data_source = 'mcrt'
group by table_id_src, data_year 
order by table_id_src, data_year 

