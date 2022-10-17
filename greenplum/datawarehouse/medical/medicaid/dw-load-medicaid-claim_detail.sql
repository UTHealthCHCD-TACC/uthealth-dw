
/* ******************************************************************************************************
 *  load claim detail for medicaid
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 * ******************************************************************************************************
 *  jwozny  || 9/27/2021 || add discharge status
 * ******************************************************************************************************
 *  wc001  || 10/11/2021 || migrate to dw_staging
 * ******************************************************************************************************
 *  jwozny  || 11/03/2021 || add provider variables
 * ********************************************************************************************************
 *  gmunoz  || 10/25/2021 || adding dev.fiscal_year_func() logic
 * ************************************************************************************************************
 *  jwozny  || 11/29/2021  || changed discharge status to pat_stat from enc_header for the encounter table + added logic for procedure_type
 * ************************************************************************************************************
 *  jwozny || 01/04/2022  || added case when to logic for cpt_hcpcs_cd to fix bad proc_cd values (they were revenue codes) in raw data. only a problem in clm_detail
 * **********************************************************************************************
 *  jwozny || 02/18/2022  || added table_id_src values
 * **********************************************************************************************
 *  iperez  || 09/30/2022 || added claim id source and member id source to columns
 * ******************************************************************************************************
*/



----  // BEGIN SCRIPT
do $$
begin

---claim
insert into dw_staging.claim_detail ( 
data_source, year, uth_claim_id, claim_sequence_number, uth_member_id,
 from_date_of_service, to_date_of_service, month_year_id, place_of_service,
 network_ind, network_paid_ind, admit_date, discharge_date, cpt_hcpcs_cd,
 procedure_type,
 proc_mod_1, proc_mod_2, revenue_cd,
 charge_amount, allowed_amount, paid_amount,
 copay, deductible, coins, cob,
 bill_type_inst, bill_type_class, bill_type_freq,
 units, drg_cd,  claim_sequence_number_src,
 fiscal_year, cost_factor_year, discharge_status,
 bill_provider, ref_provider, other_provider, perf_rn_provider, perf_at_provider, perf_op_provider,
 table_id_src,
 claim_id_src,
 member_id_src,
 load_date,
 provider_type
)
select 'mdcd', 
	   extract(year from a.from_dos) as year, 
	   c.uth_claim_id, 
	   null, 
	   c.uth_member_id,
       a.from_dos, 
       a.to_dos, 
       get_my_from_date(a.from_dos) as month_year, 
       trim(a.pos),
       true, 
       true, 
       case when d.adm_dt = '' then null else d.adm_dt::date end , 
       case when d.dis_dt = '' then null else d.dis_dt::date end, 
       case 
		   when a.proc_cd  ~ '[a-zA-Z0-9]{5}$' then a.proc_cd 
		   when lpad(a.proc_cd,4,'0') = lpad(a.rev_cd,4,'0') 
		    and a.sub_proc_cd ~ '[a-zA-Z0-9]{5}$' then a.sub_proc_cd 
		   when a.proc_cd = '' 
		    and a.sub_proc_cd ~ '[a-zA-Z0-9]{5}$' then a.sub_proc_cd 
		   when a.proc_cd <> rev_cd 
		    and a.proc_cd <> '' 
		    and a.proc_cd  !~ '[a-zA-Z0-9]{5}$' 
		    and a.sub_proc_cd ~ '[a-zA-Z0-9]{5}$' then a.sub_proc_cd
		   when lpad(a.proc_cd,4,'0') = lpad(a.rev_cd,4,'0') 
		    and a.rev_cd <> '' then null 
		   when a.proc_cd = '' 
		    and a.rev_cd = '' 
		    and a.sub_proc_cd ~ '[a-zA-Z0-9]{5}$' then a.sub_proc_cd 
	   end as cpt_hcpcs_cd,
	   null as procedure_type,
	   proc_mod_1, proc_mod_2,
       case 
	   	when a.rev_cd = '' or rev_cd ~ '[a-zA-Z0-9]{5}$' then null 
	   	else lpad(a.rev_cd,4,'0')
	   end as revenue_cd,
       a.dtl_bill_amt, a.dtl_alwd_amt, a.dtl_pd_amt,
       null, null, null, null,
       substring(b.bill,1,1), 
       substring(b.bill,2,1), 
       substring(b.bill,3,1),
       null, b.drg, a.clm_dtl_nbr,
       dev.fiscal_year_func(a.from_dos), 
       null, 
       d.pat_stat_cd,
       null as bill_provider, 
       a.ref_prov_npi as ref_provider, 
       null as other_provider,
       a.perf_prov_npi as perf_rn_provider, 
       null as perf_at_provider, 
       null as perf_op_provider,
       'clm_detail',
       b.icn as claim_id_src,
       b.pcn as member_id_src,
       current_date as load_date,
       a.txm_cd
from medicaid.clm_detail a
	join medicaid.clm_proc b
      on b.icn  = a.icn
     and b.year_fy = a.year_fy
    join data_warehouse.dim_uth_claim_id c
      on c.member_id_src = b.pcn
      and c.claim_id_src = a.icn
    join medicaid.clm_header d
      on d.icn = b.icn
     and d.year_fy = b.year_fy
;

raise notice 'clm detail done %', clock_timestamp();





---enc  20min
insert into dw_staging.claim_detail ( 
data_source, year, uth_claim_id, claim_sequence_number, uth_member_id,
 from_date_of_service, to_date_of_service, month_year_id, place_of_service,
 network_ind, network_paid_ind, admit_date, discharge_date, cpt_hcpcs_cd,
 procedure_type, proc_mod_1, proc_mod_2,
 revenue_cd,
 charge_amount, allowed_amount, paid_amount,
 copay, deductible, coins, cob,
 bill_type_inst, bill_type_class, bill_type_freq,
 units, drg_cd, claim_sequence_number_src,
 fiscal_year, cost_factor_year, discharge_status,
 bill_provider, ref_provider, other_provider, perf_rn_provider, perf_at_provider, perf_op_provider,
 table_id_src,
 claim_id_src,
 member_id_src,
 load_date,
 provider_type
)
select 'mdcd', 
	   extract(year from a.fdos_dt::date), 
       c.uth_claim_id, 
       null, 
       c.uth_member_id,
       a.fdos_dt::date, 
       a.tdos_csl::date, 
       get_my_from_date(a.fdos_dt::date) as month_year, 
       trim(a.pos),
       true, 
       true, 
       d.adm_dt::date, 
       d.dis_dt::date, 
       case 
	       when a.proc_cd ~ '[a-zA-Z0-9]{5}$' then a.proc_cd 
	   end as cpt_hcpcs_cd,
       null as procedure_type,
       proc_mod_cd_1, proc_mod_cd_2,
       case 
	   	when a.rev_cd = '' or rev_cd ~ '[a-zA-Z0-9]{5}$' then null 
	   	else lpad(a.rev_cd,4,'0')
	   end as revenue_cd,
       a.sub_chrg_amt::numeric, 
       a.dt_pd_amt::numeric, 
       null AS paid_amount,
       null, null, null, null,
       substring(b.bill,1,1), 
       substring(b.bill,2,1), 
       substring(b.bill,3,1),
       a.dt_ln_unt::numeric, 
       b.drg, 
       a.ln_nbr,
       dev.fiscal_year_func(a.fdos_dt::date), 
       null, 
       d.pat_stat,
       null as bill_provider, 
       a.sub_ref_prov_npi as ref_provider, 
       null as other_provider,
       a.sub_rend_prov_npi as perf_rn_provider, 
       null as perf_at_provider, 
       a.sub_opt_phy_npi as perf_op_provider,
       'enc_det',
       b.derv_enc as claim_id_src,
       b.mem_id as member_id_src,
       current_date as load_date,
       a.sub_rend_prv_tax_cd
from medicaid.enc_det a
	join medicaid.enc_proc b
      on b.derv_enc  = a.derv_enc
     and b.year_fy = a.year_fy
    join data_warehouse.dim_uth_claim_id c
      on c.member_id_src = b.mem_id
      and c.claim_id_src = b.derv_enc
    join medicaid.enc_header d
      on d.derv_enc = b.derv_enc
     and d.year_fy = b.year_fy
;

raise notice 'enc det done %', clock_timestamp();

---finalize 12min
analyze dw_staging.claim_detail;

raise notice 'done %', clock_timestamp();

end $$
;

---validate
select count(*), data_source, year
from dw_staging.claim_detail
group by 2,3
order by 2 ,3
;

----------------- END SCRIPT
        /*case when substring(proc_cd,1,1) ~ '[0-9]' 
        			then 'CPT'
	   	     when substring(proc_cd,1,1) ~ '[a-zA-Z]' 
	   	     		then 'HCPCS' else null */
