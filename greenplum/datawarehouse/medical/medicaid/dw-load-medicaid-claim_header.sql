

/* ******************************************************************************************************
 *  load claim header for medicaid
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 * ****************************************************************************************************** 
 * gmunoz  || 10/20/2021 || adding fiscal year logic
 * ******************************************************************************************************
 * jwozny  || 11/09/2021 || added provider variables, removed old _src variables, pointed at dw_staging
 * ******************************************************************************************************
 * jwozny  || 8/10/2022  || changed pos logic for claim_type in claim detail and changed encounter claim_type to tx_cd
 * ******************************************************************************************************
 * */


--confirm 1 to 1 icn
select count(*), count(distinct icn)
from medicaid.clm_header ch 

select count(*), count(distinct icn)
from medicaid.clm_proc; 


-----------claims
with cte_pos as ( select max(pos) as pos, icn  
                  from medicaid.clm_detail 
                  group by icn 
) 
insert into dw_staging.claim_header ( 
       data_source, "year", uth_claim_id, uth_member_id, uth_admission_id,
       from_date_of_service, claim_type, 
       total_charge_amount, total_allowed_amount, 
       fiscal_year, to_date_of_service,
       bill_provider, ref_provider, other_provider, perf_rn_provider, perf_at_provider, perf_op_provider)		
select 'mdcd', extract(year from h.hdr_frm_dos::date) as cal_year, c.uth_claim_id, c.uth_member_id, null as uth_admission_id,
       h.hdr_frm_dos::Date, case when pos.pos = '1' then 'P' else 'F' end as claim_type, 
       h.tot_bill_amt::float ,h.tot_alwd_amt::float, 
       dev.fiscal_year_func(h.hdr_frm_dos::date) as fiscal_year,
       h.hdr_to_dos::date,
       h.bill_prov_npi as bill_provider, null as ref_provider,  null as other_provider,  
       null as perf_rn_provider, h.atd_prov_npi as perf_at_provider, null as perf_op_provider
from medicaid.clm_header h  
   join medicaid.clm_proc p 
      on h.icn  = p.icn 
   join data_warehouse.dim_uth_claim_id c 
      on p.icn = c.claim_id_src 
     and p.pcn = c.member_id_src 
     and c.data_source = 'mdcd'
   left outer join cte_pos pos 
      on pos.icn = h.icn 
;

select * from medicaid.clm_proc cp where year_fy = 2020

select count(*) from medicaid.enc_header eh 

select count(*) from medicaid.clm_header 



-----------encounters
insert into dw_staging.claim_header ( 
       data_source, "year", uth_claim_id, uth_member_id, uth_admission_id,
       from_date_of_service, claim_type, 
       total_charge_amount, total_allowed_amount, 
       fiscal_year, to_date_of_service,
       bill_provider, ref_provider, other_provider, perf_rn_provider, perf_at_provider, perf_op_provider)		
select 'mdcd', extract(year from h.frm_dos::date) as cal_year, c.uth_claim_id, c.uth_member_id, null as uth_admission_id,
       h.frm_dos::Date, 
       case when h.tx_cd = 'P' then 'P' 
            when h.tx_cd = 'I' then 'F' 
            else null end as claim_type, 
       h.tot_chrg_amt::float ,h.mco_pd_amt::float,     
       dev.fiscal_year_func(h.frm_dos::date) as fiscal_year,
      h.to_dos::date,
      h.bill_prov_npi as bill_provider, null as ref_provider,  null as other_provider,  
      null as perf_rn_provider, h.attd_phy_npi as perf_at_provider, null as perf_op_provider
  from medicaid.enc_header h  
join medicaid.enc_proc p 
      on h.derv_enc = p.derv_enc       
   join data_warehouse.dim_uth_claim_id c 
      on p.derv_enc = claim_id_src 
     and p.mem_id = member_id_src 
     and c.data_source = 'mdcd' 
;   



analyze dw_staging.claim_header;


select data_source, fiscal_year , count(*), count(distinct uth_claim_id)
from dw_staging.claim_header ch 
where data_source = 'mdcd'
group by data_source, fiscal_year 
order by data_source, fiscal_year 
;

select data_source, data_year , count(*), count(distinct uth_claim_id)
from data_warehouse.dim_uth_claim_id duci 
where data_source = 'mdcd'
group by data_source, data_year 
order by data_source, data_year 
;


