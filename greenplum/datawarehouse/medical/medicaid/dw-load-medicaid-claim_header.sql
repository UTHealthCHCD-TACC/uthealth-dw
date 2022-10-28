

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
 *  iperez || 09/30/2022 || added claim id source and member id source to columns
 * ******************************************************************************************************
 *  jwozny || 10/10/2022 || added load_date, provider_type
 * ******************************************************************************************************
 * */


drop table if exists dw_staging.claim_header;

create table dw_staging.claim_header
(like data_warehouse.claim_header including defaults) 
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
distributed by (uth_member_id)
partition by list(data_source)
 (partition optz values ('optz'),
  partition optd values ('optd'),
  partition truv values ('truv'),
  partition mdcd values ('mdcd'),
  partition mcrt values ('mcrt'),
  partition mcrn values ('mcrn')
 )
; 



-----------claims
with cte_pos as ( select max(pos) as pos, icn  
                  from medicaid.clm_detail 
                  group by icn 
) 
insert into dw_staging.claim_header ( 
       data_source, "year", uth_claim_id, uth_member_id, 
       from_date_of_service, claim_type, 
       total_charge_amount, total_allowed_amount, total_paid_amount ,
       fiscal_year, to_date_of_service,
       bill_provider, ref_provider, other_provider, 
       perf_rn_provider, perf_at_provider, perf_op_provider,
       table_id_src, claim_id_src, member_id_src, load_date, provider_type)		
select distinct 'mdcd', 
	   extract(year from h.hdr_frm_dos::date) as year, 
	   c.uth_claim_id, 
	   c.uth_member_id, 
       h.hdr_frm_dos::Date, 
       case when pos.pos = '1' then 'P' else 'F' end as claim_type, 
       h.tot_bill_amt::float ,
       h.tot_alwd_amt::float, 
       h.hdr_pd_amt::float,
       h.year_fy as fiscal_year,
       h.hdr_to_dos::date,
       h.bill_prov_npi as bill_provider, 
       null as ref_provider,  
       null as other_provider,  
       null as perf_rn_provider, 
       h.atd_prov_npi as perf_at_provider, 
       null as perf_op_provider,
       'clm_header' AS table_id_src,
       h.icn as claim_id_src,
       p.pcn as member_id_src,
       current_date as load_date,
       h.hdr_txm_cd 
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

analyze dw_staging.claim_header;


-----------encounters
insert into dw_staging.claim_header ( 
       data_source, "year", uth_claim_id, uth_member_id, 
       from_date_of_service, claim_type, 
       total_charge_amount, total_allowed_amount, total_paid_amount ,
       fiscal_year, to_date_of_service,
       bill_provider, ref_provider, other_provider, perf_rn_provider, perf_at_provider, perf_op_provider,
       table_id_src, claim_id_src, member_id_src, load_date, provider_type)
select distinct 'mdcd', 
       extract(year from h.frm_dos::date) as cal_year, 
       c.uth_claim_id, c.uth_member_id, 
       h.frm_dos::Date, 
       case when h.tx_cd = 'P' then 'P' 
            when h.tx_cd = 'I' then 'F' 
            else null end as claim_type, 
      h.tot_chrg_amt::float ,
      h.mco_pd_amt::float,     
      null::float as total_paid_amount,
      h.year_fy  as fiscal_year,
      h.to_dos::date,
      h.bill_prov_npi as bill_provider, 
      null as ref_provider,  
      null as other_provider,  
      null as perf_rn_provider, 
      h.attd_phy_npi as perf_at_provider, 
      null as perf_op_provider,
      'enc_header' AS table_id_src,
      h.derv_enc as claim_id_src,
      p.mem_id as member_id_src,
      current_date as load_date,
      h.bill_prov_tax_cd 
  from medicaid.enc_header h  
  join medicaid.enc_proc p 
      on h.derv_enc = p.derv_enc       
   join data_warehouse.dim_uth_claim_id c 
      on p.derv_enc = claim_id_src 
     and p.mem_id = member_id_src 
     and c.data_source = 'mdcd' 
;   

vacuum analyze dw_staging.claim_header;

with cte_pos as ( select max(pos) as pos, icn  
                  from medicaid.htw_clm_detail 
                  group by icn 
) 
insert into dw_staging.claim_header ( 
       data_source, "year", uth_claim_id, uth_member_id,
       from_date_of_service, claim_type, 
       total_charge_amount, total_allowed_amount, total_paid_amount ,
       fiscal_year, to_date_of_service,
       bill_provider, ref_provider, other_provider, perf_rn_provider, perf_at_provider, perf_op_provider,
       table_id_src, claim_id_src, member_id_src, load_date, provider_type)
select distinct 'mdcd', 
	   extract(year from h.hdr_frm_dos::date) as year, 
	   c.uth_claim_id, 
	   c.uth_member_id, 
       h.hdr_frm_dos::Date, 
       case when pos.pos = '1' then 'P' else 'F' end as claim_type, 
       h.tot_bill_amt::float ,
       h.tot_alwd_amt::float, 
       h.hdr_pd_amt::float,
       dev.fiscal_year_func(h.hdr_frm_dos::date) as fiscal_year,
       h.hdr_to_dos::date,
       h.bill_prov_npi as bill_provider, 
       null as ref_provider,  
       null as other_provider,  
       null as perf_rn_provider, 
       h.atd_prov_npi as perf_at_provider, 
       null as perf_op_provider,
       'htw_clm_header' AS table_id_src,
       h.icn as claim_id_src,
       p.pcn as member_id_src,
       current_date as load_date,
       h.hdr_txm_cd 
from medicaid.htw_clm_header h  
join medicaid.htw_clm_proc p 
  on h.icn  = p.icn 
   join data_warehouse.dim_uth_claim_id c 
      on p.icn = c.claim_id_src 
     and p.pcn = c.member_id_src 
     and c.data_source = 'mdcd'
    left outer join cte_pos pos 
      on pos.icn = h.icn 
      ;       
     
vacuum analyze dw_staging.claim_header;
alter table dw_staging.claim_header owner to uthealth_dev;
grant select on dw_staging.claim_header to uthealth_analyst;
