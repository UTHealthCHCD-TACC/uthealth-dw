---medicaid claim_header load script


--confirm 1 to 1 icn
select count(*), count(distinct icn)
from medicaid.clm_header ch 

select count(*), count(distinct icn)
from medicaid.clm_proc; 


delete from data_warehouse.claim_header where data_source = 'mdcd';

-----------claims
with cte_pos as ( select max(pos) as pos, icn  
                  from medicaid.clm_detail 
                  group by icn 
) 
insert into data_warehouse.claim_header ( 
       data_source, "year", uth_claim_id, uth_member_id, 
       from_date_of_service, claim_type, 
       total_charge_amount, total_allowed_amount, claim_id_src, member_id_src, 
       table_id_src, fiscal_year, to_date_of_service)
select 'mdcd', extract(year from h.hdr_frm_dos::date) as cal_year, c.uth_claim_id, c.uth_member_id, 
       h.hdr_frm_dos::Date, case when pos.pos <> '' then 'P' else 'F' end as claim_type, 
       h.tot_bill_amt::float ,h.tot_alwd_amt::float, h.icn , p.pcn, 
       'clm_header' as tableidsrc,  h.year_fy, h.hdr_to_dos::date
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
with cte_pos as ( 
select max(pos) as pos, derv_enc  
                  from medicaid.enc_det 
                  group by derv_enc                                     
) 
insert into data_warehouse.claim_header ( 
       data_source, "year", uth_claim_id, uth_member_id, 
       from_date_of_service, claim_type, 
       total_charge_amount, total_allowed_amount, claim_id_src, member_id_src, 
       table_id_src,  fiscal_year, to_date_of_service)
select 'mdcd', extract(year from h.frm_dos::date) as cal_year, c.uth_claim_id, c.uth_member_id, 
       h.frm_dos::Date, 
       case when pos.pos <> '' then 'P' else 'F' end as claim_type, 
       h.tot_chrg_amt::float ,h.mco_pd_amt::float, h.derv_enc , p.mem_id,
       'enc_header' as tableidsrc, h.year_fy, h.to_dos::date       
  from medicaid.enc_header h  
join medicaid.enc_proc p 
      on h.derv_enc = p.derv_enc       
   join data_warehouse.dim_uth_claim_id c 
      on p.derv_enc = claim_id_src 
     and p.mem_id = member_id_src 
     and c.data_source = 'mdcd' 
   left outer join cte_pos pos 
      on pos.derv_enc = h.derv_enc 
;   



vacuum analyze data_warehouse.claim_header;


select data_source, fiscal_year , count(*), count(distinct uth_claim_id)
from data_warehouse.claim_header ch 
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


