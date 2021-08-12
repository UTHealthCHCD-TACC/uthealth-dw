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
       table_id_src, bill_type, fiscal_year, to_date_of_service)
select 'mdcd', extract(year from h.hdr_frm_dos::date) as cal_year, c.uth_claim_id, c.uth_member_id, 
       h.hdr_frm_dos::Date, case when pos.pos <> '' then 'P' else 'F' end as claim_type, 
       h.tot_bill_amt::float ,h.tot_alwd_amt::float, h.icn , p.pcn, 
       'clm_header' as tableidsrc, h.bill_prov_ty_cd, h.year_fy, h.hdr_to_dos::date
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
       table_id_src, bill_type, fiscal_year, to_date_of_service)
select 'mdcd', extract(year from h.frm_dos::date) as cal_year, c.uth_claim_id, c.uth_member_id, 
       h.frm_dos::Date, 
       case when pos.pos <> '' then 'P' else 'F' end as claim_type, 
       h.tot_chrg_amt::float ,h.mco_pd_amt::float, h.derv_enc , p.mem_id,
       'enc_header' as tableidsrc, h.bill_prov_typ_cd, h.year_fy, h.to_dos::date       
  from medicaid.enc_header_new h  
join medicaid.enc_proc_new p 
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

----- claim detail
drop table if exists dev.wc_medicaid_detail ;
create table dev.wc_medicaid_detail 
with(appendonly=true,orientation=column)
as select * from data_warehouse.claim_detail limit 0
distributed by (member_id_src);
;

insert into dev.wc_medicaid_detail ( data_source, year, uth_claim_id, claim_sequence_number, uth_member_id, 
                                     from_date_of_service, to_date_of_service, month_year_id, place_of_service,
                                     network_ind, network_paid_ind, admit_date, discharge_date, cpt_hcpcs, 
                                     procedure_type, proc_mod_1, proc_mod_2, revenue_cd, 
                                     charge_amount, allowed_amount, paid_amount, 
                                     copay, deductible, coins, cob, 
                                     bill_type_inst, bill_type_class, bill_type_freq, 
                                     units, drg_cd, claim_id_src, member_id_src, table_id_src, claim_sequence_number_src, 
                                     cob_type, fiscal_year, cost_factor_year, discharge_status 
                                     )
                                     
           select 'mdcd', *
           from medicaid.clm_detail a 
              join data_warehouse.dim_uth_claim_id b 
                 on a.icn = b.claim_id_src 
                and 
           
           
           
 ;


select count(*), year_fy 
from 

---for healthy texas women htw - up through 2017 they gave us a flag. for 2018 and forward they broke 
--- htw out into seperate tables

select icn 
	    ,unnest(array[d.prim_dx_cd, d.dx_cd_1, d.dx_cd_2, d.dx_cd_3, d.dx_cd_4, d.dx_cd_5, d.dx_cd_6, 
	                  d.dx_cd_7, d.dx_cd_8, d.dx_cd_9, d.dx_cd_10, d.dx_cd_11, d.dx_cd_12, d.dx_cd_13, 
	                  d.dx_cd_14, d.dx_cd_15, d.dx_cd_16, d.dx_cd_17, d.dx_cd_18, d.dx_cd_19,
	                  d.dx_cd_20, d.dx_cd_21, d.dx_cd_22, d.dx_cd_23, d.dx_cd_24, d.dx_cd_25 ]) as dx_cd
		,unnest(array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25]) as dx_pos 
		,*
from medicaid.clm_dx d;
