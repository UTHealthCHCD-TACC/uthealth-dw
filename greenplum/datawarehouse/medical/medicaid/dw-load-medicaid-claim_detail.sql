
/* ******************************************************************************************************
 *  load claim detail for medicaid
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 * ******************************************************************************************************
 *  jw001  || 9/27/2021 || add discharge status 
 * ****************************************************************************************************** 
 * */






----- claim detail
drop table if exists dev.wc_medicaid_detail ;
create table dev.wc_medicaid_detail 
with(appendonly=true,orientation=column)
as select * from data_warehouse.claim_detail limit 0
distributed by (member_id_src);
;



---claim
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
select 'mdcd', extract(year from a.from_dos) as year, c.uth_claim_id, a.clm_dtl_nbr::int2, c.uth_member_id, 
       a.from_dos, a.to_dos, get_my_from_date(a.from_dos) as month_year, lpad(a.pos,2,'0'), 
       true, true, case when d.adm_dt = '' then null else d.adm_dt::date end , case when d.dis_dt = '' then null else d.dis_dt::date end, a.proc_cd, 
       null, substring(proc_mod_1,1,1), substring(proc_mod_2,1,1), 
       case when isdigit(rev_cd) is false then null 
            when length(rev_cd) > 4 then null 
            else lpad(rev_cd,4,'0') end as revenue_code,  
       a.dtl_bill_amt, a.dtl_alwd_amt, a.dtl_pd_amt,     
       null, null, null, null, 
       substring(b.bill,1,1), substring(b.bill,2,1), substring(b.bill,3,1), 
       null, b.drg, b.icn, b.pcn, 'clm_detail', a.clm_dtl_nbr, 
       null, a.year_fy, null, d.pat_stat_cd 
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



---enc
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
select 'mdcd', extract(year from a.fdos_dt::date), c.uth_claim_id, a.ln_nbr::numeric, c.uth_member_id, 
       a.fdos_dt::date, a.tdos_csl::date, get_my_from_date(a.fdos_dt::date) as month_year, lpad(a.pos,2,'0'),
       true, true, d.adm_dt::date, d.dis_dt::date, a.proc_cd, 
       null,  substring(proc_mod_cd_1,1,1), substring(proc_mod_cd_2,1,1), 
       case when isdigit(rev_cd) is false then null 
            when length(rev_cd) > 4 then null 
            else lpad(rev_cd,4,'0') end as revenue_code,  
       a.sub_chrg_amt::numeric, null, a.dt_pd_amt::numeric, 
       null, null, null, null, 
       substring(b.bill,1,1), substring(b.bill,2,1), substring(b.bill,3,1),  
       a.dt_ln_unt::numeric, b.drg, b.derv_enc, b.mem_id, 'enc_det', null, 
       null, a.year_fy, null, a.pat_stat
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


vacuum analyze dev.wc_medicaid_detail;


select table_id_src, fiscal_year, count(*) 
from dev.wc_medicaid_detail 
group by table_id_src, fiscal_year
order by table_id_src, fiscal_year
;


select count(*), fiscal_year 
from data_warehouse.claim_detail cd 
where data_source = 'mdcd'
group by fiscal_year 
order by fiscal_year 
;


insert into data_warehouse.claim_detail select * from dev.wc_medicaid_detail;

vacuum analyze data_warehouse.claim_detail;
