
/* ******************************************************************************************************
 *  load claim detail for medicaid
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 * ******************************************************************************************************
 *  jw001  || 9/27/2021 || add discharge status 
 * ****************************************************************************************************** 
 *  wc001  || 10/11/2021 || migrate to dw_staging
 * ****************************************************************************************************** 
 *  jw002  || 11/03/2021 || add provider variables 
=======
 *  gmunoz  || 10/25/2021 || adding dev.fiscal_year_func() logic
 * ************************************************************************************************************ 
 *  jw003  || 11/29/2021  || changed discharge status to pat_stat from enc_header for the encounter table
 * ************************************************************************************************************
 * 
 * */



----  // BEGIN SCRIPT 

---create working table in dw_staging 
drop table if exists dw_staging.claim_detail;

create table dw_staging.claim_detail (
	data_source bpchar(4),
	"year" int2,
	uth_member_id int8,
	uth_claim_id numeric,
	claim_sequence_number int4,
	from_date_of_service date,
	to_date_of_service date,
	month_year_id int4,
	place_of_service text,
	network_ind bool,
	network_paid_ind bool,
	admit_date date,
	discharge_date date,
	discharge_status bpchar(2),
	cpt_hcpcs_cd text,
	procedure_type text,
	proc_mod_1 bpchar(2),
	proc_mod_2 bpchar(2),
	drg_cd text,
	revenue_cd bpchar(4),
	charge_amount numeric(13,2),
	allowed_amount numeric(13,2),
	paid_amount numeric(13,2),
	copay numeric(13,2),
	deductible numeric(13,2),
	coins numeric(13,2),
	cob numeric(13,2),
	bill_type_inst bpchar(1),
	bill_type_class bpchar(1),
	bill_type_freq bpchar(1),
	units int4,
	fiscal_year int2,
	cost_factor_year int2,
	table_id_src text,
	claim_sequence_number_src text,
	row_id bigserial
	) 
with(appendonly=true,orientation=column, compresstype=zlib, compresslevel=5)
distributed by (row_id);
;

alter sequence dw_staging.claim_detail_row_id_seq cache 200;



-------------insert existing records from data warehouse. except for this data source
insert into dw_staging.claim_detail 
select * from data_warehouse.claim_detail 
where data_source not in ('mdcd')
;

vacuum analyze dw_staging.claim_detail;


---claim
insert into dw_staging.claim_detail ( data_source, year, uth_claim_id, claim_sequence_number, uth_member_id, 
                                     from_date_of_service, to_date_of_service, month_year_id, place_of_service,
                                     network_ind, network_paid_ind, admit_date, discharge_date, cpt_hcpcs_cd, 
                                     procedure_type, proc_mod_1, proc_mod_2, revenue_cd, 
                                     charge_amount, allowed_amount, paid_amount, 
                                     copay, deductible, coins, cob, 
                                     bill_type_inst, bill_type_class, bill_type_freq, 
                                     units, drg_cd,  claim_sequence_number_src, 
                                     fiscal_year, cost_factor_year, discharge_status,
                                      bill_provider, ref_provider, other_provider, perf_rn_provider, perf_at_provider, perf_op_provider
                                     )                                          
select 'mdcd', extract(year from a.from_dos) as year, c.uth_claim_id, null, c.uth_member_id, 
       a.from_dos, a.to_dos, get_my_from_date(a.from_dos) as month_year, trim(a.pos), 
       true, true, case when d.adm_dt = '' then null else d.adm_dt::date end , case when d.dis_dt = '' then null else d.dis_dt::date end, a.proc_cd, 
       null, proc_mod_1, proc_mod_2, 
       case when isdigit(rev_cd) is false then null 
            when length(rev_cd) > 4 then null 
            else lpad(rev_cd,4,'0') end as revenue_code,  
       a.dtl_bill_amt, a.dtl_alwd_amt, a.dtl_pd_amt,     
       null, null, null, null, 
       substring(b.bill,1,1), substring(b.bill,2,1), substring(b.bill,3,1), 
       null, b.drg, a.clm_dtl_nbr, 
       dev.fiscal_year_func(a.from_dos),
       null, 
       d.pat_stat_cd,
       null as bill_provider, a.ref_prov_npi as ref_provider, null as other_provider, 
       a.perf_prov_npi as perf_rn_provider, null as perf_at_provider, null as perf_op_provider
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


select * 
from dw_staging.claim_detail cd 
where data_source = 'mdcd' 
  and year = 2019;

where year_fy = 2019
;



---enc  20min
insert into dw_staging.claim_detail ( data_source, year, uth_claim_id, claim_sequence_number, uth_member_id, 
                                     from_date_of_service, to_date_of_service, month_year_id, place_of_service,
                                     network_ind, network_paid_ind, admit_date, discharge_date, cpt_hcpcs_cd, 
                                     procedure_type, proc_mod_1, proc_mod_2, 
                                     revenue_cd, 
                                     charge_amount, allowed_amount, paid_amount, 
                                     copay, deductible, coins, cob, 
                                     bill_type_inst, bill_type_class, bill_type_freq, 
                                     units, drg_cd, claim_sequence_number_src, 
                                     fiscal_year, cost_factor_year, discharge_status,
                                     bill_provider, ref_provider, other_provider, perf_rn_provider, perf_at_provider, perf_op_provider
                                     )                                      
select 'mdcd', extract(year from a.fdos_dt::date), c.uth_claim_id, null, c.uth_member_id, 
       a.fdos_dt::date, a.tdos_csl::date, get_my_from_date(a.fdos_dt::date) as month_year, trim(a.pos),
       true, true, d.adm_dt::date, d.dis_dt::date, a.proc_cd, 
       null,  proc_mod_cd_1, proc_mod_cd_2,
       case when isdigit(rev_cd) is false then null 
            when length(rev_cd) > 4 then null 
            else lpad(rev_cd,4,'0') end as revenue_code,  
       a.sub_chrg_amt::numeric, null, a.dt_pd_amt::numeric, 
       null, null, null, null, 
       substring(b.bill,1,1), substring(b.bill,2,1), substring(b.bill,3,1),  
       a.dt_ln_unt::numeric, b.drg, a.ln_nbr,
       dev.fiscal_year_func(a.fdos_dt::date),
       null, 
       d.pat_stat,
              null as bill_provider, a.sub_ref_prov_npi as ref_provider, null as other_provider, 
       a.sub_rend_prov_npi as perf_rn_provider, null as perf_at_provider, a.sub_opt_phy_npi as perf_op_provider
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


---finalize 12min
vacuum analyze dw_staging.claim_detail;

---validate
select count(*), fiscal_year 
from dw_staging.claim_detail
where data_source = 'mdcd'
group by fiscal_year 
order by fiscal_year 
;

----------------- END SCRIPT 