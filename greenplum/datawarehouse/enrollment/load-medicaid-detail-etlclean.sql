drop table if exists dw_staging.claim_detail_etl;

create table dw_staging.claim_detail_etl as (
select 'mdcd' as data_source, 
	   extract(year from a.from_dos) as year, 
       a.from_dos, 
       a.to_dos, 
       get_my_from_date(a.from_dos) as month_year, 
       trim(a.pos) as pos,
       case when d.adm_dt = '' then null else d.adm_dt::date end, 
       case when d.dis_dt = '' then null else d.dis_dt::date end,
       trim(a.proc_cd) as proc_cd,
       trim(a.rev_cd) as rev_cd,
       trim(a.sub_proc_cd) as sub_proc_cd,
	   trim(proc_mod_1) as proc_mod_1, 
	   trim(proc_mod_2) as proc_mod_2,
       a.dtl_bill_amt as charge,
       a.dtl_alwd_amt as allowed,
       a.dtl_pd_amt as paid,
       substring(b.bill,1,1) as bill_inst, 
       substring(b.bill,2,1) as bill_class, 
       substring(b.bill,3,1) as bill_freq,
       null::numeric as units,
       trim(b.drg) as drg, 
       trim(a.clm_dtl_nbr) as claim_line_number,
       dev.fiscal_year_func(a.from_dos) as fiscal_year, 
       trim(d.pat_stat_cd) as pat_stat_cd ,
       null::text as bill_provider, 
       trim(a.ref_prov_npi) as ref_provider, 
       null::text as other_provider,
       trim(a.perf_prov_npi) as perf_rn_provider, 
       null::text as perf_at_provider, 
       null::text as perf_op_provider,
       trim(a.ref_prov_npi) as ref_prov_npi , 
       trim(a.perf_prov_npi) as  perf_prov_npi, 
       'clm_detail'::text as table_src,
       a.icn::text as claim_id_src,
       b.pcn::text as member_id_src, 
       trim(a.txm_cd) as txm_cd 
  from medicaid.clm_detail a
  join medicaid.clm_proc b
    on b.icn  = a.icn
  join medicaid.clm_header d
    on d.icn = b.icn
    ) distributed by (claim_id_src)
;

vacuum analyze dw_staging.claim_detail_etl;

insert into dw_staging.claim_detail_etl
select 'mdcd', 
	   extract(year from a.fdos_dt::date), 
       a.fdos_dt::date as from_dos, 
       a.tdos_csl::date as to_dos, 
       get_my_from_date(a.fdos_dt::date) as month_year, 
       trim(a.pos) as pos,
       d.adm_dt::date, 
       d.dis_dt::date, 
       trim(a.proc_cd) as proc_cd,
       trim(a.rev_cd) as rev_cd,
       null as sub_proc_cd,
       trim(proc_mod_cd_1), 
       trim(proc_mod_cd_2),
       a.sub_chrg_amt::numeric, 
       a.dt_pd_amt::numeric, 
       null as paid,
       substring(b.bill,1,1), 
       substring(b.bill,2,1), 
       substring(b.bill,3,1),
       a.dt_ln_unt::numeric, 
       trim(b.drg) as drg, 
       trim(a.ln_nbr) as claim_line_number,
       dev.fiscal_year_func(a.fdos_dt::date) as fiscal_year, 
       trim(d.pat_stat) as pat_stat_cd,
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
    join medicaid.enc_header d
      on d.derv_enc = b.derv_enc
      ;

vacuum analyze dw_staging.claim_detail_etl;











