
 /* ******************************************************************************************************
 * Author || Date      || Notes
 * ******************************************************************************************************
 * various authors  || <09/05/2023 || created
 * ******************************************************************************************************
 * xzhang  			|| 09/05/2023 || Changed table name from claim_detail to mcd_claim_detail
 * 									 Changed unit (dt_ln_unt) to float instead of int
 * xzhang			|| 11/16/2023 || Modified to include line_status (paid/denied/etc on line level)
 * 									 Also rolled extraneous update statements into create table statements
*/

drop table if exists dw_staging.enc_detail_etl;

CREATE TABLE dw_staging.enc_detail_etl (
    year_fy int null,
	ln_nbr varchar NULL,
	enc_stat_cd varchar NULL,
	fdos_dt varchar NULL,
	tdos_csl varchar NULL,
	proc_cd varchar NULL,
	sub_chrg_amt varchar NULL,
	dt_pd_amt varchar NULL,
	dt_ln_unt varchar NULL,
	proc_mod_cd_1 varchar NULL,
	proc_mod_cd_2 varchar NULL,
	pos varchar NULL,
	rev_cd varchar NULL,
	sub_opt_phy_npi varchar NULL,
	sub_ref_prov_npi varchar NULL,
	sub_rend_prov_npi varchar NULL,
	sub_rend_prv_tax_cd varchar NULL,
	derv_enc varchar NULL
)
WITH (
	appendonly=true,
	orientation=row,
	checksum=true,
	compresslevel=5,
	compresstype=zlib
)
DISTRIBUTED BY (derv_enc);

----------------------------------------------


insert into dw_staging.enc_detail_etl
select 
	year_fy,
    case when trim(ln_nbr) = '' then null else trim(ln_nbr) end,
    case when trim(enc_stat_cd) = '' then null else trim(enc_stat_cd) end,
    case when trim(fdos_dt) = '' then null else trim(fdos_dt) end,
    case when trim(tdos_csl) = '' then null else trim(tdos_csl) end,
    case when trim(proc_cd) = '' then null else trim(proc_cd) end,
    case when trim(sub_chrg_amt) = '' then null else trim(sub_chrg_amt) end,
    case when trim(dt_pd_amt) = '' then null else trim(dt_pd_amt) end,
    case when trim(dt_ln_unt) = '' then null else trim(dt_ln_unt) end,
    case when trim(proc_mod_cd_1) = '' then null else trim(proc_mod_cd_1) end,
    case when trim(proc_mod_cd_2) = '' then null else trim(proc_mod_cd_2) end,
    case when trim(pos) = '' then null else trim(pos) end,
    case when trim(rev_cd) = '' then null else trim(rev_cd) end,
    case when trim(sub_opt_phy_npi) = '' then null else trim(sub_opt_phy_npi) end,
    case when trim(sub_ref_prov_npi) = '' then null else trim(sub_ref_prov_npi) end,
    case when trim(sub_rend_prov_npi) = '' then null else trim(sub_rend_prov_npi) end,
    case when trim(sub_rend_prv_tax_cd) = '' then null else trim(sub_rend_prv_tax_cd) end,
    case when trim(derv_enc) = '' then null else trim(derv_enc) end
from medicaid.enc_det ;

analyze dw_staging.enc_detail_etl;
   
update dw_staging.enc_detail_etl
   set rev_cd = 
   		case when length(rev_cd) = 4 then rev_cd
   		     when length(rev_cd) = 3 then lpad(rev_cd,4,'0')
   		     else null end,
   	   proc_cd = 
	   	   case when length(proc_cd) = 5 then proc_cd 
	   	   else null end
;
   		    
vacuum analyze dw_staging.enc_detail_etl;


---------------------------etl enc header-----------------------------

drop table if exists dw_staging.enc_header_etl;

CREATE TABLE dw_staging.enc_header_etl (
	adm_dt date NULL,
	dis_dt date NULL,
	pat_stat varchar NULL,
	derv_enc varchar NULL
)
WITH (
	appendonly=true,
	orientation=column,
	checksum=true,
	compresslevel=5,
	compresstype=zlib
)
DISTRIBUTED BY (derv_enc);

insert into dw_staging.enc_header_etl
select 
	adm_dt,
	dis_dt,
	case when trim(pat_stat) = '' then null else trim(pat_stat) end,
	trim(derv_enc)
from medicaid.enc_header;
  
vacuum analyze dw_staging.enc_header_etl;
 
---------------------------etl enc proc-----------------------------

drop table if exists dw_staging.enc_proc_etl;

CREATE TABLE dw_staging.enc_proc_etl (
		mem_id varchar null,
		bill varchar null,
		derv_enc varchar null,
		drg varchar null
)
WITH (
	appendonly=true,
	orientation=column,
	checksum=true,
	compresslevel=5,
	compresstype=zlib
)
DISTRIBUTED BY (derv_enc);

insert into dw_staging.enc_proc_etl
select 
	trim(mem_id),
    case when trim(bill) = '' then null else trim(bill) end,
	trim(derv_enc),
    case when trim(drg) = '' then null else trim(drg) end
from medicaid.enc_proc ;
      
vacuum analyze dw_staging.enc_proc_etl;

---------------------------------------------------------------------

drop table if exists dw_staging.detail_enc_etl;

CREATE TABLE dw_staging.detail_enc_etl (
	year_fy int null,
	derv_enc varchar null,
	mem_id varchar null,
	adm_dt date null, 
	dis_dt date null, 
	ln_nbr varchar null,
	enc_stat_cd varchar null,
	pat_stat varchar null,
	fdos_dt date null, 
	tdos_csl date null, 
	proc_cd varchar null,
	sub_chrg_amt numeric null,
	dt_pd_amt numeric null,
	dt_ln_unt float null,
	proc_mod_cd_1 varchar null,
	proc_mod_cd_2 varchar null,
	pos varchar null,
	rev_cd varchar null,
	sub_opt_phy_npi varchar null,
	sub_ref_prov_npi varchar null,
	sub_rend_prov_npi varchar null,
	sub_rend_prv_tax_cd varchar null,
	bill_i varchar null,
	bill_c varchar null,
	bill_f varchar null,
	bill varchar null,
	drg varchar null
)
WITH (
	appendonly=true,
	orientation=row,
	compresstype=zlib
)
DISTRIBUTED BY (derv_enc);

insert into dw_staging.detail_enc_etl
select 
	d.year_fy,
	h.derv_enc,
	p.mem_id,
	h.adm_dt,
	h.dis_dt,
	d.ln_nbr,
	d.enc_stat_cd,
	h.pat_stat,
	d.fdos_dt::date,
	d.tdos_csl::date,
	d.proc_cd,
	d.sub_chrg_amt::numeric,
	d.dt_pd_amt::numeric,
	d.dt_ln_unt::numeric,
	d.proc_mod_cd_1,
	d.proc_mod_cd_2,
	d.pos,
	d.rev_cd,
	d.sub_opt_phy_npi,
	d.sub_ref_prov_npi,
	d.sub_rend_prov_npi,
	d.sub_rend_prv_tax_cd,
	p.bill,
	p.drg
from dw_staging.enc_header_etl h 
join dw_staging.enc_proc_etl p 
on h.derv_enc  = p.derv_enc 
join dw_staging.enc_detail_etl d 
on d.derv_enc = h.derv_enc 
;

analyze dw_staging.detail_enc_etl;

--select * from dw_staging.detail_enc_etl;

insert into dw_staging.mcd_claim_detail
select distinct 
    'mdcd' as data_source,
	extract(year from a.fdos_dt) as year,
	b.uth_member_id as uth_member_id,
	b.uth_claim_id as uth_claim_id,
	a.ln_nbr::int as claim_sequence_number,
	a.fdos_dt as from_date_of_service,
	a.tdos_csl as to_date_of_service,
	get_my_from_date(a.fdos_dt) as month_year_id,
	a.pos as place_of_service,
	true as network_ind,
	true as network_paid_ind,
	a.adm_dt as admit_date,
	a.dis_dt as discharge_date,
	a.pat_stat as discharge_status,
	a.proc_cd as cpt_hcpcs_cd,
	null as procedure_type,
	a.proc_mod_cd_1 as proc_mod_1,
	a.proc_mod_cd_2 as proc_mod_2,
	a.drg as drg_cd,
	a.rev_cd as revenue_cd,
	a.sub_chrg_amt as charge_amount,
	a.dt_pd_amt as allowed_amount,
	null::numeric as paid_amount,
	null::numeric as copay,
	null::numeric as deductible,
	null::numeric as coins,
	null::numeric as cob,
	substring(a.bill,1,1) as bill_type_inst,
	substring(a.bill,2,1) as bill_type_class,
	substring(a.bill,3,1) as bill_type_freq,
	dt_ln_unt as units,
	a.year_fy  as fiscal_year,
	null::int as cost_factor_year,
	'enc_det' as table_id_src,
	null as bill_provider,
	a.sub_ref_prov_npi as ref_provider,
	null as other_provider,
	a.sub_rend_prov_npi as perf_rn_provider,
	null as perf_at_provider,
	sub_opt_phy_npi as perf_op_provider,
	a.derv_enc as claim_id_src,
	a.mem_id as member_id_src,
	current_date as load_date,
	a.sub_rend_prv_tax_cd as provider_type,
	a.bill as bill,
	a.enc_stat_cd as line_status
from dw_staging.detail_enc_etl a 
join data_warehouse.dim_uth_claim_id b 
  on a.mem_id = b.member_id_src 
 and a.derv_enc = b.claim_id_src 
 and b.data_source in ('mdcd', 'mhtw', 'mcpp')
;

vacuum analyze dw_staging.mcd_claim_detail;

drop table if exists dw_staging.enc_detail_etl;
drop table if exists dw_staging.enc_header_etl;
drop table if exists dw_staging.enc_proc_etl;
drop table if exists dw_staging.detail_enc_etl;
