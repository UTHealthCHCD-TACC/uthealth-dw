
drop table if exists dw_staging.enc_detail_etl;

CREATE TABLE dw_staging.enc_detail_etl (
	ln_nbr varchar NULL,
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
trim(ln_nbr),
trim(fdos_dt),
trim(tdos_csl),
trim(proc_cd),
trim(sub_chrg_amt),
trim(dt_pd_amt),
trim(dt_ln_unt),
trim(proc_mod_cd_1),
trim(proc_mod_cd_2),
trim(pos),
trim(rev_cd),
trim(sub_opt_phy_npi),
trim(sub_ref_prov_npi),
trim(sub_rend_prov_npi),
trim(sub_rend_prv_tax_cd),
trim(derv_enc)
from medicaid.enc_det ;

analyze dw_staging.enc_detail_etl;

update dw_staging.enc_detail_etl
   set ln_nbr = case when ln_nbr = '' then null else ln_nbr end,
    fdos_dt = case when fdos_dt = '' then null else fdos_dt end,
    tdos_csl = case when tdos_csl = '' then null else tdos_csl end,
    proc_cd = case when proc_cd = '' then null else proc_cd end,
    sub_chrg_amt = case when sub_chrg_amt = '' then null else sub_chrg_amt end,
    dt_pd_amt = case when dt_pd_amt = '' then null else dt_pd_amt end,
    dt_ln_unt = case when dt_ln_unt = '' then null else dt_ln_unt end,
    proc_mod_cd_1 = case when proc_mod_cd_1 = '' then null else proc_mod_cd_1 end,
    proc_mod_cd_2 = case when proc_mod_cd_2 = '' then null else proc_mod_cd_2 end,
    pos = case when pos = '' then null else pos end,
    rev_cd = case when rev_cd = '' then null else rev_cd end,
    sub_opt_phy_npi = case when sub_opt_phy_npi = '' then null else sub_opt_phy_npi end,
    sub_ref_prov_npi = case when sub_ref_prov_npi = '' then null else sub_ref_prov_npi end,
    sub_rend_prov_npi = case when sub_rend_prov_npi = '' then null else sub_rend_prov_npi end,
    sub_rend_prv_tax_cd = case when sub_rend_prv_tax_cd = '' then null else sub_rend_prv_tax_cd end,
    derv_enc = case when derv_enc = '' then null else derv_enc end;
   
   
update dw_staging.enc_detail_etl
   set rev_cd = 
   		case when length(rev_cd) = 4 then rev_cd
   		     when length(rev_cd) = 3 then lpad(rev_cd,4,'0')
   		     else null end ;
   		     
update dw_staging.enc_detail_etl
   set proc_cd = 
   	   case when length(proc_cd) = 5 then proc_cd 
   	   else null end ;
   		    
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
trim(pat_stat),
trim(derv_enc)
from medicaid.enc_header;

analyze dw_staging.enc_header_etl;

update dw_staging.enc_header_etl
   set pat_stat = case when pat_stat = '' then null else pat_stat end;
  
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
trim(bill),
trim(derv_enc),
trim(drg)
from medicaid.enc_proc ;

analyze dw_staging.enc_proc_etl;

update dw_staging.enc_proc_etl
   set bill = case when bill = '' then null else bill end,
       drg = case when drg = '' then null else drg end;
      
vacuum analyze dw_staging.enc_proc_etl;

---------------------------------------------------------------------


drop table if exists dw_staging.detail_enc_etl;

CREATE TABLE dw_staging.detail_enc_etl (
derv_enc varchar null,
mem_id varchar null,
adm_dt date null, 
dis_dt date null, 
ln_nbr varchar null,
pat_stat varchar null,
fdos_dt date null, 
tdos_csl date null, 
proc_cd varchar null,
sub_chrg_amt numeric null,
dt_pd_amt numeric null,
dt_ln_unt int null,
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
h.derv_enc,
p.mem_id,
h.adm_dt,
h.dis_dt,
d.ln_nbr,
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
substring(p.bill,1,1) as bill_i,
substring(p.bill,2,1) as bill_c,
substring(p.bill,3,1) as bill_f,
p.drg
from dw_staging.enc_header_etl h 
join dw_staging.enc_proc_etl p 
on h.derv_enc  = p.derv_enc 
join dw_staging.enc_detail_etl d 
on d.derv_enc = h.derv_enc 
;

analyze dw_staging.detail_enc_etl;


select * from dw_staging.detail_enc_etl;

insert into dw_staging.claim_detail
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
	null::int as copay,
	null::int as deductible,
	null::int as coins,
	null::int as cob,
	bill_i,
	bill_c,
	bill_f,
	dt_ln_unt as units,
	dev.fiscal_year_func(a.fdos_dt) as fiscal_year,
	null::int as cost_factor_year,
	'enc_det' as table_id_src,
	null as claim_sequence_number_src,
	null as bill_provider,
	a.sub_ref_prov_npi as ref_provider,
	null as other_provider,
	a.sub_rend_prov_npi as perf_rn_provider,
	null as perf_at_provider,
	sub_opt_phy_npi as perf_op_provider,
	a.derv_enc as claim_id_src,
	a.mem_id as member_id_src,
	current_date as load_date,
	a.sub_rend_prv_tax_cd as provider_type
from dw_staging.detail_enc_etl a 
join data_warehouse.dim_uth_claim_id b 
  on a.mem_id = b.member_id_src 
 and a.derv_enc = b.claim_id_src 
 and b.data_source = 'mdcd'
;

vacuum analyze dw_staging.claim_detail;

drop table if exists dw_staging.enc_detail_etl;
drop table if exists dw_staging.enc_header_etl;
drop table if exists dw_staging.enc_proc_etl;
drop table if exists dw_staging.detail_enc_etl;
