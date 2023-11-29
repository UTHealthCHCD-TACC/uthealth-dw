 /* ******************************************************************************************************
 * Author || Date      || Notes
 * ******************************************************************************************************
 * various authors  || <09/05/2023 || created
 * ******************************************************************************************************
 * xzhang  			|| 09/05/2023 || Changed table name from claim_detail to mcd_claim_detail
 * xzhang			|| 11/16/2023 || Modified to include line_status (paid/denied/etc on line level)
 * 									 Also rolled extraneous update statements into create table statements
*/

drop table if exists dw_staging.htw_clm_detail_etl;

CREATE TABLE dw_staging.htw_clm_detail_etl (
	icn text NULL,
	clm_dtl_nbr text NULL,
	dtl_stat_cd text NULL,
	proc_cd text NULL,
	sub_proc_cd text NULL,
	dtl_bill_amt numeric NULL,
	dtl_alwd_amt numeric NULL,
	dtl_pd_amt numeric NULL,
	proc_mod_1 text NULL,
	proc_mod_2 text NULL,
	pos text NULL,
	rev_cd text null,
	ref_prov_npi text NULL,
	perf_prov_npi text NULL,
	txm_cd text NULL,
	perf_prov_id text NULL,
	sub_perf_prov_sfx text NULL
)
WITH (
	appendonly=true,
	orientation=row,
	compresstype=zlib
)
DISTRIBUTED BY (icn);

insert into dw_staging.htw_clm_detail_etl
select 
    case when trim(icn) = '' then null else trim(icn) end,
    case when trim(clm_dtl_nbr) = '' then null else trim(clm_dtl_nbr) end,
    case when trim(dtl_stat_cd) = '' then null else trim(dtl_stat_cd) end,
	case when (trim(proc_cd) = '' or length(trim(proc_cd)) < 5) then null else trim(proc_cd) end,
	case when (trim(sub_proc_cd) = '' or length(trim(sub_proc_cd)) < 5) then null else trim(sub_proc_cd) end,
    dtl_bill_amt,
    dtl_alwd_amt,
    dtl_pd_amt,
    case when trim(proc_mod_1) = '' then null else trim(proc_mod_1) end,
    case when trim(proc_mod_2) = '' then null else trim(proc_mod_2) end,
    case when trim(pos) = '' then null else trim(pos) end,
	case when trim(rev_cd) = '' then null else lpad(trim(rev_cd), 4, '0') end,
    case when trim(ref_prov_npi) = '' then null else trim(ref_prov_npi) end,
    case when trim(perf_prov_npi) = '' then null else trim(perf_prov_npi )end,
    case when trim(txm_cd) = '' then null else trim(txm_cd) end,
    case when trim(perf_prov_id) = '' then null else trim(perf_prov_id) end,
    case when trim(sub_perf_prov_sfx) = '' then null else trim(sub_perf_prov_sfx) end
from medicaid.htw_clm_detail  
;

analyze dw_staging.htw_clm_detail_etl;

update dw_staging.htw_clm_detail_etl
   set proc_cd = sub_proc_cd where proc_cd is null and sub_proc_cd is not null;
       
vacuum analyze dw_staging.htw_clm_detail_etl;


-----------------------HEADER------------------------------

drop table if exists dw_staging.htw_clm_header_etl;

CREATE TABLE dw_staging.htw_clm_header_etl (
	icn text NULL,
	adm_dt text NULL,
	dis_dt text NULL,
	from_dos text NULL,
	to_dos text NULL,
	pat_stat_cd text NULL
)
WITH (
	appendonly=true,
	orientation=row,
	compresstype=zlib
)
DISTRIBUTED BY (icn);

insert into dw_staging.htw_clm_header_etl 
select 
	trim(icn),
	case when trim(adm_dt) = '' then null else trim(adm_dt) end,
	case when trim(dis_dt) = '' then null else trim(dis_dt) end,
	trim(hdr_frm_dos),
	trim(hdr_to_dos),
	case when trim(pat_stat_cd) = '' then null else trim(pat_stat_cd) end
from medicaid.htw_clm_header;
  
vacuum analyze dw_staging.htw_clm_header_etl ;



---------------------------------------------------

drop table if exists dw_staging.htw_clm_proc_etl;


CREATE TABLE dw_staging.htw_clm_proc_etl (
	icn text NULL,
	pcn text NULL,
	drg text null,
	bill text null
)
WITH (
	appendonly=true,
	orientation=row,
	compresstype=zlib
)
DISTRIBUTED BY (icn);

insert into dw_staging.htw_clm_proc_etl
select trim(icn),
       trim(pcn),
       case when trim(drg) = '' then null else trim(drg) end,
       case when trim(bill) = '' then null else trim(bill) end
  from medicaid.htw_clm_proc 
  ;
  
vacuum analyze dw_staging.htw_clm_proc_etl;

---------------------------------------------------------

drop table if exists dw_staging.htw_detail_etl;

CREATE TABLE dw_staging.htw_detail_etl (
	icn text NULL,
	pcn text NULL,
	clm_dtl_nbr text NULL,
	dtl_stat_cd text NULL,
	from_dos date NULL,
	to_dos date NULL,
	proc_cd text NULL,
	dtl_bill_amt numeric NULL,
	dtl_alwd_amt numeric NULL,
	dtl_pd_amt numeric NULL,
	proc_mod_1 text NULL,
	proc_mod_2 text NULL,
	pos text NULL,
	rev_cd text null,
	ref_prov_npi text NULL,
	perf_prov_npi text NULL,
	txm_cd text NULL,
	perf_prov_id text NULL,
	sub_perf_prov_sfx text null,
	adm_dt date NULL,
	dis_dt date NULL,
	pat_stat_cd text null,
	drg text null,
	bill text null
)
WITH (
	appendonly=true,
	orientation=row,
	compresstype=zlib
)
DISTRIBUTED BY (icn);

insert into dw_staging.htw_detail_etl 
select 
	h.icn,
	p.pcn,
	clm_dtl_nbr,
	dtl_stat_cd,
	from_dos::date,
	to_dos::date,
	proc_cd,
	dtl_bill_amt,
	dtl_alwd_amt,
	dtl_pd_amt,
	proc_mod_1,
	proc_mod_2,
	pos,
	rev_cd,
	ref_prov_npi,
	perf_prov_npi,
	txm_cd,
	perf_prov_id,
	sub_perf_prov_sfx,
	adm_dt::date,
	dis_dt::date,
	pat_stat_cd,
	drg,
	bill
from dw_staging.htw_clm_header_etl h 
join dw_staging.htw_clm_proc_etl p 
on h.icn = p.icn 
join dw_staging.htw_clm_detail_etl d 
on d.icn = h.icn 
;

analyze dw_staging.htw_detail_etl ;

drop table dw_staging.htw_clm_header_etl;
drop table dw_staging.htw_clm_proc_etl;
drop table dw_staging.htw_clm_detail_etl ;

---------------------------------------------------------------
------------------ insert into staging-------------------------
---------------------------------------------------------------

insert into dw_staging.mcd_claim_detail
select distinct 
    'mdcd' as data_source,
	extract(year from a.from_dos) as year,
	b.uth_member_id as uth_member_id,
	b.uth_claim_id as uth_claim_id,
	a.clm_dtl_nbr::int as claim_sequence_number,
	a.from_dos as from_date_of_service,
	a.to_dos as to_date_of_service,
	get_my_from_date(a.from_dos) as month_year_id,
	a.pos as place_of_service,
	true as network_ind,
	true as network_paid_ind,
	a.adm_dt as admit_date,
	a.dis_dt as discharge_date,
	a.pat_stat_cd as discharge_status,
	a.proc_cd as cpt_hcpcs_cd,
	null as procedure_type,
	a.proc_mod_1 as proc_mod_1,
	a.proc_mod_2 as proc_mod_2,
	a.drg as drg_cd,
	a.rev_cd as revenue_cd,
	a.dtl_bill_amt as charge_amount,
	a.dtl_alwd_amt as allowed_amount,
	a.dtl_pd_amt as paid_amount,
	null::numeric as copay,
	null::numeric as deductible,
	null::numeric as coins,
	null::numeric as cob,
	substring(bill,1,1),
	substring(bill,2,1),
	substring(bill,3,1),
	null::numeric as units,
	dev.fiscal_year_func(a.from_dos) as fiscal_year,
	null::int as cost_factor_year,
	'htw_clm_detail' as table_id_src,
	null as bill_provider,
	a.ref_prov_npi as ref_provider,
	null as other_provider,
	a.perf_prov_npi as perf_rn_provider,
	null as perf_at_provider,
	null as perf_op_provider,
	a.icn as claim_id_src,
	a.pcn as member_id_src,
	current_date as load_date,
	a.txm_cd as provider_type,
	a.bill as bill,
	a.dtl_stat_cd as line_status
from dw_staging.htw_detail_etl a 
join data_warehouse.dim_uth_claim_id b 
  on a.pcn = b.member_id_src 
 and a.icn = b.claim_id_src 
 and b.data_source in ('mdcd', 'mhtw', 'mcpp')
;


vacuum analyze dw_staging.mcd_claim_detail;
