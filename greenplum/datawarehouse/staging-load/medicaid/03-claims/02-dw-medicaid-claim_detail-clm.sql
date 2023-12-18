 /* ******************************************************************************************************
 * Author || Date      || Notes
 * ******************************************************************************************************
 * various authors  || <09/05/2023 || created
 * ******************************************************************************************************
 * xzhang  			|| 09/05/2023 || Changed table name from claim_detail to mcd_claim_detail
 * xzhang			|| 11/15/2023 || Modified to include line_status (paid/denied/etc on line level)
 * 									 Also rolled extraneous update statements into create table statements
 * xzhang			|| 11/29/23   || Fixed issue with 2019/2020 duplicated claims in proc table
 * 									 Previously yielded duplicated rows in final table
*/

/* ETL TABLES FOR CLEANING */

drop table if exists dw_staging.mcd_claim_detail;

create table dw_staging.mcd_claim_detail
(like data_warehouse.claim_detail including defaults) 
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
;

drop table if exists dw_staging.clm_detail_etl;

CREATE TABLE dw_staging.clm_detail_etl (
	year_fy int null,
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

insert into dw_staging.clm_detail_etl
select 
	year_fy,
	trim(icn),
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
	case when trim(perf_prov_npi) = '' then null else trim(perf_prov_npi) end,
	case when trim(txm_cd) = '' then null else trim(txm_cd) end,
	case when trim(perf_prov_id) = '' then null else trim(perf_prov_id) end,
	case when trim(sub_perf_prov_sfx) = '' then null else trim(sub_perf_prov_sfx) end
from medicaid.clm_detail 
;

analyze dw_staging.clm_detail_etl;

update dw_staging.clm_detail_etl
   set proc_cd = sub_proc_cd where proc_cd is null and sub_proc_cd is not null;
       
vacuum analyze dw_staging.clm_detail_etl;


-----------------------HEADER------------------------------

drop table if exists dw_staging.clm_header_etl;

CREATE TABLE dw_staging.clm_header_etl (
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

insert into dw_staging.clm_header_etl 
select 
	trim(icn),
	case when trim(adm_dt) = '' then null else trim(adm_dt) end,
	case when trim(dis_dt) = '' then null else trim(dis_dt) end,
	trim(hdr_frm_dos),
	trim(hdr_to_dos),
	case when trim(pat_stat_cd) = '' then null else trim(pat_stat_cd) end
from medicaid.clm_header ;
  
vacuum analyze dw_staging.clm_header_etl ;

--------------------------------------------------------------------------
--------------------------------------------------------------------------
--------------------------------------------------------------------------
--We have some duplicate claims from 2019/2020 so need to fix

CREATE TABLE dw_staging.clm_proc_etl (
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

insert into dw_staging.clm_proc_etl
select trim(icn),
       trim(max(pcn)),
       case when trim(max(drg)) = '' then null else trim(max(drg)) end,
       case when trim(max(bill)) = '' then null else trim(max(bill)) end
from medicaid.clm_proc
group by 1;
  
vacuum analyze dw_staging.clm_proc_etl;


------------master table -----------------

drop table if exists dw_staging.detail_etl;

CREATE TABLE dw_staging.detail_etl (
	year_fy int null,
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

insert into dw_staging.detail_etl 
select 
	d.year_fy,
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
from dw_staging.clm_header_etl h 
join dw_staging.clm_proc_etl p 
on h.icn = p.icn 
join dw_staging.clm_detail_etl d 
on d.icn = h.icn 
;

analyze dw_staging.detail_etl;

/*
 * Load staging
 */

insert into dw_staging.mcd_claim_detail
select 
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
	null::int as copay,
	null::int as deductible,
	null::int as coins,
	null::int as cob,
	substring(bill,1,1),
	substring(bill,2,1),
	substring(bill,3,1),
	null::float as units,
	a.year_fy as fiscal_year,
	null::int as cost_factor_year,
	'clm_detail' as table_id_src,
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
from dw_staging.detail_etl a 
join data_warehouse.dim_uth_claim_id b 
  on a.pcn = b.member_id_src 
 and a.icn = b.claim_id_src 
 and b.data_source in ('mdcd', 'mhtw', 'mcpp')
;

analyze dw_staging.mcd_claim_detail;

drop table dw_staging.clm_header_etl;
drop table dw_staging.clm_proc_etl;
drop table dw_staging.clm_detail_etl ;
