


--get distinct ICNs from medicaid schema (TACC) by year claims
select a.year_fy::text, count(distinct a.icn) as count
from medicaid.clm_header a inner join medicaid.clm_proc b
	on a.icn = b.icn
group by a.year_fy
order by a.year_fy;

--get distinct ICNs from medicaid schema (TACC) by year encounters
select a.year_fy::text, count(distinct a.derv_enc) as count
from medicaid.enc_header a inner join medicaid.enc_proc b
	on a.derv_enc = b.derv_enc 
group by a.year_fy
order by a.year_fy;

--get distinct ICNs from htw_clms
select 'HTW' as year_fy, count(distinct a.icn) as count
from medicaid.htw_clm_detail a inner join medicaid.htw_clm_proc b
  on a.icn = b.icn;

--get distinct ICNs from medicaid schema (TACC, Total)
select 'Total' as year_fy, count(distinct a.icn) as count
from (select icn from medicaid.clm_header
	union all select derv_enc from medicaid.enc_header
	union all select icn from medicaid.htw_clm_header) a
	 inner join
	 (select icn from medicaid.clm_proc
	union select derv_enc from medicaid.enc_proc
	union select icn from medicaid.htw_clm_proc) b
	on a.icn = b.icn;
	




--get distinct ICNs from medicaid schema (TACC) by year claims
select a.year_fy::text, count(distinct a.icn) as count
from medicaid.clm_detail a inner join medicaid.clm_proc b
	on a.icn = b.icn
group by a.year_fy
order by a.year_fy;



--get distinct ICNs from medicaid schema (TACC) by year encounters
select a.year_fy::text, count(distinct a.derv_enc) as count
from medicaid.enc_det a inner join medicaid.enc_proc b
	on a.derv_enc = b.derv_enc 
group by a.year_fy
order by a.year_fy;


--get distinct ICNs from htw_clms
select 'HTW' as year_fy, count(distinct a.icn) as count
from medicaid.htw_clm_detail a inner join medicaid.htw_clm_proc b
  on a.icn = b.icn;



--get distinct ICNs from medicaid schema (TACC, Total)
select 'Total' as year_fy, count(distinct a.icn) as count
from (select icn from medicaid.clm_detail
	union all select derv_enc from medicaid.enc_det
	union all select icn from medicaid.htw_clm_detail) a
	 inner join
	 (select icn from medicaid.clm_proc
	union select derv_enc from medicaid.enc_proc
	union select icn from medicaid.htw_clm_proc) b
	on a.icn = b.icn;


select * from dw_staging.claim_detail limit 5;

--claims spot checking!

select * from dev.xz_dwqa_temp1 limit 10;

select distinct fy from dev.xz_dwqa_temp1;

drop table if exists dev.xz_dwqa_temp2;

select trim(icn) as icn, fy, trim(pcn) as pcn, trim(dos) as dos,
	trim(prim_dx_qal) as icd, trim(prim_dx_cd) as dx
into dev.xz_dwqa_temp2
from dev.xz_dwqa_temp1
where prim_dx_cd is not null and trim(prim_dx_cd) != '';

insert into dev.xz_dwqa_temp2
select trim(icn) as icn, fy, trim(pcn) as pcn, trim(dos) as dos,
	trim(dx_cd_qual_1) as icd, trim(dx_cd_1) as dx
from dev.xz_dwqa_temp1
where dx_cd_1 is not null and trim(dx_cd_1) != '';

select * from dev.xz_dwqa_temp2;



drop table if exists dev.xz_dwqa_temp3;

select a.*, b.claim_id_src, b.fiscal_year, b.member_id_src, b.from_date_of_service, b.icd_version, b.diag_cd,
	case when b.claim_id_src is null then 1 else 0 end as claim_id_mismatch,
	case when b.fiscal_year is null then 1 
		when b.fiscal_year::text != a.fy then 1 else 0 end as fy_mismatch,
	case when b.member_id_src is null then 1 
		when b.member_id_src != a.pcn then 1 else 0 end as mem_id_mismatch,
	case when b.from_date_of_service is null then 1 
		when b.from_date_of_service::text != a.dos then 1 else 0 end as dos_mismatch,
	case when b.icd_version is null then 1
		when b.icd_version != a.icd then 1 else 0 end as icd_mismatch,
	case when b.diag_cd is null then 1
		when b.diag_cd != a.dx then 1 else 0 end as dx_mismatch
into dev.xz_dwqa_temp3
from dev.xz_dwqa_temp2 a left join dw_staging.claim_diag b on a.icn = b.claim_id_src and a.dx = b.diag_cd;

select * from dev.xz_dwqa_temp3 limit 10;

select icn, claim_id_src, pcn, member_id_src from dev.xz_dwqa_temp3 where mem_id_mismatch = 1;

select * from dev.xz_dwqa_temp3 where mem_id_mismatch = 1;

select * from dev.xz_dwqa_temp3 where dos_mismatch = 1;

select * from dev.xz_dwqa_temp3 where icd_mismatch = 1;

select sum(claim_id_mismatch) as clam_id_mismatch, sum(fy_mismatch) as fy_mismatch, 
 sum(mem_id_mismatch) as mem_id_mismatch, sum(dos_mismatch) as dos_mismatch,
 sum(icd_mismatch) as icd_mismatch, sum(dx_mismatch) as dx_mismatch
from dev.xz_dwqa_temp3;

select count(*)

select * from dw_staging.claim_diag where claim_id_src like '0000DD1001356424530002D1J%';

select * from dw_staging.claim_diag where claim_id_src like '0000DD10013564245300%';

select * from dw_staging.claim_diag where claim_id_src is null;


select * from medicaid.clm_dx where icn = '0000DD1001356424530002';
0000DD1001356424530002            

select a.fy, count(*)
from dev.xz_dwqa_temp1 a left join dw_staging.claim_diag b on a.icn = b.claim_id_src
where b.claim_id_src is null
group by a.fy 
order by a.fy;

--proc codes

drop table if exists dev.xz_dwqa_temp3;

select a.*, b.claim_id_src, b.fiscal_year, b.member_id_src, b.from_date_of_service, b.icd_version, b.proc_cd,
	case when b.claim_id_src is null then 1 else 0 end as claim_id_mismatch,
	case when b.fiscal_year is null then 1 
		when b.fiscal_year::text != a.fy then 1 else 0 end as fy_mismatch,
	case when b.member_id_src is null then 1 
		when b.member_id_src != a.pcn then 1 else 0 end as mem_id_mismatch,
	case when b.from_date_of_service is null then 1 
		when b.from_date_of_service::text != a.dos then 1 else 0 end as dos_mismatch,
	case when b.icd_version is null then 1
		when b.icd_version != a.icd then 1 else 0 end as icd_mismatch,
	case when b.proc_cd is null then 1
		when b.proc_cd != a.proc then 1 else 0 end as proc_mismatch
into dev.xz_dwqa_temp3
from dev.xz_dwqa_temp2 a left join dw_staging.claim_icd_proc b on a.icn = b.claim_id_src and a.proc = b.proc_cd;

select fy, sum(claim_id_mismatch) as clam_id_mismatch, sum(fy_mismatch) as fy_mismatch, 
 sum(mem_id_mismatch) as mem_id_mismatch, sum(dos_mismatch) as dos_mismatch,
 sum(icd_mismatch) as icd_mismatch, sum(proc_mismatch) as proc_mismatch, count(*) as proc_cds_checked
from dev.xz_dwqa_temp3
group by fy
order by fy;

select a.fy, count(*)
from dev.xz_dwqa_temp1 a left join dw_staging.claim_icd_proc b on a.icn = b.claim_id_src
where b.claim_id_src is null
group by a.fy 
order by a.fy;


select count(*) from dev.xz_dwqa_temp3;

select * from dev.xz_dwqa_temp3;

select * from dev.xz_dwqa_temp3 where claim_id_mismatch = 1;

--errors
100050030201202302237745	2012	530415105	2011-10-20	9	09604
0000114865735501I71			2013	615899960	2013-01-17	9	09543
00000006013695940I8H		2013	530599678	2012-09-25	9	741
200050030201332696272753	2013	615791078	2013-08-13	9	00040
00000006013740662I8H		2013	614420242	2012-09-16	9	09915

select * from dw_staging.claim_icd_proc where claim_id_src = '100050030201202302237745'; --pos5
select * from dw_staging.claim_icd_proc where claim_id_src = '200050030201332696272753'; --pos5



select * from dw_staging.claim_icd_proc where claim_id_src = '00000006013695940I8H'; --does not exist

select * from medicaid.clm_proc where icn = '100050030201202302237745';

select proc_icd_qal_5, proc_icd_cd_5 from medicaid.clm_proc
where proc_icd_cd_5 is not null and proc_icd_cd_5 != '' and ;



select * from medicaid.clm_proc where file like '%enc%' limit 5;

select proc_icd_qal_5, proc_icd_cd_5 from medicaid.enc_proc where proc_icd_cd_5 != '' limit 100;

select distinct file from medicaid.clm_proc;



--SQL kill code
select usename, pid, state, waiting, query_start , query, *
from pg_catalog.pg_stat_activity where
usename in ('xrzhang') and ---- put your username
state = 'active'
order by state, usename;

select  pg_terminate_backend(2931); --- put in whatever the pid id is




select file, derv_enc, proc_icd_cd_4, proc_icd_cd_5, proc_icd_cd_6
from medicaid.enc_proc where proc_icd_cd_5 is not null and proc_icd_cd_5 != '' and
file like '%14%';


'00001008040474IN4' missing proc cd

select count(*) from dev.xz_mcd_clm_proc5 ;
select count(*) from dev.xz_mcd_enc_proc5 ;


--



drop table if exists dev.xz_temp1;
select distinct a.icn
into dev.xz_temp1
from (select icn from medicaid.clm_detail where year_fy = 2016
	union all select derv_enc from medicaid.enc_det where year_fy = 2016
	union all select icn from medicaid.htw_clm_detail where year_fy = 2016) a
	 inner join
	 (select icn from medicaid.clm_proc where year_fy = 2016
	union select derv_enc from medicaid.enc_proc where year_fy = 2016
	union select icn from medicaid.htw_clm_proc where year_fy = 2016) b
	on a.icn = b.icn;


drop table if exists dev.xz_temp2;
select distinct claim_id_src
into dev.xz_temp2
from dw_staging.claim_detail
where fiscal_year = 2016; 

select * from dev.xz_temp1 a full join dev.xz_temp2 b
on a.icn = b.claim_id_src
where a.icn is null 
or b.claim_id_src is null limit 15;

100050061201824971411214	
200030051201824969747387	
100020081201825772263539	
200020011201822872259600	
100023030201825471872748	
200030010201822965131322	
100020081201825772260923	
200030051201824969747435	
100020081201825672263244	
200050030201825170762497	
100023030201825572268596	
100023030201825773193901	
200020030201825472039624	
100020081201825672260494	
200030051201824969747534	

select * from dw_staging.claim_detail where claim_id_src = '100020030201825773134393'

select * from medicaid.clm_proc where icn = '100050061201824971411214'; --is here
select * from medicaid.clm_detail where icn = '100050061201824971411214'; --is here too...?
select * from medicaid.clm_header where icn = '100050061201824971411214'; --is missing from here

select * from medicaid.clm_header where icn not in (select claim_id_src from dw_staging.claim_header)

select * from dev.xz_dwqa_temp1 where fy = 2021::text;
select * from dev.xz_dwqa_temp1 where fy = 2021::text and (trim(prim_dx_cd) = '' or prim_dx_cd is null);
select * from dev.xz_dwqa_temp1 where fy = 2020::text and (trim(prim_dx_cd) = '' or prim_dx_cd is null);

select a.fy, count(*)
from dev.xz_dwqa_temp1 a left join dw_staging.claim_diag b on a.icn = b.claim_id_src
where b.claim_id_src is null
group by a.fy 
order by a.fy;


select icn
from dev.xz_dwqa_temp1 a left join dw_staging.claim_diag b on a.icn = b.claim_id_src
where b.claim_id_src is null
and fy = 2020::text;

0000201932613429900D1M
0000202017602174700D1M
0000201926134725200D1M
100021030202021011137294
000030000053467974D1J
0000202017602172600D1M
0000202017602175100D1M
000030000047683566D1J
0000201926134724900D1K
000030000053467969D1J
100021030202021011137331
000030000048451985D1J
0000202020933737800D1M
100021030202008472320284

select * from dev.xz_dwqa_temp1 where icn = '0000201932613429900D1M'

select * from medicaid.enc_dx where year_fy = 2021 and (trim(prim_dx_cd) = '' or prim_dx_cd is null);


drop table if exists dev.xz_dwqa_temp2;

select a.*, b.from_date_of_service, b.claim_type as dw_claim_type, b.total_charge_amount, b.total_allowed_amount,
	b.total_paid_amount, b.fiscal_year, b.to_date_of_service, b.bill_provider as dw_bill_provider, b.ref_provider as dw_ref_provider, 
	b.perf_at_provider as dw_perf_at_provider, b.provider_type as dw_provider_type,
	case when b.claim_id_src is null then 1 when b.claim_id_src::text != trim(a.icn)::text then 1 else 0 end as icn_mismatch,
	case when b.fiscal_year is null then 1 when b.fiscal_year::text != trim(a.fy)::text then 1 else 0 end as fy_mismatch,
	case when b.from_date_of_service is null then 1 when b.from_date_of_service::text != trim(a.from_dos)::text then 1 else 0 end as from_dos_mismatch,
	case when b.to_date_of_service is null then 1 when b.to_date_of_service::text != trim(a.to_dos)::text then 1 else 0 end as to_dos_mismatch,
	case when b.claim_type is null then 1 when b.claim_type::text != trim(a.claim_type)::text then 1 else 0 end as claim_type_mismatch,
	case when b.total_charge_amount is null then 1 when b.total_charge_amount::float != trim(a.tot_charge_amt)::float then 1 else 0 end as tot_charge_amt_mismatch,
	case when b.total_allowed_amount is null then 1 when b.total_allowed_amount::float != trim(a.tot_allowed_amt)::float then 1 else 0 end as tot_allowed_amt_mismatch,
	case when b.total_paid_amount is null and a.tot_paid_amt is not null then 1 when b.total_paid_amount::float != trim(a.tot_paid_amt)::float then 1 else 0 end as tot_paid_amt_mismatch,
	case when b.bill_provider is null and trim(a.bill_provider) = '' then 0
		when b.bill_provider is null and trim(a.bill_provider) != '' then 1
		when b.bill_provider::text != trim(a.bill_provider)::text then 1 else 0 end as bill_provider_mismatch,
	case when b.perf_at_provider is null and trim(a.perf_at_provider) = '' then 0
		when b.perf_at_provider is null and trim(a.perf_at_provider) != '' then 1
		when b.perf_at_provider::text != trim(a.perf_at_provider)::text then 1 else 0 end as perf_at_provider_mismatch,
	case when b.provider_type is null then 1 when b.provider_type::text != trim(a.provider_type)::text then 1 else 0 end as provider_type_mismatch
into dev.xz_dwqa_temp2
from dev.xz_dwqa_temp1 a left join dw_staging.claim_header b on a.icn = b.claim_id_src;
--about 2 mins

select * from dev.xz_dwqa_temp2 where tot_paid_amt_mismatch = 1;

select icn, count(*) as count from dev.xz_dwqa_temp2
group by icn having count(*) > 1;


select fy, count(*) as count from dev.xz_dwqa_temp2
group by fy
order by fy;

select fy, count(*) as count from dev.xz_dwqa_temp1
group by fy
order by fy;


select * from dev.xz_dwqa_temp1;

--CLAIM DETAIL

select * from dw_staging.claim_detail limit 1;

select * from medicaid.clm_proc where drg is not null limit 1 ;

select * from data_warehouse.claim_detail limit 5;

SELECT table_schema, table_name, ordinal_position, column_name, udt_name FROM INFORMATION_SCHEMA.COLUMNS
where table_schema = 'dw_staging' and table_name = 'claim_detail'
order by ordinal_position;


select * from medicaid.enc_det limit 1;

--trim and null

update dev.xz_dwqa_temp1
   set spc_claim_sequence_number = case when trim(spc_claim_sequence_number) = '' then null else trim(spc_claim_sequence_number) end,
spc_from_date_of_service = case when trim(spc_from_date_of_service) = '' then null else trim(spc_from_date_of_service) end,
spc_to_date_of_service = case when trim(spc_to_date_of_service) = '' then null else trim(spc_to_date_of_service) end,
spc_place_of_service = case when trim(spc_place_of_service) = '' then null else trim(spc_place_of_service) end,
spc_cpt_hcpcs_cd = case when trim(spc_cpt_hcpcs_cd) = '' then null else trim(spc_cpt_hcpcs_cd) end,
sub_proc_cd = case when trim(sub_proc_cd) = '' then null else trim(sub_proc_cd) end,
spc_proc_mod_1 = case when trim(spc_proc_mod_1) = '' then null else trim(spc_proc_mod_1) end,
spc_proc_mod_2 = case when trim(spc_proc_mod_2) = '' then null else trim(spc_proc_mod_2) end,
spc_revenue_cd = case when trim(spc_revenue_cd) = '' then null else trim(spc_revenue_cd) end,
spc_charge_amount = case when trim(spc_charge_amount) = '' then null else trim(spc_charge_amount) end,
spc_allowed_amount = case when trim(spc_allowed_amount) = '' then null else trim(spc_allowed_amount) end,
spc_paid_amount = case when trim(spc_paid_amount) = '' then null else trim(spc_paid_amount) end,
spc_ref_provider = case when trim(spc_ref_provider) = '' then null else trim(spc_ref_provider) end,
spc_perf_rn_provider = case when trim(spc_perf_rn_provider) = '' then null else trim(spc_perf_rn_provider) end,
spc_perf_op_provider = case when trim(spc_perf_op_provider) = '' then null else trim(spc_perf_op_provider) end,
spc_provider_type = case when trim(spc_provider_type) = '' then null else trim(spc_provider_type) end,
spc_admit_date = case when trim(spc_admit_date) = '' then null else trim(spc_admit_date) end,
spc_discharge_date = case when trim(spc_discharge_date) = '' then null else trim(spc_discharge_date) end,
spc_discharge_status = case when trim(spc_discharge_status) = '' then null else trim(spc_discharge_status) end,
spc_claim_id_src = case when trim(spc_claim_id_src) = '' then null else trim(spc_claim_id_src) end,
spc_drg_cd = case when trim(spc_drg_cd) = '' then null else trim(spc_drg_cd) end,
spc_bill_type_inst = case when trim(spc_bill_type_inst) = '' then null else trim(spc_bill_type_inst) end,
spc_bill_type_class = case when trim(spc_bill_type_class) = '' then null else trim(spc_bill_type_class) end,
spc_bill_type_freq = case when trim(spc_bill_type_freq) = '' then null else trim(spc_bill_type_freq) end,
spc_units = case when trim(spc_units) = '' then null else trim(spc_units) end,
spc_member_id_src = case when trim(spc_member_id_src) = '' then null else trim(spc_member_id_src) end;

--Fix numbers if they are not the right length
update dev.xz_dwqa_temp1
set spc_cpt_hcpcs_cd = case when length(spc_cpt_hcpcs_cd) < 5 then null else spc_cpt_hcpcs_cd end,
	sub_proc_cd = case when length(sub_proc_cd) < 5 then null else sub_proc_cd end,
	spc_revenue_cd = lpad(spc_revenue_cd, 4, '0');
	
--Use SUB_PROC_CD to replace PROC_CD in the cases where we're missing a PROC_CD
--Yes we do lose some SUB_PROC_CDs but anyone who needs them will have to go back to the original data
--as none of the other data sources have more than 1 CPT_HCSPCS_CD

update dev.xz_dwqa_temp1
	set spc_cpt_hcpcs_cd = sub_proc_cd where spc_cpt_hcpcs_cd is null and sub_proc_cd is not null;


drop table if exists dev.xz_dwqa_temp2;

select a.*,
b.claim_sequence_number,
b.from_date_of_service,
b.to_date_of_service,
b.place_of_service,
b.cpt_hcpcs_cd,
b.proc_mod_1,
b.proc_mod_2,
b.revenue_cd,
b.charge_amount,
b.allowed_amount,
b.paid_amount,
b.ref_provider,
b.perf_rn_provider,
b.perf_op_provider,
b.provider_type,
b.admit_date,
b.discharge_date,
b.discharge_status,
b.claim_id_src,
b.drg_cd,
b.bill_type_inst,
b.bill_type_class,
b.bill_type_freq,
b.units,
b.member_id_src,
case when b.claim_sequence_number is null and a.spc_claim_sequence_number is not null then 1 when b.claim_sequence_number::text != a.spc_claim_sequence_number then 1 else 0 end as claim_sequence_number_mismatch,
case when b.from_date_of_service is null and a.spc_from_date_of_service is not null then 1 when b.from_date_of_service::text != a.spc_from_date_of_service then 1 else 0 end as from_date_of_service_mismatch,
case when b.to_date_of_service is null and a.spc_to_date_of_service is not null then 1 when b.to_date_of_service::text != a.spc_to_date_of_service then 1 else 0 end as to_date_of_service_mismatch,
case when b.place_of_service is null and a.spc_place_of_service is not null then 1 when b.place_of_service::text != a.spc_place_of_service then 1 else 0 end as place_of_service_mismatch,
case when b.cpt_hcpcs_cd is null and a.spc_cpt_hcpcs_cd is not null then 1 when b.cpt_hcpcs_cd::text != a.spc_cpt_hcpcs_cd then 1 else 0 end as cpt_hcpcs_cd_mismatch,
case when b.proc_mod_1 is null and a.spc_proc_mod_1 is not null then 1 when b.proc_mod_1::text != a.spc_proc_mod_1 then 1 else 0 end as proc_mod_1_mismatch,
case when b.proc_mod_2 is null and a.spc_proc_mod_2 is not null then 1 when b.proc_mod_2::text != a.spc_proc_mod_2 then 1 else 0 end as proc_mod_2_mismatch,
case when b.revenue_cd is null and a.spc_revenue_cd is not null then 1 when b.revenue_cd::text != a.spc_revenue_cd then 1 else 0 end as revenue_cd_mismatch,
case when b.charge_amount is null and a.spc_charge_amount is not null then 1 when b.charge_amount::float != a.spc_charge_amount::float then 1 else 0 end as charge_amount_mismatch,
case when b.allowed_amount is null and a.spc_allowed_amount is not null then 1 when b.allowed_amount::float != a.spc_allowed_amount::float then 1 else 0 end as allowed_amount_mismatch,
case when b.paid_amount is null and a.spc_paid_amount is not null then 1 when b.paid_amount::float != a.spc_paid_amount::float then 1 else 0 end as paid_amount_mismatch,
case when b.ref_provider is null and a.spc_ref_provider is not null then 1 when b.ref_provider::text != a.spc_ref_provider then 1 else 0 end as ref_provider_mismatch,
case when b.perf_rn_provider is null and a.spc_perf_rn_provider is not null then 1 when b.perf_rn_provider::text != a.spc_perf_rn_provider then 1 else 0 end as perf_rn_provider_mismatch,
case when b.perf_op_provider is null and a.spc_perf_op_provider is not null then 1 when b.perf_op_provider::text != a.spc_perf_op_provider then 1 else 0 end as perf_op_provider_mismatch,
case when b.provider_type is null and a.spc_provider_type is not null then 1 when b.provider_type::text != a.spc_provider_type then 1 else 0 end as provider_type_mismatch,
case when b.admit_date is null and a.spc_admit_date is not null then 1 when b.admit_date::text != a.spc_admit_date then 1 else 0 end as admit_date_mismatch,
case when b.discharge_date is null and a.spc_discharge_date is not null then 1 when b.discharge_date::text != a.spc_discharge_date then 1 else 0 end as discharge_date_mismatch,
case when b.discharge_status is null and a.spc_discharge_status is not null then 1 when b.discharge_status::text != a.spc_discharge_status then 1 else 0 end as discharge_status_mismatch,
case when b.claim_id_src is null and a.spc_claim_id_src is not null then 1 when b.claim_id_src::text != a.spc_claim_id_src then 1 else 0 end as claim_id_src_mismatch,
case when b.drg_cd is null and a.spc_drg_cd is not null then 1 when b.drg_cd::text != a.spc_drg_cd then 1 else 0 end as drg_cd_mismatch,
case when b.bill_type_inst is null and a.spc_bill_type_inst is not null then 1 when b.bill_type_inst::text != a.spc_bill_type_inst then 1 else 0 end as bill_type_inst_mismatch,
case when b.bill_type_class is null and a.spc_bill_type_class is not null then 1 when b.bill_type_class::text != a.spc_bill_type_class then 1 else 0 end as bill_type_class_mismatch,
case when b.bill_type_freq is null and a.spc_bill_type_freq is not null then 1 when b.bill_type_freq::text != a.spc_bill_type_freq then 1 else 0 end as bill_type_freq_mismatch,
case when b.units is null and a.spc_units is not null then 1 when b.units::text != a.spc_units then 1 else 0 end as units_mismatch,
case when b.member_id_src is null and a.spc_member_id_src is not null then 1 when b.member_id_src::text != a.spc_member_id_src then 1 else 0 end as member_id_src_mismatch
into dev.xz_dwqa_temp2
from dev.xz_dwqa_temp1 a left join dw_staging.claim_detail b
on a.spc_claim_id_src = b.claim_id_src and trim(a.spc_claim_sequence_number)::int = b.claim_sequence_number;

select * from dev.xz_dwqa_temp2;


select claim_sequence_number, spc_claim_sequence_number from dev.xz_dwqa_temp2
where claim_sequence_number_mismatch = 1;


select bill_type_freq, spc_bill_type_freq from dev.xz_dwqa_temp2
where bill_type_freq_mismatch = 1;

select charge_amount, spc_charge_amount from dev.xz_dwqa_temp2
where charge_amount_mismatch = 1;

select from_date_of_service, spc_from_date_of_service from dev.xz_dwqa_temp2
where from_date_of_service_mismatch = 1;

/* DW side is more accurate
2012-01-25	0001-01-01
2013-03-04	0001-01-01
2011-10-14	0001-01-01
2011-10-05	0001-01-01
2011-10-03	0001-01-01
2011-12-22	0001-01-01
2012-11-01	0001-01-01
2011-10-05	0001-01-01 */

select to_date_of_service, spc_to_date_of_service from dev.xz_dwqa_temp2
where to_date_of_service_mismatch = 1;

/*same here
2019-04-11	0001-01-01
2011-12-09	0001-01-01
2016-09-22	0001-01-01
2017-01-18	0001-01-01
2019-08-30	0001-01-01
2016-04-06	0001-01-01
2018-12-17	0001-01-01
2019-04-04	0001-01-01
2019-04-04	0001-01-01
2017-03-01	0001-01-01
2016-12-28	0001-01-01
2019-04-04	0001-01-01
*/

select a.units as dw_units, a.spc_units, b.table_id_src
from dev.xz_dwqa_temp2 a left join dw_staging.claim_detail b
on a.spc_claim_id_src = b.claim_id_src and trim(a.spc_claim_sequence_number)::int = b.claim_sequence_number
where units_mismatch = 1;

/*2012	0000Q048TXE63657PH5	4	3.50
2012	0000Q324TXE44483PH5	4	3.50
2012	000015027902483P46	44	43.50
2012	0000R297TXE38476P5B	7	6.50
2012	0000P222TXE55417PW6	5	4.50
2012	0000R255TXE29103PH5	5	4.75*/

select * from dev.xz_dwqa_temp2 where units_mismatch = 1;

select * from medicaid.clm_detail where icn = '0001R325TXEA9793P86';

select * from medicaid.enc_det where derv_enc = '0001R325TXEA9793P86';

select * from dw_staging.claim_detail where claim_id_src = '200020030201831393337407';

select claim_sequence_number, claim_id_src from dw_staging.claim_detail where claim_id_src = '100031030202123043806368';


select count(*) from dev.xz_dwqa_temp1
group by spc_fy
order by spc_fy;





select count(*) from medicaid.clm_detail where length(proc_cd) = 5 and length(sub_proc_cd) = 5;
--757913819

select count(*) from medicaid.clm_detail where length(proc_cd) = 5 and length(sub_proc_cd) = 5 and proc_cd != sub_proc_cd;
--123,879,601

select icn, proc_cd, sub_proc_cd from medicaid.clm_detail where length(proc_cd) = 5 and length(sub_proc_cd) = 5 and proc_cd != sub_proc_cd;

drop table if exists dev.xz_dwqa_temp;

select proc_cd, sub_proc_cd, proc_cd || ' ' || sub_proc_cd as concatenated
into dev.xz_dwqa_temp
from medicaid.clm_detail where length(proc_cd) = 5 and length(sub_proc_cd) = 5 and proc_cd != sub_proc_cd;

select sub_proc_cd, count(*) as freq from dev.xz_dwqa_temp
group by sub_proc_cd
order by count(*) desc;

select concatenated, count(*) as freq from dev.xz_dwqa_temp
group by concatenated
order by count(*) desc;


--int check for units

select * from 
