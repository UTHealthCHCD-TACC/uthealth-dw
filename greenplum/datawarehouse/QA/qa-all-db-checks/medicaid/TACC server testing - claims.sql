


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













