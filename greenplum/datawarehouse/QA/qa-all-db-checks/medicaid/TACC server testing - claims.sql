


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