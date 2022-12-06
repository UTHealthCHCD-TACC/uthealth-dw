/* This script is for testing code for the local SQL server before bringing it into R
 * for dw_staging claims QA
 */

/******************************************
 * Simple look at tables in question
 *****************************************/
SELECT top 5 * FROM medicaid.dbo.CLM_HEADER_12;
SELECT top 5 * FROM medicaid.dbo.CLM_DETAIL_12;
SELECT top 5 * FROM medicaid.dbo.CLM_PROC_12;
SELECT top 5 * FROM medicaid.dbo.CLM_DX_12;

/******************************************
 * Build table of claims data on a yearly basis
 * 
 * --because claims are useless without member IDs, we join everything to proc
 *****************************************/

SELECT a.ICN as ICN_PROC, b.ICN as ICN_HEAD, c.ICN as ICN_DET, d.ICN as ICN_DX
into work.dbo.xz_dwqa_clm_12
from medicaid.dbo.CLM_PROC_12 a full join
	medicaid.dbo.CLM_HEADER_12 b on a.icn = b.icn full JOIN 
	medicaid.dbo.CLM_DETAIL_12 c on a.icn = c.icn full JOIN 
	medicaid.dbo.CLM_DX_12 d on a.icn = d.icn;

SET STATISTICS TIME ON;

select count(distinct a.icn)
from medicaid.dbo.CLM_PROC_12 a full join
	medicaid.dbo.CLM_HEADER_12 b on a.icn = b.icn


	
	
select count(distinct a.icn)
from medicaid.dbo.clm_proc_13 a inner join medicaid.dbo.enc_proc_13 b
on a.icn = b.DERV_ENC
	
select count(distinct a.derv_enc)
from medicaid.dbo.enc_proc_13 a inner join medicaid.dbo.enc_det_13 b
on a.derv_enc = b.derv_enc;


--htw

select count(distinct a.icn)
from medicaid.dbo.clm_proc_1819_htw a inner join medicaid.dbo.clm_detail_1819_htw b
on a.icn = b.icn;

select count(distinct a.icn)
from medicaid.dbo.clm_proc_1819_htw a inner join medicaid.dbo.clm_header_1819_htw b
on a.icn = b.icn;

/**************************************************
 *  SPOT CHECKING!!! WOOT WOOT
 **************************************************/

/*claim_diag columns: data_source, uth_member_id, uth_claim_id, from_date_of_service,
 * diag_cd, diag_position, poa_src (present on admission?), icd_version, load_date, year, fiscal_year
 */

select top 10 * from medicaid.dbo.clm_dx_12;

select top 10 * from medicaid.dbo.clm_dx_13;

select ICN, PRIM_DX_CD, DX_CD_1 from medicaid.dbo.clm_dx_12
where PRIM_DX_CD != DX_CD_1;

select count(*) from medicaid.dbo.clm_dx_12
where PRIM_DX_CD != DX_CD_1;

drop table if exists work.dbo.xz_dwqa_temp1;

select top 1000 ICN, PRIM_DX_QAL, PRIM_DX_CD,
	DX_CD_QUAL_1, DX_CD_1, DX_CD_QUAL_2, DX_CD_2,
	DX_CD_QUAL_3, DX_CD_3, DX_CD_QUAL_4, DX_CD_4,
	DX_CD_QUAL_5, DX_CD_5, DX_CD_QUAL_6, DX_CD_6,
	DX_CD_QUAL_7, DX_CD_7, DX_CD_QUAL_8, DX_CD_8,
	DX_CD_QUAL_9, DX_CD_9, DX_CD_QUAL_10, DX_CD_10,
	DX_CD_QUAL_11, DX_CD_11, DX_CD_QUAL_12, DX_CD_12,
	DX_CD_QUAL_13, DX_CD_13, DX_CD_QUAL_14, DX_CD_14,
	DX_CD_QUAL_15, DX_CD_15, DX_CD_QUAL_16, DX_CD_16,
	DX_CD_QUAL_17, DX_CD_17, DX_CD_QUAL_18, DX_CD_18,
	DX_CD_QUAL_19, DX_CD_19, DX_CD_QUAL_20, DX_CD_20,
	DX_CD_QUAL_21, DX_CD_21, DX_CD_QUAL_22, DX_CD_22,
	DX_CD_QUAL_23, DX_CD_23, DX_CD_QUAL_24, DX_CD_24,
	DX_CD_QUAL_25, DX_CD_25
into work.dbo.xz_dwqa_temp1 
from medicaid.dbo.clm_dx_12 
tablesample(1200 rows);

select b.pcn, c.FROM_DOS as dos, a.*
into work.dbo.xz_dwqa_temp2
from work.dbo.xz_dwqa_temp1 a left join
	medicaid.dbo.clm_proc_12 b on a.icn = b.icn left join
	medicaid.dbo.clm_detail_12 c on a.icn = c.icn;

select top 5 * from work.dbo.xz_dwqa_temp2;

--from encounters

select top 10 *
from medicaid.dbo.enc_dx_13;

drop table if exists work.dbo.xz_dwqa_temp3;

select top 1000 DERV_ENC, PRIM_DX_QAL, PRIM_DX_CD,
	DX_CD_QAL_1, DX_CD_1, DX_CD_QAL_2, DX_CD_2,
	DX_CD_QAL_3, DX_CD_3, DX_CD_QAL_4, DX_CD_4,
	DX_CD_QAL_5, DX_CD_5, DX_CD_QAL_6, DX_CD_6,
	DX_CD_QAL_7, DX_CD_7, DX_CD_QAL_8, DX_CD_8,
	DX_CD_QAL_9, DX_CD_9, DX_CD_QAL_10, DX_CD_10,
	DX_CD_QAL_11, DX_CD_11, DX_CD_QAL_12, DX_CD_12,
	DX_CD_QAL_13, DX_CD_13, DX_CD_QAL_14, DX_CD_14,
	DX_CD_QAL_15, DX_CD_15, DX_CD_QAL_16, DX_CD_16,
	DX_CD_QAL_17, DX_CD_17, DX_CD_QAL_18, DX_CD_18,
	DX_CD_QAL_19, DX_CD_19, DX_CD_QAL_20, DX_CD_20,
	DX_CD_QAL_21, DX_CD_21, DX_CD_QAL_22, DX_CD_22,
	DX_CD_QAL_23, DX_CD_23, DX_CD_QAL_24, DX_CD_24,
	'' as DX_CD_QAL_25, '' as DX_CD_25
into work.dbo.xz_dwqa_temp3 
from medicaid.dbo.enc_dx_13
tablesample(1200 rows);

insert into work.dbo.xz_dwqa_temp2
select b.MEM_ID, c.FDOS_DT as dos, a.*
from work.dbo.xz_dwqa_temp3 a left join
	medicaid.dbo.enc_proc_13 b on a.derv_enc = b.derv_enc left join
	medicaid.dbo.enc_det_13 c on a.derv_enc = c.derv_enc;

select * from work.dbo.xz_dwqa_temp2 where icn = '100020030201314345517976';

select * from medicaid.dbo.enc_dx_13 where DERV_ENC = '0000DD1001356424530002D1J';


select * from medicaid.dbo.clm_detail_13 where icn = '100020030201314345517976';
select * from medicaid.dbo.clm_header_13 where icn = '100020030201314345517976';

select * from medicaid.dbo.clm_detail_12 where icn = '100020030201314345517976';


select a.ICN, b.FROM_DOS, c.HDR_FRM_DOS
into work.dbo.xz_dwqa_temp4
from medicaid.dbo.clm_dx_13 a left join medicaid.dbo.clm_detail_13 b on a.icn = b.ICN 
left join medicaid.dbo.clm_header_13 c on a.icn = c.icn;


select a.ICN, b.FROM_DOS, c.HDR_FRM_DOS
from medicaid.dbo.clm_dx_13 a left join medicaid.dbo.clm_detail_13 b on a.icn = b.ICN 
left join medicaid.dbo.clm_header_13 c on a.icn = c.icn
where a.icn = '200031030201326076578721';

--now for procs

drop table if exists work.dbo.xz_dwqa_temp1;
drop table if exists work.dbo.xz_dwqa_temp2;


select top 1000 ICN, PCN, PROC_ICD_QAL_1, PROC_ICD_CD_1,
	PROC_ICD_QAL_2, PROC_ICD_CD_2,
	PROC_ICD_QAL_3, PROC_ICD_CD_3,
	PROC_ICD_QAL_4, PROC_ICD_CD_4,
	PROC_ICD_QAL_5, PROC_ICD_CD_5,
	PROC_ICD_QAL_6, PROC_ICD_CD_6,
	PROC_ICD_QAL_7, PROC_ICD_CD_7,
	PROC_ICD_QAL_8, PROC_ICD_CD_8,
	PROC_ICD_QAL_9, PROC_ICD_CD_9,
	PROC_ICD_QAL_10, PROC_ICD_CD_10,
	PROC_ICD_QAL_11, PROC_ICD_CD_11,
	PROC_ICD_QAL_12, PROC_ICD_CD_12,
	PROC_ICD_QAL_13, PROC_ICD_CD_13,
	PROC_ICD_QAL_14, PROC_ICD_CD_14,
	PROC_ICD_QAL_15, PROC_ICD_CD_15,
	PROC_ICD_QAL_16, PROC_ICD_CD_16,
	PROC_ICD_QAL_17, PROC_ICD_CD_17,
	PROC_ICD_QAL_18, PROC_ICD_CD_18,
	PROC_ICD_QAL_19, PROC_ICD_CD_19,
	PROC_ICD_QAL_20, PROC_ICD_CD_20,
	PROC_ICD_QAL_21, PROC_ICD_CD_21,
	PROC_ICD_QAL_22, PROC_ICD_CD_22,
	PROC_ICD_QAL_23, PROC_ICD_CD_23,
	PROC_ICD_QAL_24, PROC_ICD_CD_24,
	PROC_ICD_QAL_25, PROC_ICD_CD_25
into work.dbo.xz_dwqa_temp1 
from medicaid.dbo.clm_proc_13
tablesample(1200 rows);

select b.FROM_DOS as dos, a.*
into work.dbo.xz_dwqa_temp2
from work.dbo.xz_dwqa_temp1 a left join
	medicaid.dbo.clm_detail_12 b on a.icn = b.icn;

select top 5 * from work.dbo.xz_dwqa_temp2;

--from encounters

select top 10 *
from medicaid.dbo.enc_dx_13;

drop table if exists work.dbo.xz_dwqa_temp3;

select top 1000 DERV_ENC, MEM_ID, PRIM_PROC_QAL, PRIM_PROC_CD,
	PROC_ICD_QAL_1, PROC_ICD_CD_1,
	PROC_ICD_QAL_2, PROC_ICD_CD_2,
	PROC_ICD_QAL_3, PROC_ICD_CD_3,
	PROC_ICD_QAL_4, PROC_ICD_CD_4,
	PROC_ICD_QAL_5, PROC_ICD_CD_5,
	PROC_ICD_QAL_6, PROC_ICD_CD_6,
	PROC_ICD_QAL_7, PROC_ICD_CD_7,
	PROC_ICD_QAL_8, PROC_ICD_CD_8,
	PROC_ICD_QAL_9, PROC_ICD_CD_9,
	PROC_ICD_QAL_10, PROC_ICD_CD_10,
	PROC_ICD_QAL_11, PROC_ICD_CD_11,
	PROC_ICD_QAL_12, PROC_ICD_CD_12,
	PROC_ICD_QAL_13, PROC_ICD_CD_13,
	PROC_ICD_QAL_14, PROC_ICD_CD_14,
	PROC_ICD_QAL_15, PROC_ICD_CD_15,
	PROC_ICD_QAL_16, PROC_ICD_CD_16,
	PROC_ICD_QAL_17, PROC_ICD_CD_17,
	PROC_ICD_QAL_18, PROC_ICD_CD_18,
	PROC_ICD_QAL_19, PROC_ICD_CD_19,
	PROC_ICD_QAL_20, PROC_ICD_CD_20,
	PROC_ICD_QAL_21, PROC_ICD_CD_21,
	PROC_ICD_QAL_22, PROC_ICD_CD_22,
	PROC_ICD_QAL_23, PROC_ICD_CD_23,
	PROC_ICD_QAL_24, PROC_ICD_CD_24
into work.dbo.xz_dwqa_temp3 
from medicaid.dbo.enc_proc_13
tablesample(1200 rows);

insert into work.dbo.xz_dwqa_temp2
select b.FDOS_DT as dos, a.*
from work.dbo.xz_dwqa_temp3 a left join
	medicaid.dbo.enc_proc_13 b on a.derv_enc = b.derv_enc left join
	medicaid.dbo.enc_det_13 c on a.derv_enc = c.derv_enc;



select * from medicaid.dbo.clm_proc_13 where icn = '200050030201332696272753';

select * from medicaid.dbo.enc_proc_13 where DERV_ENC = '00000006013695940I8H';

select count(*)from medicaid.dbo.enc_proc_13; --54,031,309
select count(*) from medicaid.dbo.enc_proc_13 where PRIM_PROC_CD != '';

select * from medicaid.dbo.enc_proc_13 where PRIM_PROC_CD != '';


select icn, 'clm_proc_12' as tbl_name, PROC_ICD_CD_5 from medicaid.dbo.clm_proc_12
where proc_icd_cd_5 is not null and trim(proc_icd_cd_5) != '';

select derv_enc, 'enc_proc_13' as tbl_name, PROC_ICD_CD_5 from medicaid.dbo.enc_proc_13
where proc_icd_cd_5 is not null and trim(proc_icd_cd_5) != '';

select icn, 'clm_proc_1819_HTW' as tbl_name, PROC_ICD_CD_5 from medicaid.dbo.clm_proc_1819_HTW
where proc_icd_cd_5 is not null and trim(proc_icd_cd_5) != '';


drop table if exists work.dbo.xz_mcd_clm_proc5;
drop table if exists work.dbo.xz_mcd_enc_proc5;

select icn, 'clm_proc_12' as tbl_name, PROC_ICD_CD_5
into work.dbo.xz_mcd_clm_proc5
from medicaid.dbo.clm_proc_12
where proc_icd_cd_5 is not null and trim(proc_icd_cd_5) != '';

select derv_enc, 'enc_proc_13' as tbl_name, PROC_ICD_CD_5
into work.dbo.xz_mcd_enc_proc5
from medicaid.dbo.enc_proc_13
where proc_icd_cd_5 is not null and trim(proc_icd_cd_5) != '';


select * from medicaid.dbo.enc_proc_21 where derv_enc = '00001008040474IN4'

select count(*) from work.dbo.xz_mcd_clm_proc5;
select count(*) from work.dbo.xz_mcd_enc_proc5;



select * from medicaid.dbo.clm_header_16 where icn = '100050061201824971411214';







SELECT top 5 * FROM medicaid.dbo.CLM_HEADER_12;
SELECT top 5 * FROM medicaid.dbo.CLM_detail_12; --has pos, get max pos group by icn

SELECT top 5 * FROM medicaid.dbo.CLM_HEADER_12 where trim(hdr_txm_cd) != '';
select top 5 * from medicaid.dbo.enc_header_13 where trim(TX_CD) != '';

--clm
with p as (select max(pos) as pos, icn from medicaid.dbo.clm_detail_12 group by icn)
select top 5 a.hdr_frm_dos as from_dos,
	case when p.pos = '1' then 'P' else 'F' end as claim_type,
	a.tot_bill_amt as tot_charge_amt,
	a.TOT_ALWD_AMT as tot_allowed_amt,
	a.hdr_pd_amt as tot_paid_amt,
	'2012' as fy,
	a.hdr_to_dos as to_dos,
	a.BIL_PROV_NPI as bill_provider,
	a.atd_prov_npi as ref_provider,
	null as perf_at_provider,
	a.icn as icn,
	a.hdr_txm_cd as provider_type
from medicaid.dbo.clm_header_12 a tablesample(1000 rows)
	left join p on a.icn = p.icn;

--enc
select top 5 FRM_DOS,
	case when TX_CD = 'P' then 'P'
		when TX_CD = 'I' then 'F'
		else null end as claim_type,
	TOT_CHRG_AMT as tot_charge_amt,
	MCO_PD_AMT as tot_allowed_amt,
	null as tot_paid_amt,
	'2013' as fy,
	to_dos as to_dos,
	BILL_PROV_NPI as bill_provider,
	null as ref_provider,
	ATTD_PHY_NPI as perf_at_provider,
	DERV_ENC as icn,
	BILL_PROV_TAX_CD as provider_type
from medicaid.dbo.enc_header_13 tablesample(1000 rows);

--htw
with p as (select max(pos) as pos, icn from medicaid.dbo.clm_detail_1819_htw group by icn)
select top 5 a.hdr_frm_dos as from_dos,
	case when p.pos = '1' then 'P' else 'F' end as claim_type,
	a.tot_bill_amt as tot_charge_amt,
	a.TOT_ALWD_AMT as tot_allowed_amt,
	a.hdr_pd_amt as tot_paid_amt,
	'2012' as fy,
	a.hdr_to_dos as to_dos,
	a.BILL_PROV_NPI as bill_provider,
	a.atd_prov_npi as ref_provider,
	null as perf_at_provider,
	a.icn as icn,
	a.hdr_txm_cd as provider_type
from medicaid.dbo.CLM_header_1819_HTW a tablesample(1000 rows)
	left join p on a.icn = p.icn;



select bill_prov_npi from medicaid.dbo.clm_header_21;


--details now

select top 5 * from medicaid.dbo.clm_detail_12;
select top 5 * from medicaid.dbo.clm_proc_12;

drop table if exists work.dbo.xz_dwqa_temp1;

select top 10  '2012' as spc_fy,
a.clm_dtl_nbr as spc_claim_sequence_number,
a.from_dos as spc_from_date_of_service,
a.to_dos as spc_to_date_of_service,
a.from_dos as spc_place_of_service,
a.proc_cd as spc_cpt_hcpcs_cd,
a.proc_mod_1 as spc_proc_mod_1,
a.proc_mod_2 as spc_proc_mod_2,
a.rev_cd as spc_revenue_cd,
a.dtl_bill_amt as spc_charge_amount,
a.dtl_alwd_amt as spc_allowed_amount,
a.dtl_pd_amt as spc_paid_amount,
a.ref_prov_npi as spc_ref_provider,
a.perf_prov_npi as spc_perf_rn_provider,
"" as spc_perf_op_provider,
a.txm_cd as spc_provider_type,
b.adm_dt as spc_admit_date,
b.dis_dt as spc_discharge_date,
b.pat_stat_cd as spc_discharge_status,
b.icn as spc_claim_id_src,
c.drg as spc_drg_cd,
substring(c.bill, 1, 1) as spc_bill_type_inst,
substring(c.bill, 1, 2) as spc_bill_type_class,
substring(c.bill, 1, 3) as spc_bill_type_freq,
"" as spc_units,
c.pcn as spc_member_id_src
into work.dbo.xz_dwqa_temp1
from medicaid.dbo.clm_detail_12 a tablesample(100 rows) inner join
	medicaid.dbo.clm_header_12 b on a.icn = b.icn inner join 
	medicaid.dbo.clm_proc_12 c on a.icn = c.icn;

--Make spc_perf_op_provider and spc_units into varchar(50)
alter table work.dbo.xz_dwqa_temp1
alter column spc_perf_op_provider varchar(50);

alter table work.dbo.xz_dwqa_temp1
alter column spc_units varchar(50);


insert into work.dbo.xz_dwqa_temp1
select top 10  '2012' as spc_fy,
a.ln_nbr as spc_claim_sequence_number,
a.fdos_dt  as spc_from_date_of_service,
a.tdos_csl  as spc_to_date_of_service,
a.pos  as spc_place_of_service,
a.proc_cd  as spc_cpt_hcpcs_cd,
a.proc_mod_cd_1  as spc_proc_mod_1,
a.proc_mod_cd_2  as spc_proc_mod_2,
a.rev_cd  as spc_revenue_cd,
a.sub_chrg_amt  as spc_charge_amount,
a.dt_pd_amt  as spc_allowed_amount,
NULL as spc_paid_amount,
a.sub_ref_prov_npi  as spc_ref_provider,
a.sub_rend_prov_npi  as spc_perf_rn_provider,
a.sub_opt_phy_npi as spc_perf_op_provider,
a.sub_rend_prv_tax_cd  as spc_provider_type,
b.adm_dt  as spc_admit_date,
b.dis_dt  as spc_discharge_date,
b.pat_stat  as spc_discharge_status,
b.derv_enc  as spc_claim_id_src,
c.drg  as spc_drg_cd,
substring(c.bill, 1, 1) as spc_bill_type_inst,
substring(c.bill, 1, 2) as spc_bill_type_class,
substring(c.bill, 1, 3) as spc_bill_type_freq,
a.dt_ln_unt as spc_units,
c.mem_id  as spc_member_id_src

from medicaid.dbo.enc_det_13 a tablesample(100 rows) inner join
	medicaid.dbo.enc_header_13 b on a.derv_enc = b.derv_enc inner join 
	medicaid.dbo.enc_proc_13 c on a.derv_enc = c.derv_enc;


select top 5 * from medicaid.dbo.enc_proc_13;


select 

























