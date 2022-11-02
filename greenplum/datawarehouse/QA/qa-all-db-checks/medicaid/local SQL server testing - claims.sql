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









