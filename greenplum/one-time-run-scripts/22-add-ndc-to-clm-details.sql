/*************************************
 * This script adds 3 columns to clm details:
 * 
 * ndc  - this is frequently named REV_CNTR_IDE_NDC_UPC_NUM	
 *     can contain NDC codes
 *     or UPC (universal product codes)
 *     or IDE (investigational device) codes - IDEs will have revenue center 0624
 * ndc_qty - this is frequently named REV_CNTR_NDC_QTY
 * ndc_unit - this is frequently named REV_CNTR_NDC_QTY_QLFR_CD
 * 
 * Note that BCARRIER and DME tables only have the NDC code without the qty or units
 */

/*************************
 * Exploratory: what tables have NDC codes?
 */

/*
select table_name, column_name
from information_schema.columns
where table_schema = 'medicare_texas' and column_name like '%ndc%'
order by table_name, column_name;

bcarrier_line_k	line_ndc_cd
dme_line_k	line_ndc_cd
hha_revenue_center_k	rev_cntr_ide_ndc_upc_num
hha_revenue_center_k	rev_cntr_ndc_qty
hha_revenue_center_k	rev_cntr_ndc_qty_qlfr_cd
hospice_revenue_center_k	rev_cntr_ide_ndc_upc_num
hospice_revenue_center_k	rev_cntr_ndc_qty
hospice_revenue_center_k	rev_cntr_ndc_qty_qlfr_cd
inpatient_revenue_center_k	rev_cntr_ide_ndc_upc_num
inpatient_revenue_center_k	rev_cntr_ndc_qty
inpatient_revenue_center_k	rev_cntr_ndc_qty_qlfr_cd
outpatient_revenue_center_k	rev_cntr_ide_ndc_upc_num
outpatient_revenue_center_k	rev_cntr_ndc_qty
outpatient_revenue_center_k	rev_cntr_ndc_qty_qlfr_cd
snf_revenue_center_k	rev_cntr_ide_ndc_upc_num
snf_revenue_center_k	rev_cntr_ndc_qty
snf_revenue_center_k	rev_cntr_ndc_qty_qlfr_cd

select * from medicare_texas.bcarrier_line_k
where line_ndc_cd is not null; -- it's all null

select * from medicare_texas.dme_line_k
where line_ndc_cd is not null; -- there are values in here

--is qty strictly numeric?

select rev_cntr_ndc_qty from medicare_texas.inpatient_revenue_center_k;

select rev_cntr_ndc_qty from medicare_texas.inpatient_revenue_center_k
where not rev_cntr_ndc_qty ~ '^[0-9]*\.?[0-9]+$'
;
--inpatient checks out

select rev_cntr_ndc_qty from medicare_texas.hha_revenue_center_k
where not rev_cntr_ndc_qty ~ '^[0-9]*\.?[0-9]+$'
;
--hha checks out

select rev_cntr_ndc_qty from medicare_texas.outpatient_revenue_center_k
where not rev_cntr_ndc_qty ~ '^[0-9]*\.?[0-9]+$'
;
--outpatient checks out. Let's assume they all check out
*/

/*********************************
 * Actually add columns
********************************/

alter table data_warehouse.claim_detail
add column ndc text,
add column ndc_qty float,
add column ndc_unit text
;

