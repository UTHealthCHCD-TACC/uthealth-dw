/* ******************************************************************************************************
 *  add provider columns to claim detail tables 
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 * ******************************************************************************************************
 *  jw001  || 11/3/2021 ||  create script
 * ****************************************************************************************************** 
 * */

alter table dw_staging.claim_detail
add column bill_provider varchar,
add column ref_provider varchar,
add column other_provider varchar,
add column perf_rn_provider varchar,
add column perf_at_provider varchar,
add column perf_op_provider varchar
;

alter table dw_staging.claim_header 
add column bill_provider varchar,
add column ref_provider varchar,
add column other_provider varchar,
add column perf_rn_provider varchar,
add column perf_at_provider varchar,
add column perf_op_provider varchar
;

