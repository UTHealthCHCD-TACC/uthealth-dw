/* ******************************************************************************************************
 *  Convenience code to move a table to another schema
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wallingTACC  || 1/1/2019 || script created
 * ******************************************************************************************************
 * 
*/

ALTER TABLE dw_qa.claim_detail_diag_by_claim
SET SCHEMA dev;

create table dw_qa.claim_header (like data_warehouse.claim_header);
insert into dw_qa.claim_header
select * from data_warehouse.claim_header;