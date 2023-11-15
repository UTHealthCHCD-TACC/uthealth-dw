/*****************************************
 * Add claim_status and claim_type_src 
 * 
 * NOT RUN YET
 */

--claim_header
alter table data_warehouse.claim_header
add column claim_status varchar(20),
add column claim_type_code_src varchar(20);

vacuum analyze data_warehouse.claim_header;




select  from data_warehouse.claim_detail

select clm from medicaid.clm_header












