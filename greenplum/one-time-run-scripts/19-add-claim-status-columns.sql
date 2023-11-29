/*****************************************
 * Add claim_status and claim_type_src 
 * 
 * NOT RUN YET
 */

select 'Add column to claim_header started at: ' || current_timestamp as message;

--claim_header
alter table data_warehouse.claim_header
add column claim_status varchar(2);

select 'Vacuum analyze started at: ' || current_timestamp as message;

vacuum analyze data_warehouse.claim_header;


select 'Add column to claim_detail started at: ' || current_timestamp as message;

--claim_detail
alter table data_warehouse.claim_detail
add column line_status varchar(2);

select 'Vacuum analyze started at: ' || current_timestamp as message;

vacuum analyze data_warehouse.claim_detail;

select 'Script completed at: ' || current_timestamp as message;

