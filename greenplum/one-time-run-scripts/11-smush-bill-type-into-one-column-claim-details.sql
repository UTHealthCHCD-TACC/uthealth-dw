/*****************
 * Small blurb of script that smushes bill type into one column for claim details
 */

/*
alter table data_warehouse.claim_detail
add column bill varchar(4); */

select 'Filling in bill type started at: ' || current_timestamp as message;

update data_warehouse.claim_detail
set bill = bill_type_inst || bill_type_class || bill_type_freq;

select 'bill type fill-in completed; vacuum analyze started at: ' || current_timestamp as message;

vacuum analyze data_warehouse.claim_detail;













