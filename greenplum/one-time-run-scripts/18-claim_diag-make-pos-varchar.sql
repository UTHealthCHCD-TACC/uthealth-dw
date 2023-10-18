/****************************************
 * Make the diag_position column in claim_diag varchar(2) instead of int4
 * so that we can accomodate Primary diagnosis, Admitting diagnosis, and Discharge diagnosis
 * (not in DW but part of APCD)
 * 
 * 10/17/23 Xiaorui
 * 
 * 12 mins data type change 2 mins vac analyze
 */

select 'DW claim_diag diag_position data type change started at: ' || current_timestamp as message;

alter table data_warehouse.claim_diag
alter column diag_position type varchar(2);

select 'vacuum analyze started at: ' || current_timestamp as message;

vacuum analyze data_warehouse.claim_diag;

select 'Finished: ' || current_timestamp as message;
