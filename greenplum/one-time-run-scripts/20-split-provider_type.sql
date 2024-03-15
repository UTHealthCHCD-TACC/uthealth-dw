/****************************************
 * This script renames provider_type to provider_taxonomy and adds in a provider_specialty column
 * to claim_detail and claim_header
 * 
 * This script optimized for psql (bc we know it's gonna take A WHILE)
 */

/*******CLAIM HEADER********/

select 'Splitting out provider_type in claim_header' as message;

select 'Add columns: ' || current_timestamp as message;
--add columns (9 mins)
alter table data_warehouse.claim_header
add column provider_taxonomy varchar(11),
add column provider_specialty varchar(20);

select 'Populate provider taxonomy: ' || current_timestamp as message;
--move data into provider_taxonomy (1 min)
update data_warehouse.claim_header
set provider_taxonomy = provider_type
where data_source in ('mdcd', 'mcpp', 'mhtw');

select 'Populate provider specialty: ' || current_timestamp as message;
--move data into provider_specialty (5.5 mins)
update data_warehouse.claim_header
set provider_specialty = provider_type
where data_source in ('truc', 'trum');

select 'Drop provider type: ' || current_timestamp as message;
--drop provider_type (instantaneous)
alter table data_warehouse.claim_header
drop column provider_type;

select 'Vacuum analyze: ' || current_timestamp as message;
--vacuum analyze (2 mins)
vacuum analyze data_warehouse.claim_header;

/*******CLAIM DETAIL********/
select 'Splitting out provider_type in claim_detail' as message;

select 'Add columns: ' || current_timestamp as message;
--add columns
alter table data_warehouse.claim_detail
add column provider_taxonomy varchar(11),
add column provider_specialty varchar(20);

select 'Populate provider taxonomy: ' || current_timestamp as message;
--move data into provider_taxonomy
update data_warehouse.claim_detail
set provider_taxonomy = provider_type
where data_source in ('mdcd', 'mcpp', 'mhtw');

select 'Populate provider specialty: ' || current_timestamp as message;
--move data into provider_specialty
update data_warehouse.claim_detail
set provider_specialty = provider_type
where data_source in ('truc', 'trum', 'mcrn', 'mcrt');

select 'Drop provider type: ' || current_timestamp as message;
--drop provider_type
alter table data_warehouse.claim_header
drop column provider_type;

select 'Vacuum analyze: ' || current_timestamp as message;
--vacuum analyze
vacuum analyze data_warehouse.claim_detail;

select 'Script completed at: ' || current_timestamp as message;
