
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
