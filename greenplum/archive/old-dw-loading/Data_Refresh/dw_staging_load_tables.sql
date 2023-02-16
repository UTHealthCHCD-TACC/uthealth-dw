
do $$
declare
---(1) change my_data_source according to what data is being updated, use two single quotes around each data source
---example:  my_data_source text := ' (''truv'',''mcrt'',''optz'') ';  //   := ' (''truv'') ';
	my_data_source text := ' (''optz'',''optd'') ';
begin
	
	execute 'insert into dw_staging.claim_header
		select * 
		from data_warehouse.claim_header
		where data_source not in ' || my_data_source || ';'
;

raise notice 'claim header created';

end
$$


do $$
declare
---(1) change my_data_source according to what data is being updated, use two single quotes around each data source
---example:  my_data_source text := ' (''truv'',''mcrt'',''optz'') ';  //   := ' (''truv'') ';
	my_data_source text := ' (''optz'',''optd'') ';
begin
	
execute 'insert into dw_staging.claim_diag
		select * 
		from data_warehouse.claim_diag
		where data_source not in ' || my_data_source || ';'
;
raise notice 'claim diag created';


end
$$



do $$
declare
---(1) change my_data_source according to what data is being updated, use two single quotes around each data source
---example:  my_data_source text := ' (''truv'',''mcrt'',''optz'') ';  //   := ' (''truv'') ';
	my_data_source text := ' (''optz'',''optd'') ';
begin

execute 'insert into dw_staging.claim_icd_proc
		select * 
		from data_warehouse.claim_icd_proc
		where data_source not in ' || my_data_source || ';'
;
raise notice 'claim icd proc created';

end $$ 






do $$
declare
---(1) change my_data_source according to what data is being updated, use two single quotes around each data source
---example:  my_data_source text := ' (''truv'',''mcrt'',''optz'') ';  //   := ' (''truv'') ';
	my_data_source text := ' (''optz'',''optd'') ';
begin

execute 'insert into dw_staging.pharmacy_claims
		select * 
		from data_warehouse.pharmacy_claims
		where data_source not in ' || my_data_source || ';'
;

raise notice 'pharmacy claims created';


end $$ 


do $$
declare
---(1) change my_data_source according to what data is being updated, use two single quotes around each data source
---example:  my_data_source text := ' (''truv'',''mcrt'',''optz'') ';  //   := ' (''truv'') ';
	my_data_source text := ' (''optz'',''optd'') ';
begin


execute 'insert into dw_staging.member_enrollment_monthly 
		select * 
		from data_warehouse.member_enrollment_monthly 
		where data_source not in ' || my_data_source || ';'
;

raise notice 'enrollment monthly loaded';
end $$ 


do $$
declare
---(1) change my_data_source according to what data is being updated, use two single quotes around each data source
---example:  my_data_source text := ' (''truv'',''mcrt'',''optz'') ';  //   := ' (''truv'') ';
	my_data_source text := ' (''optz'',''optd'') ';
begin


-------------insert existing records from data warehouse. except for this data source
execute 'insert into dw_staging.claim_detail 
		select * 
		from data_warehouse.claim_detail
		where data_source not in ' || my_data_source || ';'
;

raise notice 'claim_detail done';

end $$;