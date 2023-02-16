CREATE OR REPLACE FUNCTION dev.function_monitoring_test()
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$

begin
	
------------ /  BEGIN SCRIPT
	
raise notice 'begin script';

perform pg_sleep(5);

raise notice 'phase 2';

perform pg_sleep(3);

raise notice 'end script';

end 
$$
EXECUTE ON ANY;

select dev.function_monitoring_test()