
CREATE SCHEMA dbo;

grant usage on schema dbo to uthealthadmin;


-- kills any postgresql connection (using its PID)
CREATE OR REPLACE FUNCTION dbo.pg_kill_connection(pid integer)
RETURNS boolean AS $body$
DECLARE
    result boolean;
BEGIN
    result := (select pg_catalog.pg_terminate_backend(pid));
    RETURN result;
END;
$body$
    LANGUAGE plpgsql
    SECURITY DEFINER
    VOLATILE
    RETURNS NULL ON NULL INPUT
    SET search_path = pg_catalog;


grant execute on function dbo.pg_kill_connection(pid integer) to uthealthadmin;

CREATE FUNCTION dbo.get_sa() RETURNS SETOF pg_stat_activity AS
$$ SELECT * FROM pg_catalog.pg_stat_activity; $$
LANGUAGE sql
VOLATILE
SECURITY DEFINER;


CREATE VIEW dbo.pg_stat_activity AS SELECT * FROM dbo.get_sa();

grant select on dbo.pg_stat_activity to uthealthadmin;


CREATE FUNCTION dbo.set_table_perms() RETURNS boolean AS
$body$
BEGIN
FOR table_name IN SELECT schemaname||'.'||tablename AS table_name FROM pg_tables WHERE schemaname in ('data_warehouse', 'dev', 'dev2016', 'optum_dod', 'optum_zip', 'tableau', 'truven', 'reference_tables', 'medicare')LOOP
RAISE NOTICE 'Setting permissions for %', table_name;
EXECUTE 'grant all on ' || table_name || ' to uthealthadmin';
END LOOP;
END;
$body$;
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER;

grant execute on function dbo.set_table_perms() to uthealthadmin;

