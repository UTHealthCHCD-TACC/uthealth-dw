/* 
******************************************************************************************************
 *  Adds functions in a dbo schema allowing non-superusers the ability to perform a subset of superuser activities.
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wallingTACC  ||8/25/2021 || comments added
 * ******************************************************************************************************
 */

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

-- Allow ownership change
CREATE OR REPLACE FUNCTION dbo.change_owner(table_name varchar, owner varchar)
RETURNS boolean AS $body$
DECLARE
    result boolean;
BEGIN
    result := (ALTER TABLE table_name owner TO owner);
    RETURN result;
END;
$body$
    LANGUAGE plpgsql
    SECURITY DEFINER
    VOLATILE
    RETURNS NULL ON NULL INPUT
    SET search_path = pg_catalog;

grant execute on function dbo.pg_kill_connection(pid integer) to uthealthadmin;


CREATE OR REPLACE FUNCTION dbo.pg_cancel_backend(pid integer)
RETURNS boolean AS $body$
DECLARE
    result boolean;
BEGIN
    result := (select pg_catalog.pg_cancel_backend(pid));
    RETURN result;
END;
$body$
    LANGUAGE plpgsql
    SECURITY DEFINER
    VOLATILE
    RETURNS NULL ON NULL INPUT
    SET search_path = pg_catalog;

grant execute on function dbo.pg_cancel_backend(pid integer) to uthealthadmin;

CREATE FUNCTION dbo.get_sa() RETURNS SETOF pg_stat_activity AS
$$ SELECT * FROM pg_catalog.pg_stat_activity; $$
LANGUAGE sql
VOLATILE
SECURITY DEFINER;


CREATE VIEW dbo.pg_stat_activity AS SELECT * FROM dbo.get_sa();

grant select on dbo.pg_stat_activity to uthealthadmin;


CREATE or replace FUNCTION dbo.set_table_perms(role varchar, perm_level varchar) RETURNS boolean AS
$body$
BEGIN
FOR table_name IN 
SELECT schemaname||'.'||tablename AS table_name 
FROM pg_tables 
WHERE schemaname in ('data_warehouse', 'dev', 'dev2016', 'optum_zip', 'optum_zip', 'tableau', 'truven', 'reference_tables', 'medicare')
LOOP
RAISE NOTICE 'Setting permissions for %', table_name;
EXECUTE 'grant ' || perm_level || ' on ' || table_name || ' to ' || role;
END LOOP;
END;
$body$
LANGUAGE plpgsql
VOLATILE
SECURITY definer;

grant execute on function dbo.set_table_perms() to uthealthadmin;


CREATE or replace FUNCTION dbo.set_sequence_perms() RETURNS boolean AS
$body$
DECLARE
  r record;
  seq_name TEXT;
BEGIN
FOR r in 
SELECT schemaname||'.'||relname AS s FROM pg_catalog.pg_statio_all_sequences WHERE schemaname in ('data_warehouse', 'dev', 'dev2016', 'optum_zip', 'optum_zip', 'tableau', 'truven', 'reference_tables', 'medicare')
loop
seq_name := r.s;
RAISE NOTICE 'Setting permissions for %', seq_name;
EXECUTE 'grant all on ' || seq_name || ' to uthealthadmin';
END LOOP;
return true;
END;
$body$
LANGUAGE plpgsql
VOLATILE
SECURITY definer

grant execute on function dbo.set_sequence_perms() to uthealthadmin;

select dbo.set_sequence_perms()

select * from dev.claim_detail_v1_id_seq;




------------------------------------------ All Perms
create or replace function dbo.set_all_perms()  RETURNS boolean as
$body$
BEGIN
PERFORM  dbo.set_table_perms();
PERFORM  dbo.set_sequence_perms();
RETURN true;
END;
$body$
LANGUAGE plpgsql
VOLATILE
SECURITY definer

grant execute on function dbo.set_all_perms() to uthealthadmin;