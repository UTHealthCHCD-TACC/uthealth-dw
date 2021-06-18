drop function trg_create_set_owner();
CREATE OR REPLACE FUNCTION trg_create_set_owner()
 RETURNS event_trigger
 LANGUAGE plpgsql
AS $$
DECLARE
  obj record;
BEGIN
  FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands() WHERE command_tag='CREATE TABLE' LOOP
    EXECUTE format('ALTER TABLE %s OWNER TO uthealth_admin', obj.object_identity);
  END LOOP;
END;
$$;

drop event trigger trg_create_set_owner;
CREATE EVENT TRIGGER trg_create_set_owner
 ON ddl_command_end
 WHEN tag IN ('CREATE TABLE')
 EXECUTE PROCEDURE trg_create_set_owner();
 
create table dev.dw_test (like dev.am_claim_header);