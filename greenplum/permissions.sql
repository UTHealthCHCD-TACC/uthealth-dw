revoke all on schema public from public;




---making a note 
/*
 * UTHealth Admin Role
 */
create role uthealthadmin;
grant connect on database uthealth to group uthealthdev;

grant all on database uthealth to uthealthadmin;

--Schemas
grant all on schema truven to group uthealthadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA truven grant all on tables to group uthealthadmin; 

grant all on schema optum_dod_refresh to group uthealthadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA optum_dod_refresh grant all on tables to group uthealthadmin; 
GRANT all ON ALL TABLES IN SCHEMA optum_dod_refresh TO uthealthadmin;

grant all on schema optum_zip_refresh to group uthealthadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA optum_zip_refresh grant all on tables to group uthealthadmin; 
GRANT all ON ALL TABLES IN SCHEMA optum_zip_refresh TO uthealthadmin;

grant all on schema reference_tables to group uthealthadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA reference_tables grant all on tables to group uthealthadmin; 

grant all on schema data_warehouse to group uthealthadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_warehouse grant all on tables to group uthealthadmin; 

grant all on schema medicare to group uthealthadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA medicare grant all on tables to group uthealthadmin; 

grant all on schema dev to group uthealthadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA dev grant all on tables to group uthealthadmin; 

grant all on schema dev2016 to group uthealthadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA dev2016 grant all on tables to group uthealthadmin; 


/*
 * UTHealth Dev Role
 */

create role uthealthdev;
grant connect on database uthealth to group uthealthdev;


--Schemas

--truven
grant usage on schema truven to group uthealthdev;
grant select on all tables in schema truven to group uthealthdev;
ALTER DEFAULT PRIVILEGES IN SCHEMA truven grant select on tables to group uthealthdev; 

--optum_dod
grant usage on schema optum_dod_refresh to group uthealthdev;
grant select on all tables in schema optum_dod_refresh to group uthealthdev;
ALTER DEFAULT PRIVILEGES IN SCHEMA optum_dod_refresh grant select on tables to group uthealthdev; 

--optum_zip
grant usage on schema optum_zip_refresh to group uthealthdev;
grant select on all tables in schema optum_zip_refresh to group uthealthdev;
ALTER DEFAULT PRIVILEGES IN SCHEMA optum_zip_refresh grant select on tables to group uthealthdev; 

--reference_tables
grant usage on schema reference_tables to group uthealthdev;
grant select on all tables in schema reference_tables to group uthealthdev;
ALTER DEFAULT PRIVILEGES IN SCHEMA reference_tables grant select on tables to group uthealthdev; 

--data_warehouse
grant usage on schema data_warehouse to group uthealthdev;
grant select on all tables in schema data_warehouse to group uthealthdev;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_warehouse grant select on tables to group uthealthdev; 

--medicare
grant usage on schema medicare to group uthealthdev;
grant select on all tables in schema medicare to group uthealthdev;
ALTER DEFAULT PRIVILEGES IN SCHEMA medicare grant select on tables to group uthealthdev; 

--dev
grant usage on schema dev to group uthealthdev;
grant select on all tables in schema dev to group uthealthdev;
ALTER DEFAULT PRIVILEGES IN SCHEMA dev grant select on tables to group uthealthdev; 

--dev2016
grant usage on schema dev2016 to group uthealthdev;
grant select on all tables in schema dev2016 to group uthealthdev;
ALTER DEFAULT PRIVILEGES IN SCHEMA dev2016 grant select on tables to group uthealthdev; 

grant usage on schema tableau to group uthealthdev;
grant select on all tables in schema tableau to group uthealthdev;
ALTER DEFAULT PRIVILEGES IN SCHEMA tableau grant select on tables to group uthealthdev; 

/*
 * UTHealth Analyst Role
 */

CREATE ROLE smadhuri NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT LOGIN PASSWORD 'password';

CREATE ROLE chautruong NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT LOGIN PASSWORD 'password';

CREATE ROLE yliu26 NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT LOGIN PASSWORD 'password';

grant analyst to cms2;

grant analyst to smadhuri;

grant analyst to chautruong;

grant analyst to yliu26;


-- analyst role definition
drop owned by analyst cascade;

drop role analyst;

create role analyst;

grant connect on database uthealth to analyst;

--Schemas
--reference_tables (select only)
grant usage on schema reference_tables to group analyst;
grant select on all tables in schema reference_tables to group analyst;
ALTER DEFAULT PRIVILEGES IN SCHEMA reference_tables grant select on tables to group analyst; 

--data_warehouse (select only) 
grant usage on schema data_warehouse to group analyst;
grant select on all tables in schema data_warehouse to group analyst;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_warehouse grant select on tables to group analyst; 


--tableau (select only)
grant usage on schema tableau to group analyst;
grant select on all tables in schema tableau to group analyst;
ALTER DEFAULT PRIVILEGES IN SCHEMA tableau grant select on tables to group analyst; 

--dev (all access)
grant all on schema dev to analyst;

grant all on all tables in schema dev to analyst;

grant all privileges on all sequences in schema dev to analyst;

alter default privileges in schema dev grant all privileges to analyst;



--raw data tables (select only)
grant usage on schema truven, medicare, optum_dod, optum_zip to group analyst;
grant select on all tables in schema truven, medicare, optum_dod, optum_zip to group analyst;
ALTER DEFAULT PRIVILEGES IN SCHEMA truven, medicare, optum_dod, optum_zip grant select on tables to group analyst; 


-------------------------------------------------------------------------------------------

/*
 * Create User
 */
--Create User
drop role uthtest;


CREATE ROLE uthtest NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT LOGIN PASSWORD 'd3f@ult$';

grant uthealthadmin to dwtest;
grant uthealthdev to cms2;
grant analyst to uthtest;

grant connect on database uthealth to dwtest;

/*
 * Public Schema settings
 */
REVOKE ALL ON SCHEMA pg_catalog FROM public;
grant USAGE ON SCHEMA pg_catalog TO public;
grant select on tables in SCHEMA pg_catalog TO public;

/*
 * Password change
 */
alter user uthtest with password 'uthtest';

/*
 * Superuser
 */
-- Grant superuser
ALTER USER turban SUPERUSER; 

/*
 * Audit Permissions
 */
select *
FROM information_schema.role_table_grants
where grantee='uthealthdev';

select *
FROM   information_schema.table_privileges 
WHERE  grantee = 'uthealthdev';

select *
FROM information_schema.usage_privileges
where grantee='uthealthdev';

SELECT 
      r.rolname, 
      r.rolsuper, 
      r.rolinherit,
      r.rolcreaterole,
      r.rolcreatedb,
      r.rolcanlogin,
      r.rolconnlimit, r.rolvaliduntil,
  ARRAY(SELECT b.rolname
        FROM pg_catalog.pg_auth_members m
        JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)
        WHERE m.member = r.oid) as memberof
FROM pg_catalog.pg_roles r
ORDER BY 1;



select * from pg_catalog.pg_tables where tableowner = 'yliu26'
