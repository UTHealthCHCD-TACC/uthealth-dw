revoke all on schema public from public;

/*
 * SSL Status
 */
SELECT datname,usename, ssl, client_addr 
  FROM pg_catalog.pg_stat_ssl
  JOIN pg_catalog.pg_stat_activity
    ON pg_stat_ssl.pid = pg_stat_activity.pid;

   select * from pg_catalog.pg_stat_ssl
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
GRANT all ON ALL TABLES IN SCHEMA truven TO uthealthadmin;

grant all on schema optum_dod to group uthealthadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA optum_dod grant all on tables to group uthealthadmin; 
GRANT all ON ALL TABLES IN SCHEMA optum_dod TO uthealthadmin;

grant all on schema optum_zip to group uthealthadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA optum_zip grant all on tables to group uthealthadmin; 
GRANT all ON ALL TABLES IN SCHEMA optum_zip TO uthealthadmin;

grant all on schema reference_tables to group uthealthadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA reference_tables grant all on tables to group uthealthadmin; 
GRANT all ON ALL TABLES IN SCHEMA reference_tables TO uthealthadmin;

grant all on schema conditions to group uthealthadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA conditions grant all on tables to group uthealthadmin; 
GRANT all ON ALL TABLES IN SCHEMA conditions TO uthealthadmin;

grant all on schema data_warehouse to group uthealthadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_warehouse grant all on tables to group uthealthadmin; 
GRANT all ON ALL TABLES IN SCHEMA data_warehouse TO uthealthadmin;

grant all on schema medicare_national to group uthealthadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA medicare_national grant all on tables to group uthealthadmin; 
GRANT all ON ALL TABLES IN SCHEMA medicare_national TO uthealthadmin;

grant all on schema medicare_texas to group uthealthadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA medicare_texas grant all on tables to group uthealthadmin; 
GRANT all ON ALL TABLES IN SCHEMA medicare_texas TO uthealthadmin;

grant all on schema medicaid to group uthealthadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA medicaid grant all on tables to group uthealthadmin; 
GRANT all ON ALL TABLES IN SCHEMA medicaid TO uthealthadmin;

grant all on schema dev to group uthealthadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA dev grant all on tables to group uthealthadmin; 
GRANT all ON ALL TABLES IN SCHEMA dev TO uthealthadmin;

grant all on schema dw_staging to group uthealthadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA dw_staging grant all on tables to group uthealthadmin; 
GRANT all ON ALL TABLES IN SCHEMA dw_staging TO uthealthadmin;



grant all on schema qa_reporting to group uthealthadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA qa_reporting grant all on tables to group uthealthadmin; 
GRANT all ON ALL TABLES IN SCHEMA qa_reporting TO uthealthadmin;


grant usage on schema pg_aoseg to group uthealthadmin;
GRANT select ON ALL TABLES IN SCHEMA pg_aoseg TO uthealthadmin;

grant usage on schema pg_catalog to group uthealthadmin;
GRANT select ON ALL TABLES IN SCHEMA pg_catalog TO uthealthadmin;

grant usage on schema pg_bitmapindex to group uthealthadmin;
GRANT select ON ALL TABLES IN SCHEMA pg_bitmapindex TO uthealthadmin;

grant usage on schema gp_toolkit to group uthealthadmin;
GRANT select ON ALL TABLES IN SCHEMA gp_toolkit TO uthealthadmin;

/*
 * UTHealth uthealth_analyst Role
 */

--CREATE ROLE smadhuri NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT LOGIN PASSWORD 'password';


CREATE ROLE ctruong NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT LOGIN;

grant uthealthadmin to turban;

grant uthealth_analyst to smadhuri;

grant uthealth_analyst to ctruong;

grant uthealth_analyst to yliu26;

grant uthealth_analyst to lghosh1;

grant uthealth_analyst to jfu2;


-- uthealth_analyst role definition
drop owned by uthealth_analyst cascade;

drop role uthealth_analyst;

create role uthealth_analyst;

grant connect on database uthealth to uthealth_analyst;

--Schemas

---conditions
grant usage on schema conditions to group uthealth_analyst; 
grant select on all tables in schema conditions to group uthealth_analyst; 

--reference_tables (select only)
grant usage on schema reference_tables to group uthealth_analyst;
grant select on all tables in schema reference_tables to group uthealth_analyst;
ALTER DEFAULT PRIVILEGES IN SCHEMA reference_tables grant select on tables to group uthealth_analyst; 

--data_warehouse (select only) 
grant usage on schema data_warehouse to group uthealth_analyst;
grant select on all tables in schema data_warehouse to group uthealth_analyst;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_warehouse grant select on tables to group uthealth_analyst; 


--data_warehouse (select only) 
grant usage on schema medicare to group uthealth_analyst;
grant select on all tables in schema medicare to group uthealth_analyst;
ALTER DEFAULT PRIVILEGES IN SCHEMA medicare grant select on tables to group uthealth_analyst; 

--dw_qa (select only) 
grant usage on schema dw_qa to group uthealth_analyst;
grant select on all tables in schema dw_qa to group uthealth_analyst;
ALTER DEFAULT PRIVILEGES IN SCHEMA dw_qa grant select on tables to group uthealth_analyst; 

--tableau (select only)
grant all on schema tableau to group uthealth_analyst;
grant select on all tables in schema tableau to group uthealth_analyst;
ALTER DEFAULT PRIVILEGES IN SCHEMA tableau grant select on tables to group uthealth_analyst; 

--dev (all access)

grant all on schema dev to analyst;
grant all on all tables in schema dev to analyst;
grant all privileges on all sequences in schema dev to analyst;
alter default privileges in schema dev grant all privileges to group analyst;

--dw_qa (all access)
grant all on schema dw_qa to analyst;
grant all on all tables in schema dw_qa to analyst;
grant all privileges on all sequences in schema dw_qa to analyst;
alter default privileges in schema dw_qa grant all privileges to  analyst;


--conditions (all access)
grant all on schema conditions to analyst;
grant all on all tables in schema conditions to analyst;
grant all privileges on all sequences in schema conditions to analyst;
alter default privileges in schema conditions grant all on tables to analyst;

--raw data tables (select only)
grant usage on schema truven, medicare_national, medicare_texas, medicaid, optum_dod, optum_zip to group uthealth_analyst;
grant select on all tables in schema truven, medicare_national, medicare_texas, medicaid, optum_dod, optum_zip to group uthealth_analyst;
ALTER DEFAULT PRIVILEGES IN SCHEMA truven grant select on tables to group uthealth_analyst; 
ALTER DEFAULT PRIVILEGES IN SCHEMA medicare_national grant select on tables to group uthealth_analyst;
ALTER DEFAULT PRIVILEGES IN SCHEMA medicare_texas grant select on tables to group uthealth_analyst;
ALTER DEFAULT PRIVILEGES IN SCHEMA medicaid grant select on tables to group uthealth_analyst;
ALTER DEFAULT PRIVILEGES IN SCHEMA optum_zip grant select on tables to group uthealth_analyst;
ALTER DEFAULT PRIVILEGES IN SCHEMA optum_dod grant select on tables to group uthealth_analyst;

--conditions (select only)
grant all on schema conditions to group uthealth_analyst;
grant select on all tables in schema conditions to group uthealth_analyst;
ALTER DEFAULT PRIVILEGES IN SCHEMA conditions grant select on tables to group uthealth_analyst; 


/*
 * Create User
 */
--Create User
drop role uthtest;


CREATE ROLE uthtest NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT LOGIN PASSWORD 'd3f@ult$';

grant uthealthadmin to jharri66;
grant uthealthdev to jharri66;
grant uthealth_analyst to jharri66;

grant connect on database uthealth to dwtest;

alter role uthtest nosuperuser;

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
where grantee='uthealth_analyst';
and table_schema='dw_qa';

select *
FROM information_schema.role_table_grants
where table_schema='medicaid';

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


--CONNECT PERMISSIONS
REVOKE CONNECT ON DATABASE coviddb FROM PUBLIC; 
REVOKE ALL PRIVILEGES ON DATABASE uthealth FROM public;

grant uthealth_analyst to cms2;

revoke connect on database uthealth from amoosa1;

CREATE ROLE test_user2 NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT LOGIN PASSWORD 'd3f@ult$';


select * from pg_user where usename='wcough';
select * from pg_roles where rolname='wcough';

REASSIGN OWNED BY yliu26 to uthealthadmin;
