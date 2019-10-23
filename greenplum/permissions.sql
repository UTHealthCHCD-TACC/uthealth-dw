create role uthealthadmin;

grant connect on database uthealth to group uthealthdev;

--revoke all on schema truven from group uthealthdev;
grant usage on schema truven to group uthealthdev;
grant usage on schema optum_dod to group uthealthdev;
grant usage on schema reference_tables to group uthealthdev;
grant usage on schema data_warehouse to group uthealthdev;

--uthealthadmin
grant all on database uthealth to uthealthadmin;

--Schemas
grant all on schema truven to group uthealthadmin;
grant all on schema optum_dod to group uthealthadmin;
grant all on schema optum_zip to group uthealthadmin;
grant all on schema reference_tables to group uthealthadmin;
grant all on schema data_warehouse to group uthealthadmin;
grant all on schema medicare to group uthealthadmin;
grant all on schema dev to group uthealthadmin;
grant all on schema dev2016 to group uthealthadmin;

--grant select on all TABLES in schema truven to uthealthdev; # Not supported in Postgres < 9.0
select 'grant all on '||schemaname||'.'||tablename||' to uthealthadmin;'
from pg_tables where schemaname in ('data_warehouse', 'dev', 'dev2016', 'optum_dod', 'optum_zip', 'tableau', 'truven', 'reference_tables', 'medicare')
order by schemaname, tablename;

--Create User
drop role dwtest;
CREATE ROLE dwtest NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT LOGIN PASSWORD '<password>';

grant uthealthadmin to dwtest;

grant connect on database uthealth to dwtest;
grant usage on schema tableau to dwtest;

REVOKE ALL ON SCHEMA pg_catalog FROM public;
grant USAGE ON SCHEMA pg_catalog TO public;
grant select on tables in SCHEMA pg_catalog TO public;

-- Change Password
alter user cc_user with password '<password>';

-- Grant superuser
ALTER USER jharri66 SUPERUSER; 

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

