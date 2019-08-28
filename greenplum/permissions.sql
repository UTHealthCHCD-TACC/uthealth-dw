create role uthealthdev;

grant connect on database uthealth to group uthealthdev;

--revoke all on schema truven from group uthealthdev;
grant usage on schema truven to group uthealthdev;
grant usage on schema optum_dod to group uthealthdev;
grant usage on schema data_warehouse to group uthealthdev;

--grant select on all TABLES in schema truven to uthealthdev; # Not supported in Postgres < 9.0
select 'grant select on '||schemaname||'.'||tablename||' to uthealthdev;'
from pg_tables where schemaname in ('data_warehouse')
order by schemaname, tablename;

--Create User
drop role tester;
CREATE ROLE jharri66 NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT LOGIN PASSWORD '<password>';
grant uthealthdev to jharri66;

--Change Password
alter user lghosh1 with password '<enter password>';


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

