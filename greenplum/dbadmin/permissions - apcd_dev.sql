
/* ******************************************************************************************************
 * This script contains several code blocks useful for controlling user access on the Greenplum Server
 * and contains definitions for user roles
 * 
 * This script focuses on apcd_dev
 * ******************************************************************************************************
 *  Author 			|| Date      	|| Notes
 * ******************************************************************************************************
 * Xiaorui Zhang	|| 03/31/2023	|| Created
 * 
 * ****************************************************************************************************** */

/*******************************************************************
 * Role and user management code
 *******************************************************************/

--Granting user role to a user
--syntax: grant role_name to user_name
grant apcd_uthealth_dev to jharri66;
grant apcd_uthealth_dev to aryan;
grant apcd_uthealth_dev to nguyken;
grant apcd_uthealth_dev to xrzhang;

--Revokes roles from a user
revoke apcd_uthealth_analyst from cms2;

--Revoke access to specific schema from user
revoke all on all tables in schema medicaid from lghosh1;

--Revokes other permissions, such as permission to connect
--Does not remove user roles
revoke all on database uthealth FROM lghosh1;

--Revoke permissions on a schema from a user/usergroup
revoke all on schema cdl_raw from group apcd_uthealth_analyst; 
revoke all on all tables in schema cdl_raw from group apcd_uthealth_analyst; 
revoke all on all sequences in schema cdl_raw from group apcd_uthealth_analyst;
revoke usage on schema cdl_raw from group apcd_uthealth_analyst;

--This code set in theory lets other people access the tables that this person makes in dev
set role oaborisa;
alter default privileges for user oaborisa in schema dev grant all on tables to uthealth_analyst;

/*******************************************************************
 * Code to see current status
 *******************************************************************/

--Lists usernames/rolenames, assigned roles, and superuser status
SELECT r.rolname, 
  ARRAY(SELECT b.rolname
        FROM pg_catalog.pg_auth_members m
        JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)
        WHERE m.member = r.oid) as memberof
, r.rolsuper
FROM pg_catalog.pg_roles r
WHERE r.rolname !~ '^pg_'
ORDER BY 1;

--sees everyone with a particular role
select r.rolname as username, b.rolname as role
from pg_catalog.pg_roles r join pg_catalog.pg_auth_members m on r.oid = m.member
join pg_catalog.pg_roles b on m.roleid = b.oid
where b.rolname = 'apcd_uthealth_dev'
order by r.rolname;

--who's got access to a schema?
select grantee, array_agg(privilege_type::text) as privileges
from information_schema.role_table_grants
where table_schema = 'cdl_raw'
group by grantee;

/*******************************************************************
 * Create schemas
 *******************************************************************/
--this is the read-only schema where test data goes
create schema cdl_raw;

--this is the read/write schema where 
create schema dev;

/*******************************************************************
 * Role definitions: apcd_uthealth_analyst
 * 
 * Purpose: Gives read/write access to mpr, dev
 * 			Read-only access to cdl_raw
 *******************************************************************/
--run this code if you need to drop role
--just make sure it doesn't take any tables with it
drop owned by apcd_uthealth_analyst cascade;
drop role apcd_uthealth_analyst;

create role apcd_uthealth_analyst;

grant connect on database apcd_dev to apcd_uthealth_analyst;  --allows connection to apcd_dev database
grant temporary on database apcd_dev to apcd_uthealth_analyst; --allows creation of temp tables

--Grant read/write on mpr
grant all on schema mpr to apcd_uthealth_analyst; 
grant all on all tables in schema mpr to apcd_uthealth_analyst; 
grant all privileges on all sequences in schema mpr to apcd_uthealth_analyst; 
alter default privileges in schema mpr grant all on tables to apcd_uthealth_analyst; 

--Grant read/write on dev
grant all on schema dev to apcd_uthealth_analyst; 
grant all on all tables in schema dev to apcd_uthealth_analyst; 
grant all privileges on all sequences in schema dev to apcd_uthealth_analyst; 
alter default privileges in schema dev grant all on tables to apcd_uthealth_analyst;

/* --note, this access was removed 4/27/23 at Joe H's request
 * cdl_raw is where the test data for APCD is going and currently only Joe H, Xiaorui Z, and Aryan H should have access
--Grant read only on cdl_raw
grant usage on schema cdl_raw to group apcd_uthealth_analyst; 
grant select on all tables in schema cdl_raw to group apcd_uthealth_analyst; 
grant select on all sequences in schema cdl_raw to apcd_uthealth_analyst;
alter default privileges in schema cdl_raw grant select on tables to group apcd_uthealth_analyst;
*/

/*******************************************************************
 * Role definitions: apcd_uthealth_dev
 * 
 * Purpose: Inherits all permissions of apcd_uthealth_analyst
 * 			Grants read/write access to cdl_raw
 *******************************************************************/
--run this code if you need to drop role
--just make sure it doesn't take any tables with it
drop owned by apcd_uthealth_dev cascade;
drop role apcd_uthealth_dev;

create role apcd_uthealth_dev;

grant connect on database apcd_dev to apcd_uthealth_dev;  --allows connection to apcd_dev database
grant temporary on database apcd_dev to apcd_uthealth_dev; --allows creation of temp tables

--inherit permissions from apcd_uthealth_analyst
grant apcd_uthealth_analyst to apcd_uthealth_dev;

--Grant read/write on cdl_raw
grant all on schema cdl_raw to apcd_uthealth_dev; 
grant all on all tables in schema cdl_raw to apcd_uthealth_dev; 
grant all privileges on all sequences in schema cdl_raw to apcd_uthealth_dev; 
alter default privileges in schema cdl_raw grant all on tables to apcd_uthealth_dev;


/*******************************************************************
 * Role definitions: apcd_uthealth_analyst
 * 
 * Purpose: Gives read access to ONLY optum_zip and data_warehouse
 * 		Gives read/write access to dev
 *******************************************************************/
--run this code if you need to drop role
--just make sure it doesn't take any tables with it
drop owned by apcd_uthealth_analyst cascade;
drop role apcd_uthealth_analyst;

create role apcd_uthealth_analyst;

grant connect on database uthealth to apcd_uthealth_analyst;  --allows connection to uthealth database
grant temporary on database uthealth to apcd_uthealth_analyst; --allows creation of temp tables

grant usage on schema optum_zip to apcd_uthealth_analyst; 
grant select on all tables in schema optum_zip to apcd_uthealth_analyst; 
grant select on all sequences in schema optum_zip to apcd_uthealth_analyst;
alter default privileges in schema optum_zip grant select on tables to apcd_uthealth_analyst;

grant usage on schema data_warehouse to apcd_uthealth_analyst; 
grant select on all tables in schema data_warehouse to apcd_uthealth_analyst; 
grant select on all sequences in schema data_warehouse to apcd_uthealth_analyst;
alter default privileges in schema data_warehouse grant select on tables to apcd_uthealth_analyst;

grant all on schema apcd_test to apcd_uthealth_analyst; 
grant all on all tables in schema apcd_test to apcd_uthealth_analyst; 
grant all privileges on all sequences in schema apcd_test to apcd_uthealth_analyst;
alter default privileges in schema apcd_test grant all on tables to apcd_uthealth_analyst;

grant all on schema dev to apcd_uthealth_analyst; 
grant all on all tables in schema dev to apcd_uthealth_analyst; 
grant all privileges on all sequences in schema dev to apcd_uthealth_analyst; 
alter default privileges in schema dev grant all on tables to apcd_uthealth_analyst; 

