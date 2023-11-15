
/* ******************************************************************************************************
 * This script contains several code blocks useful for controlling user access on the Greenplum Server
 * and contains definitions for user roles
 * ******************************************************************************************************
 *  Author 			|| Date      	|| Notes
 * ******************************************************************************************************
 *  wallingTACC  	|| 1/1/2019 	|| script created
 * ******************************************************************************************************
 *  wcc001  		|| 9/09/2021 	|| organized uthealth_analyst and uthealthdev roles. cleaned up unused code. added comment block
 * -------------------------   script is divided into: Role Defintions, User Creation, & Audits/Misc
 * ******************************************************************************************************
 * Xiaorui Zhang	|| 03/08/2023	|| Updated, reformatted/reorganized, added user_role APCD
 * ******************************************************************************************************
 * Xiaorui Zhang	|| 06/23/2023	|| Granted read/write to staging_clean to uthealth_dev
 * 
 * ****************************************************************************************************** */

/*******************************************************************
 * Role and user management code
 *******************************************************************/

--Granting user role to a user
--syntax: grant role_name to user_name
grant apcd_uthealth_analyst to lghosh1;
grant apcd_uthealth_analyst to cms2;
grant uthealth_analyst to cms2;
grant uthealth_analyst to lghosh1;

--Grant specific user access to a specific schema
--note that grant select alone is insufficient
grant usage on schema optum_zip to lghosh1; 
grant select on all tables in schema optum_zip to lghosh1; 
grant select on all sequences in schema optum_zip to lghosh1;
alter default privileges in schema optum_zip grant select on tables to lghosh1;

--Grant superuser status
--Note that superusers do NOT have permission to grant superuser status
--regardless of what the error message says
ALTER USER lghosh1 SUPERUSER;

--Revokes roles from a user
revoke uthealth_analyst from lghosh1;
revoke uthealth_analyst from cms2;
revoke uthealth_dev from lghosh1;
revoke apcd_uthealth_analyst from cms2;

--Revoke access to specific schema from user
revoke all on all tables in schema medicaid from lghosh1;

--Revokes other permissions, such as permission to connect
--Does not remove user roles
revoke all on database uthealth FROM lghosh1;

--This code set in theory lets other people access the tables that this person makes in dev
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

--Lists the role name and privilege type for a specific table/schema
select grantee, privilege_type, table_name , table_schema 
from information_schema.role_table_grants 
where table_schema = 'medicaid' --and table_name = 'claim_detail';

--Lists privileges for a specific grantee 
select distinct grantee, privilege_type, table_schema, table_name
from information_schema.role_table_grants 
where grantee = 'lghosh1' and privilege_type = 'SELECT'
order by table_schema;

--check who has access to a specific schema
SELECT *
FROM pg_default_acl
WHERE defaclobjtype = 'r' AND defaclnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'dev');

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

/*******************************************************************
 * Role definitions: uthealth_analyst
 * 
 * Purpose: Gives read access to all schemas
 * 		Write access to dev schema
 * 		Write access to tableau schema
 *******************************************************************/

-- uthealth_analyst role definition
drop role uthealth_analyst;
create role uthealth_analyst;

grant connect on database uthealth to uthealth_analyst; --allows connection to uthealth database
grant temporary on database uthealth to uthealth_analyst; --allows creation of temp tables

grant all on all tables in schema dev to uthealth_analyst; --grants read/write privileges to schema dev
grant all privileges on all sequences in schema dev to uthealth_analyst; 
alter default privileges in schema dev grant all on tables to uthealth_analyst; 
--This code will delete all tables owned by anyone in the uthealth_analyst group along with dependent tables
drop owned by uthealth_analyst cascade;
/*from Posgresql documentation:
DROP OWNED drops all the objects within the current database that are owned by one of the specified roles.
Any privileges granted to the given roles on objects in the current database or on shared objects
(databases, tablespaces, configuration parameters) will also be revoked.
CASCADE
Automatically drop objects that depend on the affected objects, and in turn all objects that depend on those objects.*/

--***********************************************************************************************************
--Schemas permissions for uthealth_analyst 

---conditions (select only)
grant usage on schema conditions to group uthealth_analyst; 
grant select on all tables in schema conditions to group uthealth_analyst; 
grant select on all sequences in schema conditions to uthealth_analyst;
alter default privileges in schema conditions grant select on tables to group uthealth_analyst;

--data_warehouse (select only) 
grant usage on schema data_warehouse to group uthealth_analyst; 
grant select on all tables in schema data_warehouse to group uthealth_analyst; 
grant select on all sequences in schema data_warehouse to uthealth_analyst;
alter default privileges in schema data_warehouse grant select on tables to group uthealth_analyst;

--dev (all access)
grant all on schema dev to uthealth_analyst; 
grant all on all tables in schema dev to uthealth_analyst; 
grant all privileges on all sequences in schema dev to uthealth_analyst; 
alter default privileges in schema dev grant all on tables to uthealth_analyst; 

--tableau (all access)
grant all on schema tableau to uthealth_analyst; 
grant all on all tables in schema tableau to uthealth_analyst; 
grant all privileges on all sequences in schema tableau to uthealth_analyst; 
alter default privileges in schema tableau grant all on tables to uthealth_analyst; 

--dw_staging (select only )
grant usage on schema dw_staging to group uthealth_analyst; 
grant select on all tables in schema dw_staging to group uthealth_analyst; 
grant select on all sequences in schema dw_staging to uthealth_analyst;
alter default privileges in schema dw_staging grant select on tables to group uthealth_analyst;

--***raw tables (select only)
	---medicaid
	grant usage on schema medicaid to group uthealth_analyst; 
	grant select on all tables in schema medicaid to group uthealth_analyst; 
	alter default privileges in schema medicaid grant select on tables to group uthealth_analyst;
	---medicare_national
	grant usage on schema medicare_national to group uthealth_analyst; 
	grant select on all tables in schema medicare_national to group uthealth_analyst; 
	alter default privileges in schema medicare_national grant select on tables to group uthealth_analyst;
	---medicare_texas
	grant usage on schema medicare_texas to group uthealth_analyst; 
	grant select on all tables in schema medicare_texas to group uthealth_analyst; 
	alter default privileges in schema medicare_texas grant select on tables to group uthealth_analyst;
	---optum_dod
	grant usage on schema optum_dod to group uthealth_analyst; 
	grant select on all tables in schema optum_dod to group uthealth_analyst; 
	alter default privileges in schema optum_dod grant select on tables to group uthealth_analyst;
	---optum_zip
	grant usage on schema optum_zip to group uthealth_analyst; 
	grant select on all tables in schema optum_zip to group uthealth_analyst; 
	alter default privileges in schema optum_zip grant select on tables to group uthealth_analyst;
	---truven
	grant usage on schema truven to group uthealth_analyst; 
	grant select on all tables in schema truven to group uthealth_analyst; 
	alter default privileges in schema truven grant select on tables to group uthealth_analyst;
	---tableau
	grant all on schema tableau to group uthealth_analyst; 
	grant all on all tables in schema tableau to group uthealth_analyst; 
	alter default privileges in schema tableau grant all on tables to group uthealth_analyst;
	---IQVIA
	grant all on schema iqvia to group uthealth_analyst; 
	grant all on all tables in schema iqvia to group uthealth_analyst; 
	alter default privileges in schema iqvia grant all on tables to group uthealth_analyst;

--qa_reporting (select only )
grant usage on schema qa_reporting to group uthealth_analyst; 
grant select on all tables in schema qa_reporting to group uthealth_analyst; 
grant select on all sequences in schema qa_reporting to uthealth_analyst;
alter default privileges in schema qa_reporting grant select on tables to group uthealth_analyst;

--reference_tables (select only )
grant usage on schema reference_tables to group uthealth_analyst; 
grant select on all tables in schema reference_tables to group uthealth_analyst; 
grant select on all sequences in schema reference_tables to uthealth_analyst;
alter default privileges in schema reference_tables grant select on tables to group uthealth_analyst;

--truven_pay (all access)
grant all on schema truven_pay to uthealth_analyst; 
grant all on all tables in schema truven_pay to uthealth_analyst; 
grant all privileges on all sequences in schema truven_pay to uthealth_analyst; 
alter default privileges in schema truven_pay grant all on tables to uthealth_analyst; 

--crosswalk (select only)
grant usage on schema crosswalk to group uthealth_analyst; 
grant select on all tables in schema crosswalk to group uthealth_analyst; 
grant select on all sequences in schema crosswalk to uthealth_analyst;
alter default privileges in schema crosswalk grant select on tables to group uthealth_analyst;

/*******************************************************************
 * Role definition: uthealth_dev
 * 
 * Purpose: Grants same permissions as uthealth_analyst AND
 * 		Write access to:
 * 				data_warehouse
 *  			dw_staging
 * 				staging_clean
 *  			qa_reporting
 *   			reference_tables
 *   			conditions
 *   			public
 *******************************************************************/

--Create role
--drop owned by uthealth_dev cascade;

drop role uthealth_dev;
create role uthealth_dev;
grant connect on database uthealth to uthealth_dev;

--grant same permissions as uthealth-analyst
grant uthealth_analyst to uthealth_dev;

--Grant access to schemas other than raw data

--data_warehouse (all access)
grant all on schema data_warehouse to uthealth_dev; 
grant all on all tables in schema data_warehouse to uthealth_dev; 
grant all privileges on all sequences in schema data_warehouse to uthealth_dev; 
alter default privileges in schema data_warehouse grant all on tables to uthealth_dev; 
 
--dw_staging (all access)
grant all on schema dw_staging to uthealth_dev; 
grant all on all tables in schema dw_staging to uthealth_dev; 
grant all privileges on all sequences in schema dw_staging to uthealth_dev; 
alter default privileges in schema dw_staging grant all on tables to uthealth_dev; 

--staging_clean (all access)
grant all on schema staging_clean to uthealth_dev; 
grant all on all tables in schema staging_clean to uthealth_dev; 
grant all privileges on all sequences in schema staging_clean to uthealth_dev; 
alter default privileges in schema staging_clean grant all on tables to uthealth_dev; 

--qa_reporting (all access)
grant all on schema qa_reporting to uthealth_dev; 
grant all on all tables in schema qa_reporting to uthealth_dev; 
grant all privileges on all sequences in schema qa_reporting to uthealth_dev; 
alter default privileges in schema qa_reporting grant all on tables to uthealth_dev; 
grant execute on all functions in schema qa_reporting to uthealth_dev;
 
--reference_tables (all access)
grant all on schema reference_tables to uthealth_dev; 
grant all on all tables in schema reference_tables to uthealth_dev; 
grant all privileges on all sequences in schema reference_tables to uthealth_dev; 
alter default privileges in schema reference_tables grant all on tables to uthealth_dev; 

--conditions (all access)
grant all on schema conditions to uthealth_dev; 
grant all on all tables in schema conditions to uthealth_dev; 
grant all privileges on all sequences in schema conditions to uthealth_dev; 
alter default privileges in schema conditions grant all on tables to uthealth_dev; 

--public (all access)
grant all on schema public to uthealth_dev; 
grant all on all tables in schema public to uthealth_dev; 
grant all privileges on all sequences in schema public to uthealth_dev; 
alter default privileges in schema public grant all on tables to uthealth_dev; 

--uthealth_dev gets full access to crosswalk
grant all on schema crosswalk to uthealth_dev; 
grant all on all tables in schema crosswalk to uthealth_dev; 
grant all privileges on all sequences in schema crosswalk to uthealth_dev; 
alter default privileges in schema crosswalk grant all on tables to uthealth_dev; 

/*******************************************************************
 * Role definition: uthealth_admin
 * 
 * Purpose: Grants same permissions as uthealth_analyst AND uthealth_dev AND
 * 		Write access to raw data tables
 * 		Write access to tableau schema
 *******************************************************************/

--Create role
--drop owned by uthealth_admin cascade;
drop role uthealth_admin;
create role uthealth_admin;
grant connect on database uthealth to group uthealth_admin;

--grant uthealth_analyst + uthealth_dev permissions
grant uthealth_analyst to uthealth_admin;
grant uthealth_dev to uthealth_admin;

--Schema permissions for uthealthadmin

--***raw tables (all access)
	---medicaid
	grant all on schema medicaid to group uthealth_admin; 
	grant all on all tables in schema medicaid to group uthealth_admin; 
	alter default privileges in schema medicaid grant all on tables to uthealth_admin;
	---medicare_national
	grant all on schema medicare_national to group uthealth_admin; 
	grant all on all tables in schema medicare_national to group uthealth_admin; 
	alter default privileges in schema medicare_national grant all on tables to group uthealth_admin;
	---medicare_texas
	grant all on schema medicare_texas to group uthealth_admin; 
	grant all on all tables in schema medicare_texas to group uthealth_admin; 
	alter default privileges in schema medicare_texas grant all on tables to group uthealth_admin;
	---optum_dod
	grant all on schema optum_dod to group uthealth_admin; 
	grant all on all tables in schema optum_dod to group uthealth_admin; 
	alter default privileges in schema optum_dod grant all on tables to group uthealth_admin;
	---optum_zip
	grant all on schema optum_zip to group uthealth_admin; 
	grant all on all tables in schema optum_zip to group uthealth_admin; 
	alter default privileges in schema optum_zip grant all on tables to group uthealth_admin;
	---truven
	grant all on schema truven to group uthealth_admin; 
	grant all on all tables in schema truven to group uthealth_admin; 
	alter default privileges in schema truven grant all on tables to group uthealth_admin;
	---tableau
	grant all on schema tableau to group uthealth_admin; 
	grant all on all tables in schema tableau to group uthealth_admin; 
	alter default privileges in schema tableau grant all on tables to group uthealth_admin;
	---IQVIA
	grant all on schema iqvia to group uthealth_admin; 
	grant all on all tables in schema iqvia to group uthealth_admin; 
	alter default privileges in schema iqvia grant all on tables to group uthealth_admin;





















/*********************************************************************************
 * Code that Xiaorui doesn't want to delete but also does not seem useful
 * ******************************************************************************/

---(----------  ******************** ----------------------------------------------------------------------------------------------------)
---(----------  Audits/Misc          ----------------------------------------------------------------------------------------------------)

/*
 * Audit Permissions
 */

/*
 * Public Schema settings
 */
REVOKE ALL ON SCHEMA pg_catalog FROM public;
grant USAGE ON SCHEMA pg_catalog TO public;
grant select on tables in SCHEMA pg_catalog TO public;


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

revoke connect on database uthealth from amoosa1;

select * from pg_user where usename='wcough';
select * from pg_roles where rolname='wcough';

REASSIGN OWNED BY yliu26 to uthealthadmin;

alter table qa_reporting.claim_diag_column_checks owner to jwozny;


----! run this query for each new uthealth_analyst so that everyone can access their dev tables freely
set role judyk277;
alter default privileges for user judyk277 in schema dev grant all on tables to uthealth_analyst; 
----!

set role wcough;  select session_user;  select current_user;

set role nguyenk

---(----------  ********************   ----------------------------------------------------------------------------------------------------)
---(----------  User Creation          ----------------------------------------------------------------------------------------------------)
/*
 * Create User
 */
--Create User
drop role uthtest;


CREATE ROLE rhansen1 NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT LOGIN; --PASSWORD 'd3f@ult$';

grant uthealthadmin to jharri66;
grant uthealthdev to jharri66;
grant uthealth_analyst to rhansen1;

grant connect on database uthealth to dwtest;

alter role uthtest nosuperuser;


/*
 * Password change
 */
alter user uthtest with password 'uthtest';

/*
 * Superuser
 */
-- Grant superuser
ALTER USER turban SUPERUSER; 


---(----------  End User Creation          ----------------------------------------------------------------------------------------------------)
---(----------  ********************   ----------------------------------------------------------------------------------------------------)

