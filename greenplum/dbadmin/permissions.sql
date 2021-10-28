
/* ******************************************************************************************************
 *  This table is used to generate a de-identified claim id that will be used to populate claim_detail and claim_header tables
 *	The uth_claim_id column will be a sequence that is initially set to a 100,000,000
 *  This code can be re-run as new data comes in, logic is in place to prevent duplicate entries into table
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wallingTACC  || 1/1/2019 || script created
 * ******************************************************************************************************
 *  wcc001  || 9/09/2021 || organized uthealth_analyst and uthealthdev roles. cleaned up unused code. added comment block
 * -------------------------   script is divided into: Role Defintions, User Creation, & Audits/Misc
 * ******************************************************************************************************
 * 
 * ****************************************************************************************************** */


---(----------  *****************  ----------------------------------------------------------------------------------------------------)  
---(---------- Role Definitions    ----------------------------------------------------------------------------------------------------)

/*
 * UTHealth uthealth_analyst Role
 */

-- uthealth_analyst role definition
drop owned by uthealth_analyst cascade;

drop role uthealth_analyst;

create role uthealth_analyst;

grant connect on database uthealth to uthealth_analyst;

---****uthealth_analyst assigned users *****************************************************
grant uthealth_analyst to turban;

grant uthealth_analyst to lghosh1;

grant uthealth_analyst to ctruong;

grant uthealth_analyst to jharri66;

grant uthealth_analyst to jfu2;

grant uthealth_analyst to smadhuri;

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
	---uthealth/medicare_national
	grant usage on schema uthealth/medicare_national to group uthealth_analyst; 
	grant select on all tables in schema uthealth/medicare_national to group uthealth_analyst; 
	alter default privileges in schema uthealth/medicare_national grant select on tables to group uthealth_analyst;
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
	---tables
	grant all on schema tableau to group uthealth_analyst; 
	grant all on all tables in schema tableau to group uthealth_analyst; 
	alter default privileges in schema tableau grant all on tables to group uthealth_analyst;

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



/*
 * uthealth_dev role
 */

-- uthealth_dev role definition
drop owned by uthealth_dev cascade;

drop role uthealth_dev;

create role uthealth_dev;

grant connect on database uthealth to uthealth_dev;

---****uthealth_analyst assigned users *****************************************************
grant uthealth_analyst to uthealth_dev;

grant uthealth_dev to jwozny;


--***********************************************************************************************************

--***Schemas permissions for uthealth_dev***
 
---dw_staging (all access)
grant all on schema dw_staging to uthealth_dev; 
grant all on all tables in schema dw_staging to uthealth_dev; 
grant all privileges on all sequences in schema dw_staging to uthealth_dev; 
alter default privileges in schema dw_staging grant all on tables to uthealth_dev; 

---qa_reporting (all access)
grant all on schema qa_reporting to uthealth_dev; 
grant all on all tables in schema qa_reporting to uthealth_dev; 
grant all privileges on all sequences in schema qa_reporting to uthealth_dev; 
alter default privileges in schema qa_reporting grant all on tables to uthealth_dev; 

---reference_tables (all access)
grant all on schema reference_tables to uthealth_dev; 
grant all on all tables in schema reference_tables to uthealth_dev; 
grant all privileges on all sequences in schema reference_tables to uthealth_dev; 
alter default privileges in schema reference_tables grant all on tables to uthealth_dev; 



/*
 * UTHealthAdmin Role
 */


-- uthealthadmin role definition
drop owned by uthealth_admin cascade;

drop role uthealth_admin;

create role uthealth_admin;

grant connect on database uthealth to group uthealth_admin;

---****uthealth_analyst assigned users *****************************************************
grant uthealth_analyst to uthealth_admin;

grant uthealth_dev to uthealth_admin;

grant uthealth_admin to wcough; 

--******************************************************************************************


--Schema permissions for uthealthadmin

---data_warehouse (all access)
grant all on schema data_warehouse to uthealth_admin; 
grant all on all tables in schema data_warehouse to uthealth_admin; 
grant all privileges on all sequences in schema data_warehouse to uthealth_admin; 
alter default privileges in schema data_warehouse grant all on tables to uthealth_admin; 

--***raw tables (all access)
	---medicaid
	grant all on schema medicaid to group uthealth_admin; 
	grant all on all tables in schema medicaid to group uthealth_admin; 
	alter default privileges in schema medicaid grant all on tables to uthealth_admin;
	---medicare_national
	grant all on schema medicare_national to group uthealth_admin; 
	grant all on all tables in schema medicare_national to group uthealth_admin; 
	alter default privileges in schema medicare_national grant all on tables to group uthealth_admin;
	---uthealth/medicare_national
	grant all on schema uthealth/medicare_national to group uthealth_admin; 
	grant all on all tables in schema uthealth/medicare_national to group uthealth_admin; 
	alter default privileges in schema uthealth/medicare_national grant all on tables to group uthealth_admin;
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

---(----------  End Role Definitions    ----------------------------------------------------------------------------------------------------)
---(----------  ********************   ----------------------------------------------------------------------------------------------------)





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
