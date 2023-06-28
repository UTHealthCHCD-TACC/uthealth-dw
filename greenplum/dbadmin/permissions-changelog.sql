
/* ******************************************************************************************************
 * This script contains several code blocks useful for controlling user access on the Greenplum Server
 * and contains definitions for user roles
 * ******************************************************************************************************
 *  Author 			|| Date      	|| Notes
 * ******************************************************************************************************
 * Xiaorui Zhang	|| 03/10/2023	|| Created, added users to apcd_uthealth_analyst
 * 
 * ****************************************************************************************************** */

--06/023/2023
--made staging_clean accessible to devs
grant all on schema staging_clean to uthealth_dev; 
grant all on all tables in schema staging_clean to uthealth_dev; 
grant all privileges on all sequences in schema staging_clean to uthealth_dev; 
alter default privileges in schema staging_clean grant all on tables to uthealth_dev; 

--made Femi a dev so he could access staging_clean
grant uthealth_dev to oaborisa;

--06/12/2023
--Jeff needs to access new tables created in conditions by Femi
grant uthealth_dev to jfu2; --jeff already has role uthealth_dev
alter default privileges for user oaborisa in schema conditions grant all on tables to uthealth_analyst;
--also refreshed permissions on schema conditions to uthealth_analyst and uthealth_dev

--06/06/2023
grant uthealth_dev to lghosh1;

--06/02/2023
--Yesterday's code didn't fix the issue where Femi couldn't delete Joe W's tables so let's try this
grant delete on all tables in schema dev to oaborisa;
grant uthealth_dev to oaborisa;

--06/01/2023
--grant permissions to uth_analyst for future tables created in dev schema for most active users
alter default privileges for user oaborisa in schema dev grant all on tables to uthealth_analyst;
alter default privileges for user jwozny in schema dev grant all on tables to uthealth_analyst;
alter default privileges for user cms2 in schema dev grant all on tables to uthealth_analyst;
alter default privileges for user ctruong in schema dev grant all on tables to uthealth_analyst;
alter default privileges for user xrzhang in schema dev grant all on tables to uthealth_analyst;
alter default privileges for user iperez9 in schema dev grant all on tables to uthealth_analyst;

--05/02/2023
--grant uthealth_dev to Kenneth, Aryan, Joe H (Garret is already uthealth_dev)
grant uthealth_dev to nguyken;
grant uthealth_dev to aryan;
grant uthealth_dev to jharri66;

--make their tables read-only for uthealth_analyst
alter default privileges for user gmunoz1 in schema crosswalk grant select on tables to uthealth_analyst;
alter default privileges for user nguyken in schema crosswalk grant select on tables to uthealth_analyst;
alter default privileges for user aryan in schema crosswalk grant select on tables to uthealth_analyst;
alter default privileges for user jwozny in schema crosswalk grant select on tables to uthealth_analyst;
alter default privileges for user aryan in schema crosswalk grant select on tables to uthealth_analyst;
alter default privileges for user jharri66 in schema crosswalk grant select on tables to uthealth_analyst;

--04/25/23
--grant uthealth_dev to Caroline
grant uthealth_dev to cms2;

--04/25/23
--give covid analyst to Lopita
grant covid_analyst to lghosh1;

--04/12/23
--Check Youngran's status

--check access for youngran - she is uthealth_analyst
SELECT r.rolname, 
  ARRAY(SELECT b.rolname
        FROM pg_catalog.pg_auth_members m
        JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)
        WHERE m.member = r.oid) as memberof
, r.rolsuper
FROM pg_catalog.pg_roles r
WHERE r.rolname !~ '^pg_'
ORDER BY 1;

--check access for truven.mdcrt
select grantee, privilege_type, table_name , table_schema 
from information_schema.role_table_grants 
where table_schema = 'truven' and table_name = 'mdcrt'
and grantee = 'uthealth_analyst';

--truven
grant usage on schema truven to group uthealth_analyst; 
grant select on all tables in schema truven to group uthealth_analyst; 
alter default privileges in schema truven grant select on tables to group uthealth_analyst;

--grant uthealth_analyst to youngran;
grant uthealth_analyst to judyk277;

--hotfix while we work this out
--actually we didn't need it, I think the Truven data refresh did something weird to MDCRT
--granting permissoins on truven to uthealth_analyst did the trick

--04/05/2023

--Femi's unable to alter tables in conditions schema bc he's not owner.
--It looks like owners are uthealth_admin because of what I did with David Walling and Will's tables before
--I'm gonna change all tables in schemas conditions and reference_tables to uthealth_dev

select 'alter table ' || table_schema || '.' || table_name ||
	' owner to uthealth_dev;'
from information_schema."tables"
where table_type = 'BASE TABLE'
	and table_schema = 'reference_tables'
order by table_name;

alter table reference_tables.condition_desc owner to uthealth_dev;
alter table reference_tables.cpt_hcpc owner to uthealth_dev;
alter table reference_tables.medicaid_lu_contract owner to uthealth_dev;
alter table reference_tables.medicaid_me_enrl owner to uthealth_dev;
alter table reference_tables.methodist_pos_temp owner to uthealth_dev;
alter table reference_tables.mrconso_cpt_hcpcs_hcpt owner to uthealth_dev;
alter table reference_tables.mrconso_en_pref owner to uthealth_dev;
alter table reference_tables.ndc_tier_map owner to uthealth_dev;
alter table reference_tables.nppes_2021 owner to uthealth_dev;
alter table reference_tables.nppes_provider_taxonomies owner to uthealth_dev;
alter table reference_tables.optum_zip_provider_categories owner to uthealth_dev;
alter table reference_tables.public_health_regions owner to uthealth_dev;
alter table reference_tables.redbook owner to uthealth_dev;
alter table reference_tables.ref_admit_source owner to uthealth_dev;
alter table reference_tables.ref_admit_type owner to uthealth_dev;
alter table reference_tables.ref_bill_type_cd owner to uthealth_dev;
alter table reference_tables.ref_bill_type_classification owner to uthealth_dev;
alter table reference_tables.ref_bill_type_frequency owner to uthealth_dev;
alter table reference_tables.ref_bill_type_institution owner to uthealth_dev;
alter table reference_tables.ref_bus_cd owner to uthealth_dev;
alter table reference_tables.ref_cms_codes owner to uthealth_dev;
alter table reference_tables.ref_cms_icd_cm_codes owner to uthealth_dev;
alter table reference_tables.ref_cms_icd_pcs_codes owner to uthealth_dev;
alter table reference_tables.ref_data_source owner to uthealth_dev;
alter table reference_tables.ref_discharge_status owner to uthealth_dev;
alter table reference_tables.ref_drg_mdcd owner to uthealth_dev;
alter table reference_tables.ref_employee_status owner to uthealth_dev;
alter table reference_tables.ref_gender owner to uthealth_dev;
alter table reference_tables.ref_medicare_entlmt_buyin owner to uthealth_dev;
alter table reference_tables.ref_medicare_ptd_cntrct owner to uthealth_dev;
alter table reference_tables.ref_medicare_state_codes owner to uthealth_dev;
alter table reference_tables.ref_month_year owner to uthealth_dev;
alter table reference_tables.ref_ndc_package owner to uthealth_dev;
alter table reference_tables.ref_ndc_product owner to uthealth_dev;
alter table reference_tables.ref_optum_cost_factor owner to uthealth_dev;
alter table reference_tables.ref_optum_type_of_service owner to uthealth_dev;
alter table reference_tables.ref_place_of_service owner to uthealth_dev;
alter table reference_tables.ref_plan_type owner to uthealth_dev;
alter table reference_tables.ref_provider_specialty owner to uthealth_dev;
alter table reference_tables.ref_race owner to uthealth_dev;
alter table reference_tables.ref_regions owner to uthealth_dev;
alter table reference_tables.ref_revenue_code owner to uthealth_dev;
alter table reference_tables.ref_truven_state_codes owner to uthealth_dev;
alter table reference_tables.ref_tx_county_regions owner to uthealth_dev;
alter table reference_tables.ref_type_of_service owner to uthealth_dev;
alter table reference_tables.ref_zip_code owner to uthealth_dev;
alter table reference_tables.ref_zip_crosswalk owner to uthealth_dev;
alter table reference_tables.rx_va_formulary owner to uthealth_dev;
alter table reference_tables.truven_prov_specialty_cds owner to uthealth_dev;
alter table reference_tables.zcta_county_2020 owner to uthealth_dev;

select 'alter table ' || table_schema || '.' || table_name ||
	' owner to uthealth_dev;'
from information_schema."tables"
where table_type = 'BASE TABLE'
	and table_schema = 'conditions'
order by table_name;

alter table conditions.codeset owner to uthealth_dev;
alter table conditions.condition_desc owner to uthealth_dev;
alter table conditions.condition_ndc owner to uthealth_dev;
alter table conditions.conditions_member_enrollment_yearly owner to uthealth_dev;
alter table conditions.diagnosis_codes_list owner to uthealth_dev;
alter table conditions.person_profile_stage owner to uthealth_dev;
alter table conditions.person_profile_work_table owner to uthealth_dev;
alter table conditions.xl_condition_asthma_dx_1 owner to uthealth_dev;
alter table conditions.xl_condition_asthma_dx_2 owner to uthealth_dev;
alter table conditions.xl_condition_asthma_dx_3 owner to uthealth_dev;
alter table conditions.xl_condition_asthma_dx_4 owner to uthealth_dev;
alter table conditions.xl_condition_asthma_dx_output owner to uthealth_dev;
alter table conditions.xl_condition_diabetes_1 owner to uthealth_dev;
alter table conditions.xl_condition_diabetes_3 owner to uthealth_dev;
alter table conditions.xl_condition_diabetes_output owner to uthealth_dev;

--03/30/2023:
--grant uthealth_dev privileges to Lopita so she can access
--new medicaid fiscal yearly table after chip perinatal and htw split out
grant uthealth_dev to lghosh1;

--Per request from Joe W. created schema crosswalk and assigned
--read/write to dev, read-only to analyst
create schema crosswalk;

--uthealth_dev gets full access to crosswalk
grant all on schema crosswalk to uthealth_dev; 
grant all on all tables in schema crosswalk to uthealth_dev; 
grant all privileges on all sequences in schema crosswalk to uthealth_dev; 
alter default privileges in schema crosswalk grant all on tables to uthealth_dev; 

--uthealth_analyst gets read-only access to crosswalk
grant usage on schema crosswalk to group uthealth_analyst; 
grant select on all tables in schema crosswalk to group uthealth_analyst; 
grant select on all sequences in schema crosswalk to uthealth_analyst;
alter default privileges in schema crosswalk grant select on tables to group uthealth_analyst;

--03/28/2023: Grant Femi and Jeff uthealth_dev role (they need to work on conditions)
grant uthealth_dev to jfu2;
grant uthealth_dev to oaborisa;

--conditions (all access)
grant all on schema conditions to uthealth_dev; 
grant all on all tables in schema conditions to uthealth_dev; 
grant all privileges on all sequences in schema conditions to uthealth_dev; 
alter default privileges in schema conditions grant all on tables to uthealth_dev; 

--dw_staging (all access)
grant all on schema dw_staging to uthealth_dev; 
grant all on all tables in schema dw_staging to uthealth_dev; 
grant all privileges on all sequences in schema dw_staging to uthealth_dev; 
alter default privileges in schema dw_staging grant all on tables to uthealth_dev; 


--03/22/2023: Change Maria's access to uthealth_analyst
--Rationale: When she joined we gave her dev b/c of... ignorance, mostly.
--But anyway she doesn't need write access to dw

grant uthealth_analyst to ukhanova;
revoke uthealth_dev from ukhanova;

--check access
SELECT r.rolname, 
  ARRAY(SELECT b.rolname
        FROM pg_catalog.pg_auth_members m
        JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)
        WHERE m.member = r.oid) as memberof
, r.rolsuper
FROM pg_catalog.pg_roles r
WHERE r.rolname !~ '^pg_'
ORDER BY 1;

--03/20/2023: Refresh access for uthealth_analyst

--make a test table
create table reference_tables.access_test as
select * from reference_tables.cpt_hcpc limit 10;

drop table if exists reference_tables.access_test;

--reference_tables (select only )
--apparently running this grants all analysts access to new tables in ref
grant usage on schema reference_tables to group uthealth_analyst; 
grant select on all tables in schema reference_tables to group uthealth_analyst; 
grant select on all sequences in schema reference_tables to uthealth_analyst;
alter default privileges in schema reference_tables grant select on tables to group uthealth_analyst;

--dev (all access)
grant all on schema dev to uthealth_analyst; 
grant all on all tables in schema dev to uthealth_analyst; 
grant all privileges on all sequences in schema dev to uthealth_analyst; 
alter default privileges in schema dev grant all on tables to uthealth_analyst;

--data_warehouse
grant usage on schema data_warehouse to uthealth_analyst; 
grant select on all tables in schema data_warehouse to uthealth_analyst; 
grant select on all sequences in schema data_warehouse to uthealth_analyst;
alter default privileges in schema data_warehouse grant select on tables to uthealth_analyst;

--Check Joe's access
SELECT r.rolname, 
  ARRAY(SELECT b.rolname
        FROM pg_catalog.pg_auth_members m
        JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)
        WHERE m.member = r.oid) as memberof
, r.rolsuper
FROM pg_catalog.pg_roles r
WHERE r.rolname !~ '^pg_'
and r.rolname like 'jw%'
ORDER BY 1;

--check femi's access
SELECT r.rolname, 
  ARRAY(SELECT b.rolname
        FROM pg_catalog.pg_auth_members m
        JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)
        WHERE m.member = r.oid) as memberof
, r.rolsuper
FROM pg_catalog.pg_roles r
WHERE r.rolname !~ '^pg_'
and r.rolname like 'oa%'
ORDER BY 1;


--03/10/2023: Created apcd_uthealth_analyst role

/*******************************************************************
 * Role definitions: apcd_uthealth_analyst
 * 
 * Purpose: Gives read access to ONLY optum_zip and data_warehouse
 * 		Gives read/write access to dev
 *******************************************************************/
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

--granted apcd_uthealth_analyst role to APCD group
grant apcd_uthealth_analyst to nguyken;
grant apcd_uthealth_analyst to mjames11;
grant apcd_uthealth_analyst to stabot90; --this username does not exist
grant apcd_uthealth_analyst to sabot90; --this exists, not sure if typo
grant apcd_uthealth_analyst to alonab13;
grant apcd_uthealth_analyst to bjack10;
grant apcd_uthealth_analyst to aryan;

--checked permissions
--Lists usernames/rolenames, assigned roles, and superuser status
--just for APCD people
SELECT r.rolname, 
  ARRAY(SELECT b.rolname
        FROM pg_catalog.pg_auth_members m
        JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)
        WHERE m.member = r.oid) as memberof
, r.rolsuper
FROM pg_catalog.pg_roles r
WHERE r.rolname !~ '^pg_'
and r.rolname in ('nguyken', 'mjames11', 'sabot90', 'alonab13', 'bjack10', 'aryan')
ORDER BY 1;

--Caroline requested access to one of Femi's tables in dev for both her and Gina
--Xiaorui was still learning how these permissions work so I kind of used the shotgun approach
set role cms2;
alter default privileges for user cms2 in schema dev grant all on tables to uthealth_analyst;
set role rhansen1;
alter default privileges for user rhansen1 in schema dev grant all on tables to uthealth_analyst;
set role oaborisa;
alter default privileges for user oaborisa in schema dev grant all on tables to uthealth_analyst;



















