
/* ******************************************************************************************************
 * This script is the changelog of permissions for apcd_dev
 * 
 * ******************************************************************************************************
 *  Author 			|| Date      	|| Notes
 * ******************************************************************************************************
 * Xiaorui Zhang	|| 04/27/2023	|| Created
 * 
 * ****************************************************************************************************** */

--04/27/2023
--Revoked access to cdl_raw from apcd_uthealth_analyst
revoke all on schema cdl_raw from group apcd_uthealth_analyst; 
revoke all on all tables in schema cdl_raw from group apcd_uthealth_analyst; 
revoke all on all sequences in schema cdl_raw from group apcd_uthealth_analyst;
revoke usage on schema cdl_raw from group apcd_uthealth_analyst;

--grant apcd_uthealth_dev to Xiaorui
grant apcd_uthealth_dev to xrzhang;

--revoke apcd_uthealth_dev from Kenneth
revoke apcd_uthealth_dev from nguyken;

--check
select r.rolname as username, b.rolname as role
from pg_catalog.pg_roles r join pg_catalog.pg_auth_members m on r.oid = m.member
join pg_catalog.pg_roles b on m.roleid = b.oid
where b.rolname = 'apcd_uthealth_dev'
order by r.rolname;

--