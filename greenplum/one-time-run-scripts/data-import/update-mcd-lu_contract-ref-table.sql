/***************************************************************
 * This SQL code assumes that you've already run the R markdown code
 * that uploads the raw table to Greenplum
 * 
 * March 2023 we noticed that some Medicaid plan_cd were missing from our
 * lookup table. Rachel Neave asked Lisa Kalkanis @ HHSC for an updated table
 * and here we are
 * 
 * Modifications to the raw table:
 * 		1) delete rows where plan_cd = ''
 * 		2) the same plan_cd sometimes spans 2 rows - when this happens, one row is active
 * 			and the other inactive. Delete the inactive row.
 * 
 ***************************************************************/

select a.plan_cd as plan_cd_original, a.mco_program_nm as program_nm_original,
	b.plan_cd as plan_cd_new, b.mco_program_nm as program_nm_new
from backup.medicaid_lu_contract a full outer join reference_tables.medicaid_lu_contract b
on a.plan_cd = b.plan_cd and a.mco_program_nm = b.mco_program_nm
where a.plan_cd is null or b.plan_cd is null
order by a.plan_cd, b.plan_cd;

-- move the first table to the backup schema
create schema if not exists backup;
alter table reference_tables.medicaid_lu_contract set schema backup;

-- change the name of the second table to reference_tables.medicaid_lu_contract
alter table reference_tables.medicaid_lu_contract_upd rename to medicaid_lu_contract;

--delete row where plan_cd is empty
delete from reference_tables.medicaid_lu_contract
where plan_cd = '';
--Updated Rows	1

--some plan_cds have > 1 row - delete row that has active_ind = 'N' (delete inactive record)
delete from reference_tables.medicaid_lu_contract
where active_ind = 'N' and plan_cd in (
	select plan_cd from reference_tables.medicaid_lu_contract
	group by plan_cd
	having count(*) > 1
);

--update data_warehouse.update_log
update data_warehouse.update_log
set data_last_updated = current_date, --today is 03/29/2023
	details = 'Updated reference table after noticing KW plan_cd was missing'
where schema_name = 'reference_tables' and table_name = 'medicaid_lu_contract';