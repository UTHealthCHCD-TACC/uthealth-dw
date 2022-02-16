
/* ******************************************************************************************************
 *  This script will move updated tables from dw_staging to data_warehouse. This is the final step in the update process
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 * ******************************************************************************************************
 *  wc001  || 9/29/2021 || script creation
 * ****************************************************************************************************** 
 *  wc002  || 10/05/2021 || add mbsf_abcd table
 * ****************************************************************************************************** 
 *  wc003  || 02/09/2022 || add partition code block
 * ****************************************************************************************************** 
 * */

----rename existing production tables to _old
alter table data_warehouse.claim_header rename to claim_header_old; 

alter table data_warehouse.claim_detail rename to claim_detail_old; 

alter table data_warehouse.claim_diag rename to claim_diag_old; 

alter table data_warehouse.claim_icd_proc rename to claim_icd_proc_old;

alter table data_warehouse.pharmacy_claims rename to pharmacy_claims_old; 

alter table data_warehouse.member_enrollment_monthly rename to member_enrollment_monthly_old;

alter table data_warehouse.member_enrollment_yearly rename to member_enrollment_yearly_old;

alter table data_warehouse.medicare_mbsf_abcd_enrollment rename to medicare_mbsf_abcd_enrollment_old;

alter table data_warehouse.medicaid_program_enrollment rename to medicaid_program_enrollment_old;
----


-----//   move staging tables to production
alter table dw_staging.claim_header set schema data_warehouse;

alter table dw_staging.claim_detail set schema data_warehouse;

alter table dw_staging.claim_diag set schema data_warehouse;

alter table dw_staging.claim_icd_proc  set schema data_warehouse;

alter table dw_staging.pharmacy_claims set schema data_warehouse;

alter table dw_staging.member_enrollment_monthly set schema data_warehouse;

alter table dw_staging.member_enrollment_yearly set schema data_warehouse;

alter table dw_staging.medicare_mbsf_abcd_enrollment set schema data_warehouse;

alter table dw_staging.medicaid_program_enrollment set schema data_warehouse;
-----//


---|| drop previous old tables from staging 
drop table if exists  dw_staging.claim_header_old  ;

drop table if exists  dw_staging.claim_detail_old  ;

drop table if exists  dw_staging.claim_diag_old  ;

drop table if exists dw_staging.claim_icd_proc_old  ;

drop table if exists  dw_staging.pharmacy_claims_old  ;

drop table if exists  dw_staging.member_enrollment_monthly_old  ;

drop table if exists  dw_staging.member_enrollment_yearly_old  ;

drop table if exists  dw_staging.medicare_mbsf_abcd_enrollment_old  ;

drop table if exists dw_staging.medicaid_program_enrollment_old  ;



-----||  move old tables to staging
alter table  data_warehouse.claim_header_old set schema dw_staging;

alter table  data_warehouse.claim_detail_old set schema dw_staging;

alter table  data_warehouse.claim_diag_old set schema dw_staging;

alter table data_warehouse.claim_icd_proc_old set schema dw_staging;

alter table  data_warehouse.pharmacy_claims_old set schema dw_staging;

alter table  data_warehouse.member_enrollment_monthly_old set schema dw_staging;

alter table  data_warehouse.member_enrollment_yearly_old set schema dw_staging;

alter table  data_warehouse.medicare_mbsf_abcd_enrollment_old set schema dw_staging;

alter table data_warehouse.medicaid_program_enrollment_old set schema dw_staging;
-----||


do $$ 
	declare 
	r_dw_partition text;
	r_stage_partition text;
	r_dbo_partition text;
begin 
	--move old data warehouse partitions to dbo 
	for r_dw_partition in 
	select  partitiontablename 
	from pg_catalog.pg_partitions
	where partitionschemaname = 'data_warehouse'
	
	loop 
			execute 'alter table data_warehouse.' || r_dw_partition || ' set schema dbo;';
	end loop;
	
	--move new staging partition to data_warehouse 
	for r_stage_partition in 
	select  partitiontablename 
	from pg_catalog.pg_partitions
	where partitionschemaname = 'dw_staging'
	
	loop 
			execute 'alter table dw_staging.' || r_stage_partition || ' set schema data_warehouse;';
	end loop;
	

		--move new staging partition to data_warehouse 
	for r_dbo_partition in 
	select  partitiontablename 
	from pg_catalog.pg_partitions
	where partitionschemaname = 'dbo'
	
	loop 
			execute 'alter table dbo.' || r_dbo_partition || ' set schema dw_staging;';
	end loop;

	raise notice 'done';
end
$$

--validate
select *
from pg_catalog.pg_partitions
;

