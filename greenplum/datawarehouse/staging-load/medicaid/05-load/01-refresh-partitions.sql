/**********************************
 * Note that attach/detach are for PostgreSQL version 11+... so we can't do it on GP
 */


/*******************************
 * Monthly Enrollment
 ******************************/

--delete old data
delete from data_warehouse.member_enrollment_monthly_1_prt_mdcd;
vacuum analyze data_warehouse.member_enrollment_monthly_1_prt_mdcd;

--insert new data (general Medicaid)
insert into data_warehouse.member_enrollment_monthly_1_prt_mdcd
select * from dw_staging.mcd_member_enrollment_monthly_1_prt_mdcd;
vacuum analyze data_warehouse.member_enrollment_monthly_1_prt_mdcd;

--insert CHIP Perinatal partition into monthly enrollment table
insert into data_warehouse.member_enrollment_monthly_1_prt_mcpp
select * from dw_staging.mcd_member_enrollment_monthly_1_prt_mcpp;
 
--insert HTW partition into monthly enrollment table
insert into data_warehouse.member_enrollment_monthly_1_prt_mhtw
select *  from dw_staging.mcd_member_enrollment_monthly_1_prt_mhtw;

--vacuum analyze
vacuum analyze data_warehouse.member_enrollment_monthly_1_prt_mdcd;
vacuum analyze data_warehouse.member_enrollment_monthly_1_prt_mcpp;
vacuum analyze data_warehouse.member_enrollment_monthly_1_prt_mhtw;

/*******************************
 * Yearly Enrollment
 ******************************/
 
--delete old data
delete from data_warehouse.member_enrollment_yearly_1_prt_mdcd;
vacuum analyze data_warehouse.member_enrollment_yearly_1_prt_mdcd;

--insert new data (general Medicaid)
insert into data_warehouse.member_enrollment_yearly_1_prt_mdcd
select * from dw_staging.mcd_member_enrollment_yearly_1_prt_mdcd;

--insert new data (CHIP Perinatal)
insert into data_warehouse.member_enrollment_yearly_1_prt_mcpp
select * from dw_staging.mcd_member_enrollment_yearly_1_prt_mcpp;

--insert new data (HTW)
insert into data_warehouse.member_enrollment_yearly_1_prt_mhtw
select * from dw_staging.mcd_member_enrollment_yearly_1_prt_mhtw;

--vacuum analyze
vacuum analyze data_warehouse.member_enrollment_yearly_1_prt_mdcd;
vacuum analyze data_warehouse.member_enrollment_yearly_1_prt_mcpp;
vacuum analyze data_warehouse.member_enrollment_yearly_1_prt_mhtw;

/*******************************
 * Fiscal Yearly Enrollment'
 * 
 * Note that Medicaid is the only data source we are doing a fiscal yearly
 * enrollment with so... for now just delete the table and swap schema names
 ******************************/

drop table if exists data_warehouse.member_enrollment_fiscal_yearly;

create table data_warehouse.member_enrollment_fiscal_yearly
(like dw_staging.mcd_member_enrollment_fiscal_yearly including defaults) 
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
distributed by (uth_member_id)
partition by list(data_source)
 (partition mdcd values ('mdcd'),
  partition mhtw values ('mhtw'),
  partition mcpp values ('mcpp')
 )
;

insert into data_warehouse.member_enrollment_fiscal_yearly
select * from dw_staging.mcd_member_enrollment_fiscal_yearly;

vacuum analyze data_warehouse.member_enrollment_fiscal_yearly;

/*********code below here not run by XRZ**********/

















/*claim header*/

delete from data_warehouse.claim_header_1_prt_mdcd;

vacuum analyze data_warehouse.claim_header_1_prt_mdcd;

insert into data_warehouse.claim_header_1_prt_mdcd
select * 
  from dw_staging.claim_header ;
 
vacuum analyze data_warehouse.claim_header_1_prt_mdcd;


/*claim proc*/

delete from data_warehouse.claim_icd_proc_1_prt_mdcd ;

vacuum analyze data_warehouse.claim_icd_proc_1_prt_mdcd;
vacuum full analyze dw_staging.claim_icd_proc ;


insert into data_warehouse.claim_icd_proc_1_prt_mdcd
select * 
  from dw_staging.claim_icd_proc  ;
 
vacuum analyze data_warehouse.claim_icd_proc_1_prt_mdcd;


/*claim diag*/

delete from data_warehouse.claim_diag_1_prt_mdcd  ;

vacuum full analyze data_warehouse.claim_diag_1_prt_mdcd;
vacuum full analyze dw_staging.claim_diag_1_prt_mdcd;


insert into data_warehouse.claim_diag_1_prt_mdcd
select * 
  from dw_staging.claim_diag_1_prt_mdcd;
 
vacuum analyze data_warehouse.claim_diag_1_prt_mdcd;


/*claim detail*/
delete from data_warehouse.claim_detail_1_prt_mdcd  ;

vacuum full data_warehouse.claim_detail_1_prt_mdcd;

insert into data_warehouse.claim_detail_1_prt_mdcd
select * 
  from dw_staging.claim_detail  ;
 
vacuum analyze data_warehouse.claim_detail_1_prt_mdcd;