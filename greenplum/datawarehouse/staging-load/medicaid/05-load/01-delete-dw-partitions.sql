/*monthly enrollment*/

delete from data_warehouse.member_enrollment_monthly_1_prt_mdcd;

vacuum analyze data_warehouse.member_enrollment_monthly_1_prt_mdcd;

insert into data_warehouse.member_enrollment_monthly_1_prt_mdcd
select * 
  from dw_staging.member_enrollment_monthly;
 
vacuum analyze data_warehouse.member_enrollment_monthly_1_prt_mdcd;

/*yearly enrollment*/

delete from data_warehouse.member_enrollment_yearly_1_prt_mdcd;

vacuum analyze data_warehouse.member_enrollment_yearly_1_prt_mdcd;

insert into data_warehouse.member_enrollment_yearly_1_prt_mdcd
select * 
  from dw_staging.member_enrollment_yearly;
 
vacuum analyze data_warehouse.member_enrollment_yearly_1_prt_mdcd;


select count(*) from data_warehouse.member_enrollment_yearly_1_prt_mdcd;
select count(*) from dw_staging.member_enrollment_yearly;
select distinct year from dw_staging.member_enrollment_yearly;

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