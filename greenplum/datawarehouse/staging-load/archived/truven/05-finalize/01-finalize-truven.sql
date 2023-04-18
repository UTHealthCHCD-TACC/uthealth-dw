----remove all loading tables 
drop table if exists staging_clean.ccaed_etl;
drop table if exists staging_clean.ccaeo_etl;
drop table if exists staging_clean.ccaes_etl;
drop table if exists staging_clean.mdcrd_etl;
drop table if exists staging_clean.mdcro_etl;
drop table if exists staging_clean.mdcrs_etl;
drop table if exists staging_clean.truv_ccaef_etl;
drop table if exists staging_clean.truv_dim_id;
drop table if exists staging_clean.truv_mdcrf_etl;
drop table if exists staging_clean.truven_rx_claim_id;



--- insert records from data warehouse that aren't truven
insert into dw_staging.member_enrollment_monthly 
select * from data_warehouse.member_enrollment_monthly 
where data_source <> 'truv';

insert into dw_staging.member_enrollment_yearly  
select * from data_warehouse.member_enrollment_yearly 
where data_source <> 'truv';

insert into dw_staging.claim_header  
select * from data_warehouse.claim_header 
where data_source <> 'truv';

insert into dw_staging.claim_detail  
select * from data_warehouse.claim_detail 
where data_source <> 'truv';

insert into dw_staging.claim_diag  
select * from data_warehouse.claim_diag  
where data_source <> 'truv';

insert into dw_staging.claim_icd_proc  
select * from data_warehouse.claim_icd_proc  
where data_source <> 'truv';

insert into dw_staging.pharmacy_claims  
select * from data_warehouse.pharmacy_claims  
where data_source <> 'truv';

--- vacuum tables 
vacuum full analyze dw_staging.member_enrollment_yearly ;
vacuum full analyze dw_staging.member_enrollment_monthly  ;
vacuum full analyze dw_staging.claim_header ;
vacuum full analyze dw_staging.claim_detail  ;
vacuum full analyze dw_staging.claim_diag  ;
vacuum full analyze dw_staging.claim_icd_proc  ;
vacuum full analyze dw_staging.pharmacy_claims ;

--- 


