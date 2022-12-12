insert into dw_staging.member_enrollment_monthly
select * from data_warehouse.member_enrollment_monthly 
   where data_source <> 'truv';

analyze dw_staging.member_enrollment_monthly;