insert into dw_staging.member_enrollment_monthly
select * from data_warehouse.member_enrollment_monthly 
   where data_source <> 'truv';

analyze dw_staging.member_enrollment_monthly;

---
insert into dw_staging.member_enrollment_yearly 
select * from data_warehouse.member_enrollment_yearly 
   where data_source <> 'truv';

analyze dw_staging.member_enrollment_yearly;

delete from dw_staging.member_enrollment_yearly_1_prt_truv;
vacuum analyze dw_staging.member_enrollment_yearly ;

---
insert into dw_staging. 
select * from data_warehouse.member_enrollment_yearly 
   where data_source <> 'truv';
