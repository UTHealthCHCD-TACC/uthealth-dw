
------------------
drop table if exists tableau.dashboard_1720;


create table tableau.dashboard_1720
with (appendoptimized=true, orientation=column, compresstype=zlib)
as 
select a.data_source, a."year" , a.uth_member_id ,total_enrolled_months, gender_cd, age_derived , state, plan_type, bus_cd,
       uth_claim_id , claim_type, total_charge_amount , total_allowed_amount, total_paid_amount
from dw_staging.member_enrollment_yearly a 
    left outer join dw_staging.claim_header b 
     on a.uth_member_id = b.uth_member_id 
    and a."year" = b."year" 
where a.year between 2017 and 2020
  and a.data_source in ('optz', 'truv','mcrt')
distributed by (uth_member_id)
;

alter table tableau.dashboard_1720 owner to uthealth_analyst;

analyze tableau.dashboard_1720;


---
drop table if exists tableau.dashboard_conditions_1720;


create table tableau.dashboard_conditions_1720
with (appendoptimized=true, orientation=column, compresstype=zlib)
as 
select a.data_source, a."year" , a.uth_member_id ,total_enrolled_months, gender_cd, age_derived , state, plan_type, bus_cd,
       uth_claim_id , claim_type, total_charge_amount , total_allowed_amount, total_paid_amount, 
from conditions.member_enrollment_yearly a 
    left outer join data_warehouse.claim_header b 
     on a.uth_member_id = b.uth_member_id 
    and a."year" = b."year" 
where a.year between 2017 and 2020
  and a.data_source in ('optz', 'truv','mcrt')
distributed by (uth_member_id)
;

alter table tableau.dashboard_conditions_1720 owner to uthealth_analyst;

analyze tableau.dashboard_conditions_1720;