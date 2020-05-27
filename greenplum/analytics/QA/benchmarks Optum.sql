

select  count(distinct uth_member_id), sum(a.total_enrolled_months)
from data_warehouse.member_enrollment_yearly a
where year = 2018
and data_source = 'optz'
 and a.uth_member_id in ( select uth_member_id 
 						  from data_warehouse.member_enrollment_monthly b 
 						  where b.state = 'TX' 
 						  and year = 2018
 						  and bus_cd = 'COM')
;


select count(uth_claim_id),  sum(a.total_charge_amount), sum(a.total_allowed_amount)
from data_warehouse.claim_header a 
where a.data_source = 'optz'
  and a.year = 2018  
  and a.uth_member_id in ( select uth_member_id 
 						  from data_warehouse.member_enrollment_monthly b 
 						  where b.state = 'TX' 
 						  and year = 2018
 						  and bus_cd = 'COM')
  ;
 
  
  
select count(uth_rx_claim_id), sum(a.total_charge_amount ) 
from data_warehouse.pharmacy_claims a 
where a.data_source = 'optz'
  and a.year = 2018
  and a.uth_member_id in ( select uth_member_id 
 						  from data_warehouse.member_enrollment_monthly b 
 						  where b.state = 'TX' 
 						  and year = 2018
 						  and bus_cd = 'COM')
; 						