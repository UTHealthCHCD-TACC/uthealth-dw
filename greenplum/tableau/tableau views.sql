create schema tableau; 

drop view tableau.claim_header_optz_2019 ;

create view tableau.claim_header_optz_truv
as 
select * 
from data_warehouse.claim_header ch 
where year between 2018 and 2019
  and data_source in ('optz', 'truv')
;


create view tableau.enrollment_yearly_optz_truv
as 
select * 
from data_warehouse.member_enrollment_yearly 
where year between 2018 and 2019
  and data_source in ('optz', 'truv')
;



select data_source, year, claim_type, count(uth_claim_id) as claims, sum(total_allowed_amount) as allowed
from tableau.claim_header_optz_truv
group by data_source, year, claim_type
order by data_source, year, claim_type
;


select data_source, year, bus_cd, count(uth_member_id) as mems
from tableau.enrollment_yearly_optz_truv 
group by  data_source, year, bus_cd
order by  data_source, year, bus_cd
;



SELECT count(*)