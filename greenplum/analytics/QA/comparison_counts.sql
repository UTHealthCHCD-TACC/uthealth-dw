select 'enrollment_monthly', data_source, year, count(*)
from data_warehouse.member_enrollment_monthly
group by 1, 2, 3
union
select 'enrollment_yearly', data_source, year, count(*)
from data_warehouse.member_enrollment_yearly
group by 1, 2, 3
union 
select 'claim_header', data_source, year, count(*)
from data_warehouse.claim_header ch 
group by 1, 2, 3
union
select 'claim_detail', data_source, year, count(*)
from data_warehouse.claim_detail cd 
group by 1, 2, 3
union
select 'claim_diag', data_source, year, count(*)
from data_warehouse.claim_diag cd2
group by 1, 2, 3
union
select 'claim_icd_proc', data_source, year, count(*)
from data_warehouse.claim_icd_proc cip 
group by 1, 2, 3
order by 1, 2, 3;
