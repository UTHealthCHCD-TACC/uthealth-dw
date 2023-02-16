create view qa_reporting.member_counts_yearly
as
select e.uth_member_id, e."year" 
from data_warehouse.member_enrollment_yearly e
group by 1, 2
having count(*) > 1;

create view qa_reporting.member_counts_monthly
as
select e.uth_member_id, e."year" 
from data_warehouse.member_enrollment_monthly e
group by 1, 2
having count(*) > 12;