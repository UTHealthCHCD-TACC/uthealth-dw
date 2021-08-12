--Old: 34.3 s
--New: 13.4 s
analyze data_warehouse.member_enrollment_monthly;

--Old: 12.1 s
--New: 3.4 s
select count(*)
from data_warehouse.member_enrollment_monthly;

--Old: 13.6 s
--New: 4.8 s
select month_year_id, count(*)
from data_warehouse.member_enrollment_monthly
group by 1;

--Old: 1 m 30 s
--New: 34.4 s
analyze data_warehouse.claim_detail_v1;

--Old: 42.4 s
--New: 13.4 s
-- 
select data_source, procedure_type, count(*)
from data_warehouse.claim_detail_v1
where month_year_id > 2016
group by 1, 2;


--old: 2 m 46 s
--new: 91.5 s
analyze truven.ccaeo;

--old: 22.6 s
--new: 6.8 s
select count(*) from truven.ccaeo;

--old: X
--new: 55.5 s
analyze optum_dod.medical;

--old: X
--new: 6 s
select count(*) from truven.ccaeo;
