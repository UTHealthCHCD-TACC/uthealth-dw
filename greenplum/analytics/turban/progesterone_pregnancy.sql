select *
from dev.tu_pregnancy_patients_2018
limit 100;

select uth_member_id, count(*) dxs
from dev.tu_pregnancy_patients_2018
group by uth_member_id
having count(*) > 1
order by uth_member_id;

select *
from dev.tu_pregnancy_patients_2018
where uth_member_id = 151802094;

select count(*) from dev.tu_pregnancy_patients_2018;

-- Find specific codes for pre-term birth related DXs in 2018 grouped by DX code

select * into dev.pregnancy_2018_staging from (

select distinct diag_cd, dx.description, data_source, cd.uth_member_id, cd."date"
from data_warehouse.claim_diag cd 
left join reference_tables.icd_10_diags dx on dx.code = cd.diag_cd 
where uth_member_id in (select uth_member_id from dev.tu_pregnancy_patients_2018)

union

select '' as diag_cd, concat(generic_name, '/', brand_name) as description, data_source, uth_member_id, fill_date as "date"
from data_warehouse.pharmacy_claims pc 
where uth_member_id in (select uth_member_id from dev.tu_pregnancy_patients_2018)) sq
order by "date"

select * from data_warehouse.pharmacy_claims pc 
where generic_name ilike 'progesterone';

select * into dev.tu_pregnancies_with_progesterone_rx_2018
from dev.tu_pregnancy_2018_staging tps 
where uth_member_id in (select distinct uth_member_id from dev.tu_pregnancy_2018_staging where description ilike '%progesterone%')
order by uth_member_id, "date";

select *
from dev.tu_pregnancies_with_progesterone_rx_2018
where (diag_cd like 'O60%'
or (description ilike '%progesterone%'))
and (uth_member_id in (select uth_member_id from dev.tu_pregnancy_2018_staging tps where diag_cd like 'O601%'))
order by uth_member_id, "date";