/*
 * Benchmarks for query on Corral (New) and Wrangler (Old) Greenplums databases
 * Timings = 'old(seconds)/new(seconds)'
 */

-- Settings
select * 
from pg_catalog.pg_settings
where name like '%seq%';

-- 36 / 50
explain analyze
select ch.uth_claim_id, cd.bill_provider_id, cd.charge_amount 
from data_warehouse.claim_header  ch 
join data_warehouse.claim_detail cd on ch.uth_member_id = cd.uth_member_id and ch.uth_claim_id = cd.uth_claim_id 
where ch.data_year = 2016
and ch.data_source = 'optz';

-- 59 / 109
explain analyze
select data_source, year, count(*)
from data_warehouse.claim_diag
where diag_cd like '001%'
group by 1, 2
order by 1, 2;

-- 67 / 144
select count(*), count(distinct uth_rx_claim_id)
from data_warehouse.pharmacy_claims;

-- 129 / 153
explain analyze
select mey.data_source, pc.year, mey.gender_cd, pc.ndc, count(*)
from data_warehouse.pharmacy_claims pc
join data_warehouse.member_enrollment_yearly mey on pc.uth_member_id = mey.uth_member_id and pc."year" = mey."year" 
join data_warehouse.claim_diag cd on mey.uth_member_id = cd.uth_member_id and pc.year <= cd.year
where cd.diag_cd like '001%'
group by 1, 2, 3, 4
order by 1, 2, 4, 3;


