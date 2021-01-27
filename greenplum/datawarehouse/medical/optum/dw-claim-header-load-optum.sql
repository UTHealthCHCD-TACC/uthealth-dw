/*
 * Remove old records
 */
select count(*) from data_warehouse.claim_header where data_source in ('optd','optz');

delete from data_warehouse.claim_header where data_source in ('optd','optz');


vacuum analyze data_warehouse.claim_header;

select data_source, count(*) as total from data_warehouse.claim_deatil where data_source in ('optd','optz')
group by data_source;


vacuum analyze data_warehouse.claim_detail;
/
 * *
 * We assume the matching records exist in dim_uth_claim_id
 */
--Optum load: 
-- Full years = 8132 seconds = 2h15m

--optd
insert into data_warehouse.claim_header(
		data_source, uth_member_id, member_id_src, uth_claim_id, claim_id_src, year,
		year_adj, claim_type,
	    from_date_of_service,
		total_charge_amount, total_allowed_amount, total_paid_amount
		, total_charge_amount_adj, total_allowed_amount_adj, total_paid_amount_adj, data_year
		)
	select 'optd', uthc.uth_member_id, m.patid, uthc.uth_claim_id, m.clmid, extract(year from (min(m.fst_dt))),
	m.std_cost_yr::int,
	cf.claim_type_code,
	min(m.fst_dt) as from_date_of_service,
	sum(m.charge) as total_charge_amount, 
	sum(m.std_cost) as total_allowed_amount, 
	null as total_paid_amount,
	sum((m.charge * cf.cost_factor)) as total_charge_amount, 
	sum((m.std_cost * cf.cost_factor)) as total_allowed_amount_adj, 
	null as total_paid_amount_adj,
	m.year
from optum_dod.medical m
	join data_warehouse.dim_uth_claim_id uthc 
		on uthc.data_source = 'optd' 
		and m.patid::text = uthc.member_id_src 
		and m.clmid = uthc.claim_id_src
	join reference_tables.ref_optum_cost_factor cf on cf.service_type = left(m.tos_cd, (position('.' in m.tos_cd)-1)) and cf.standard_price_year = m.std_cost_yr::int
  where m.year = 2020
	group by 1, 2, 3, 4, 5, m.std_cost_yr, cf.claim_type_code, m.year
;




insert into data_warehouse.claim_header(
		data_source, uth_member_id, member_id_src, uth_claim_id, claim_id_src, year,
		year_adj, claim_type,
	    from_date_of_service,
		total_charge_amount, total_allowed_amount, total_paid_amount
		, total_charge_amount_adj, total_allowed_amount_adj, total_paid_amount_adj, data_year
		)
	select distinct on(uthc.uth_claim_id)
	'optz', uthc.uth_member_id, m.patid::text, uthc.uth_claim_id, m.clmid, extract(year from (min(m.fst_dt) over(partition by uthc.uth_claim_id))),
	m.std_cost_yr::int,
	cf.claim_type_code,
	min(m.fst_dt) over(partition by uthc.uth_claim_id) as from_date_of_service,
	sum(m.charge) over(partition by uthc.uth_claim_id) as total_charge_amount, 
	sum(m.std_cost) over(partition by uthc.uth_claim_id) as total_allowed_amount, 
	null as total_paid_amount,
	sum((m.charge * cf.cost_factor)) over(partition by uthc.uth_claim_id) as total_charge_amount, 
	sum((m.std_cost * cf.cost_factor)) over(partition by uthc.uth_claim_id) as total_allowed_amount_adj, 
	null as total_paid_amount_adj,
	m.year
from optum_zip.medical m
	join data_warehouse.dim_uth_claim_id uthc 
		on m.patid::text = uthc.member_id_src 
		and m.clmid = uthc.claim_id_src
	join reference_tables.ref_optum_cost_factor cf 
	    on cf.service_type = left(m.tos_cd, (position('.' in m.tos_cd)-1)) 
	   and cf.standard_price_year = m.std_cost_yr::int
where m.year = 2019
	--group by 1, 2, 3, 4, 5, m.std_cost_yr, cf.claim_type_code, m.year
;


delete from data_warehouse.claim_header where data_source = 'optz' and data_year = 2019;

vacuum analyze data_warehouse.claim_header

select data_year, count(*), count(distinct uth_claim_id)
from data_warehouse.claim_header
where data_source = 'optz'
group by data_year
order by data_year;


select year, count(*), count(distinct m.clmid)
from optum_zip.medical m 
where m.clmseq = '001'
group by m."year" 
order by m.year 

select * from optum_zip.medical m where year = 2020;

select * from optum_zip.medical m where m."year" = 2020 order by clmid;






/*
 * Scratch Space
 */
select data_source, claim_id_src, member_id_src, uth_member_id, count(*)  
from data_warehouse.dim_uth_claim_id
group by 1,2,3,4
having count(*) > 1;

select *
from data_warehouse.dim_uth_claim_id
where claim_id_src ='755.0'
order by member_id_src;

SET work_mem = '4024MB';
SET statement_mem = '4024MB';
select distinct claim_type from data_warehouse.claim_header ch ;

select year, count(*)
from optum_zip_refresh.medical m2
group by 1;

select *
from optum_zip_refresh.medical m
join data_warehouse.dim_uth_claim_id uthc on uthc.data_source='optz' and m.patid::text=uthc.member_id_src and m.clmid=uthc.claim_id_src
limit 10;

explain analyze
select data_source, min(from_date_of_service), max(from_date_of_service), count(*), count(distinct member_id_src || claim_id_src )
from data_warehouse.claim_header
group by 1;

select data_source, count(*)
from  data_warehouse.claim_header
group by 1;






select * 
from data_warehouse.claim_header_v1
where data_source='trvc'
and uth_claim_id=15100057738;

select * 
from data_warehouse.claim_header_v1 h
join data_warehouse.claim_detail_v1 d on h.uth_claim_id=d.uth_claim_id
where h.uth_claim_id=15100057738;


select count(*)
from dev.claim_header_optum_fix

select uth_claim_id, count(*)
from quarantine.uth_claim_ids
group by 1
having count(*) > 1


select *
from quarantine.uth_claim_ids q
join data_warehouse.dim_uth_claim_id u on q.uth_claim_id=u.uth_claim_id
where q.uth_claim_id=7883221893;

create table dw_qa.claim_detail (like data_warehouse.claim_detail)
WITH (
	appendonly=true, orientation=column
);

insert into dw_qa.claim_detail
select *
from data_warehouse.claim_detail 
where data_source not like 'opt%';




