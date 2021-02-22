/*
 * Remove old records
 */
select count(*) from data_warehouse.claim_header where data_source in ('optd','optz');

delete from data_warehouse.claim_header where data_source in ('optd','optz');


vacuum analyze data_warehouse.claim_header;


vacuum analyze data_warehouse.claim_detail;
/
 * *
 * We assume the matching records exist in dim_uth_claim_id
 */
--Optum load: 
-- Full years = 8132 seconds = 2h15m

 select * from data_warehouse.claim_header where data_source = 'optd';

---create work tables distributed on the join value
---drop if exist
drop table if exists dev.wc_optd_medical;

drop table if exists dev.wc_optd_uth_claim;

select distinct year from optum_dip.medical;



---uth claims for optum only
create table dev.wc_optd_uth_claim
with(appendonly=true,orientation=column,compresstype=zlib)
as select * from data_warehouse.dim_uth_claim_id where data_source = 'optd'
distributed by (member_id_src);


---drop if exist
drop table if exists dev.wc_optd_medical;

create table dev.wc_optd_medical 
with(appendonly=true,orientation=column,compresstype=zlib)
as select * from optum_dod.medical where year between 2019 and 2020
distributed by (patid);

delete from data_warehouse.claim_header where data_source = 'optd' ;


--optd
insert into data_warehouse.claim_header(
		data_source, uth_member_id, member_id_src, uth_claim_id, claim_id_src, year,
		year_adj, claim_type,
	    from_date_of_service,
		total_charge_amount, total_allowed_amount, total_paid_amount,
		total_charge_amount_adj, total_allowed_amount_adj, total_paid_amount_adj, 
		data_year, admission_id_src 
		)
	select distinct on(uthc.uth_claim_id)
	'optd', uthc.uth_member_id, m.patid::text, uthc.uth_claim_id, m.clmid, extract(year from (min(m.fst_dt) over(partition by uthc.uth_claim_id))),
	m.std_cost_yr::int,
	cf.claim_type_code,
	min(m.fst_dt) over(partition by uthc.uth_claim_id) as from_date_of_service,
	sum(m.charge) over(partition by uthc.uth_claim_id) as total_charge_amount, 
	sum(m.std_cost) over(partition by uthc.uth_claim_id) as total_allowed_amount, 
	null as total_paid_amount,
	sum((m.charge * cf.cost_factor)) over(partition by uthc.uth_claim_id) as total_charge_amount, 
	sum((m.std_cost * cf.cost_factor)) over(partition by uthc.uth_claim_id) as total_allowed_amount_adj, 
	null as total_paid_amount_adj,
	m.year, m.conf_id 
from dev.wc_optd_medical m 
--from optum_dod.medical m
    join dev.wc_optd_uth_claim uthc 
	--join data_warehouse.dim_uth_claim_id uthc 
		on uthc.data_source = 'optd' 
		and m.patid::text = uthc.member_id_src 
		and m.clmid = uthc.claim_id_src
	join reference_tables.ref_optum_cost_factor cf 
	on cf.service_type = left(m.tos_cd, (position('.' in m.tos_cd)-1)) 
	and cf.standard_price_year = m.std_cost_yr::int
;


---create work tables distributed on the join value
---drop if exist
drop table if exists dev.wc_optd_medical;

drop table if exists dev.wc_optd_uth_claim;

select distinct year from optum_zip.medical;

---drop if exist
drop table if exists dev.wc_optz_medical;
create table dev.wc_optz_medical 
with(appendonly=true,orientation=column,compresstype=zlib)
as select * from optum_zip.medical where year between 2007 and 2010
distributed by (patid);


create table dev.wc_optz_uth_claim
with(appendonly=true,orientation=column,compresstype=zlib)
as select * from data_warehouse.dim_uth_claim_id where data_source = 'optz'
distributed by (member_id_src);


delete from data_warehouse.claim_header where data_source = 'optz' and data_year between 2007 and 2010;


---insert optz claim header
insert into data_warehouse.claim_header(
		data_source, uth_member_id, member_id_src, uth_claim_id, claim_id_src, year,
		year_adj, claim_type,
	    from_date_of_service,
		total_charge_amount, total_allowed_amount, total_paid_amount,
		total_charge_amount_adj, total_allowed_amount_adj, total_paid_amount_adj, 
		data_year, admission_id_src 
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
	m.year, m.conf_id 
from dev.wc_optz_medical m
--from optum_zip.medical m
    join dev.wc_optz_uth_claim uthc 
	--join data_warehouse.dim_uth_claim_id uthc 
		on m.patid::text = uthc.member_id_src 
		and m.clmid = uthc.claim_id_src
	join reference_tables.ref_optum_cost_factor cf 
	    on cf.service_type = left(m.tos_cd, (position('.' in m.tos_cd)-1)) 
	   and cf.standard_price_year = m.std_cost_yr::int
	--group by 1, 2, 3, 4, 5, m.std_cost_yr, cf.claim_type_code, m.year
;



select count(distinct m.clmid) as clmcnt, m."year" 
from optum_zip.medical m 
group by m."year" 
order by m."year" ;


select count(*) , data_year 
from data_warehouse.dim_uth_claim_id 
where data_source = 'optz'
group by data_year 
order by data_year ;

select count(*), count(distinct uth_claim_id), data_year 
from data_warehouse.claim_header 
where data_source = 'optd'
group by data_year 
order by data_year;


vacuum analyze data_warehouse.claim_header;







