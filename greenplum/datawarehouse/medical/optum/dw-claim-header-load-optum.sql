/*
 * Keep DDL for reference and to generate new temp load tables
 */
drop table dev.claim_header_by_claim;
CREATE TABLE dev.claim_header_by_claim (
	data_source bpchar(4) NULL,
	uth_claim_id numeric NULL,
	uth_member_id int8 NULL,
	from_date_of_service date NULL,
	claim_type text NULL,
	place_of_service bpchar(2) NULL,
	uth_admission_id numeric NULL,
	admission_id_src text NULL,
	total_charge_amount numeric(13,2) NULL,
	total_allowed_amount numeric(13,2) NULL,
	total_paid_amount numeric(13,2) NULL,
	claim_id_src text NULL,
	member_id_src text NULL,
	table_id_src text NULL
)
WITH (
	appendonly=true, orientation=column
)
DISTRIBUTED BY (uth_claim_id);

/*
 * Remove old records
 */
delete from dw_qa.claim_header where data_source like 'opt%';

/*
 * We assume the matching records exist in dim_uth_claim_id
 */
--Optum load: 
insert into dw_qa.claim_header(data_source, uth_member_id, member_id_src, uth_claim_id, claim_id_src, 
admission_id_src, from_date_of_service, place_of_service,
total_charge_amount, total_allowed_amount, total_paid_amount)
select 'optd', uthc.uth_member_id, m.patid, uthc.uth_claim_id, m.clmid,
max(conf.conf_id) as conf_id,
min(m.fst_dt) as from_date_of_service, null as place_of_service,
sum(m.charge) as total_charge_amount, 
sum(m.std_cost) as total_allowed_amount, 
null as total_paid_amount 

explain
select count(*)
from optum_dod_refresh.medical m
join data_warehouse.dim_uth_claim_id uthc on uthc.data_source='optd' and m.patid::text=uthc.member_id_src and m.clmid=uthc.claim_id_src
left join optum_dod_refresh.confinement conf on m.conf_id=conf.conf_id
left outer join quarantine.uth_claim_ids q on uthc.uth_claim_id=q.uth_claim_id
--left join optum_zip.ref_admit_type rat on m.admit_type::text=rat.key::text
--left join optum_zip.ref_admit_channel rac on m.admit_chan::text=rac.key::text and case when m.admit_chan='4' then rac.type_id=4 else rac.type_id is null end
where m.year >= 2015 and m.year <= 2017
and q.uth_claim_id is null
group by 1, 2, 3, 4, 5;

SELECT *
FROM   pg_settings
WHERE  name like '%mem%';

SET work_mem = '4024MB';
SET statement_mem = '4024MB';



/*
 * Scratch Space
 */

select year, count(*)
from optum_zip_refresh.medical m2
group by 1;

select *
from optum_zip_refresh.medical m
join data_warehouse.dim_uth_claim_id uthc on uthc.data_source='optz' and m.patid::text=uthc.member_id_src and m.clmid=uthc.claim_id_src
limit 10;

vacuum full dw_qa.claim_header;
analyze dw_qa.claim_header;

select data_source, count(*), count(distinct uth_claim_id)
from dw_qa.claim_header
group by 1;

insert into dw_qa.claim_header(data_source, uth_member_id, member_id_src, uth_claim_id, claim_id_src, 
admission_id_src, from_date_of_service, place_of_service,
total_charge_amount, total_allowed_amount, total_paid_amount)
select data_source, uth_member_id, member_id_src, uth_claim_id, claim_id_src, 
admission_id_src, from_date_of_service, place_of_service,
total_charge_amount, total_allowed_amount, total_paid_amount
from dev.claim_header_optum;


select data_source, count(*)
from  dev.dim_uth_claim_id_optum
group by 1;


select * 
from dev.dim_uth_claim_id_optum
where data_source='optd'
and claim_id_src='4329417402';

select * 
from dev.dim_uth_claim_id_optum
where generated_value is null
limit 10;


select data_source, count(*)
from data_warehouse.claim_header_v1
group by 1;

select data_source, count(*)
from data_warehouse.claim_detail_v1
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




