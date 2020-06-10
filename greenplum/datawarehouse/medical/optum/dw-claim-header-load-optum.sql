/*
 * Remove old records
 */
delete from dw_qa.claim_header where data_source like 'opt%';

/*
 * We assume the matching records exist in dim_uth_claim_id
 */
--Optum load: 
-- Full years = 8132 seconds = 2h15m
insert into data_warehouse.claim_header(data_source, uth_member_id, member_id_src, uth_claim_id, claim_id_src, year,
admission_id_src, from_date_of_service, place_of_service,
total_charge_amount, total_allowed_amount, total_paid_amount)
select 'optd', uthc.uth_member_id, m.patid, uthc.uth_claim_id, m.clmid, uthc.data_year,
max(conf.conf_id) as conf_id,
min(m.fst_dt) as from_date_of_service, null as place_of_service,
sum(m.charge) as total_charge_amount, 
sum(m.std_cost) as total_allowed_amount, 
null as total_paid_amount 
from optum_dod.medical m
join data_warehouse.dim_uth_claim_id uthc on uthc.data_source='optd' and m.patid::text=uthc.member_id_src and m.clmid=uthc.claim_id_src
left join optum_dod.confinement conf on m.conf_id=conf.conf_id
left outer join quarantine.uth_claim_ids q on uthc.uth_claim_id=q.uth_claim_id
--left join optum_zip.ref_admit_type rat on m.admit_type::text=rat.key::text
--left join optum_zip.ref_admit_channel rac on m.admit_chan::text=rac.key::text and case when m.admit_chan='4' then rac.type_id=4 else rac.type_id is null end
where q.uth_claim_id is null
group by 1, 2, 3, 4, 5, 6;

?*

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




