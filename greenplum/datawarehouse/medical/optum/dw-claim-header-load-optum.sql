/*
 * Keep DDL for reference and to generate new temp load tables
 */
drop table dev.claim_header_optum;
CREATE TABLE dev.claim_header_optum_fix (
	data_source bpchar(4) NULL,
	uth_claim_id int8 NULL,
	claim_id_src text,
	uth_member_id int8 NULL,
	member_id_src text,
	admit_id_src text NULL,
	from_date_of_service date null,
	total_charge_amount numeric(13,2) NULL,
	total_allowed_amount numeric(13,2) NULL,
	total_paid_amount numeric(13,2) NULL,
	place_of_service text NULL
)
WITH (
	appendonly=true, orientation=column
)
DISTRIBUTED BY (uth_claim_id);


/*
 * We assume the matching records exist in dim_uth_claim_id
 */
--Optum load: 
insert into dev.claim_header_optum(data_source, uth_member_id, member_id_src, uth_claim_id, claim_id_src, 
admit_id_src, from_date_of_service, place_of_service,
total_charge_amount, total_allowed_amount, total_paid_amount)
select 'optz', uthc.uth_member_id, m.patid, uthc.uth_claim_id, m.clmid,
max(conf.conf_id) as conf_id,
min(m.fst_dt) as from_date_of_service, null as place_of_service,
sum(m.charge) as total_charge_amount, 
sum(m.std_cost) as total_allowed_amount, 
null as total_paid_amount 
from optum_zip.medical m
join data_warehouse.dim_uth_claim_id uthc on uthc.data_source='optz' and m.patid::text=uthc.member_id_src and m.clmid=uthc.claim_id_src
left join optum_zip.confinement conf on m.conf_id=conf.conf_id
left outer join quarantine.uth_claim_ids q on uthc.uth_claim_id=q.uth_claim_id
--left join optum_zip.ref_admit_type rat on m.admit_type::text=rat.key::text
--left join optum_zip.ref_admit_channel rac on m.admit_chan::text=rac.key::text and case when m.admit_chan='4' then rac.type_id=4 else rac.type_id is null end
where m.year >= 2015 and m.year <= 2017
and q.uth_claim_id is null
group by 1, 2, 3, 4, 5;

/*
 * Scratch Space
 */
insert into dev.claim_header_optum_fix
select *
from dev.claim_header_optum;
analyze dev.claim_header_optum;

select data_source, count(*), count(distinct uth_claim_id)
from dev.claim_header_optum
group by 1;

drop 


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




