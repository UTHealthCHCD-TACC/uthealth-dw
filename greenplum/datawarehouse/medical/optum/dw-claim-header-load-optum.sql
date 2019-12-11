/*
 * Keep DDL for reference and to generate new temp load tables
 */
drop table dev.claim_header_optum;
CREATE TABLE dev.claim_header_optum (
	data_source bpchar(4) NULL,
	uth_claim_id int8 NULL,
	claim_id_src text,
	uth_member_id int8 NULL,
	member_id_src text,
	admit_id_src text NULL,
	total_charge_amount numeric(13,2) NULL,
	total_allowed_amount numeric(13,2) NULL,
	total_paid_amount numeric(13,2) NULL,
	place_of_service text NULL
)
WITH (
	appendonly=true, orientation=column
)
DISTRIBUTED RANDOMLY;



/*
 * First need to deal with loading data_warehouse.dim_uth_claim_id with any new claim numbers for optum
 * We assume all patids already exist in dim_uth_member_id table.
 */

--Use a dev version of the uth id tables, must create an manually set sequence to keep 'in sync'.
drop table dev.dim_uth_claim_id_optum; 
create table dev.dim_uth_claim_id_optum 
WITH (
	appendonly=true, orientation=column
)
as select * from data_warehouse.dim_uth_claim_id;
CREATE SEQUENCE dev.dim_uth_claim_id_optum_generated_value_seq;
ALTER SEQUENCE dev.dim_uth_claim_id_optum_generated_value_seq OWNED BY dev.dim_uth_claim_id_optum.generated_value;
SELECT setval('dev.dim_uth_claim_id_optum_generated_value_seq', (SELECT max(generated_value) FROM dev.dim_uth_claim_id_optum), true);
ALTER TABLE dev.dim_uth_claim_id_optum ALTER generated_value SET DEFAULT nextval('dev.dim_uth_claim_id_optum_generated_value_seq'::regclass)
alter sequence dev.dim_uth_claim_id_optum_generated_value_seq cache 100;

drop table dev.dim_uth_member_id_optum;
create table dev.dim_uth_member_id_optum 
WITH (
	appendonly=true, orientation=column
)
as select * from data_warehouse.dim_uth_member_id;
CREATE SEQUENCE dev.dim_uth_member_id_optum_generated_serial_seq;
ALTER SEQUENCE dev.dim_uth_member_id_optum_generated_serial_seq OWNED BY dev.dim_uth_member_id_optum.generated_serial;
SELECT setval('dev.dim_uth_member_id_optum_generated_serial_seq', (SELECT max(generated_serial) FROM dev.dim_uth_member_id_optum), true);
ALTER TABLE dev.dim_uth_member_id_optum ALTER generated_serial SET DEFAULT nextval('dev.dim_uth_member_id_optum_generated_serial_seq'::regclass)
alter sequence dev.dim_uth_member_id_optum_generated_serial_seq cache 100;

--Now insert new/missing claim_ids
insert into dev.dim_uth_claim_id_optum (data_source, claim_id_src, member_id_src, data_year, uth_member_id)                                              
select distinct  'optd', a.clmid::text, a.patid::text, trunc(a.year,0), b.uth_member_id                                              
from optum_dod_medical a
  join dev.dim_uth_member_id_optum b 
    on b.data_source = 'optd'
   and b.member_id_src = a.patid::text 
  left join dev.dim_uth_claim_id_optum c
                                            on  b.data_source = c.data_source
                                              and a.clmid::text = c.claim_id_src 
                                              and a.patid::text = c.member_id_src
                                              and trunc(a.year,0) = c.data_year 
  where a.patid is not null
  and c.generated_value is null;

--Set uth_claim_id
update dev.dim_uth_claim_id_optum
set uth_claim_id =  ( substring(data_year::text,3,2) || generated_value::text  )::bigint
where uth_claim_id is null;

/*
 * Now we can actually load the data
 */
--Optum load: 
insert into dev.claim_header_optum(data_source, uth_member_id, member_id_src, uth_claim_id, claim_id_src, 
admit_id_src,
total_charge_amount, total_allowed_amount, total_paid_amount)
select 'optd', uthc.uth_member_id, m.patid, uthc.uth_claim_id, m.clmid,
max(conf.conf_id) as conf_id,
sum(0) as total_charge_amount, 
sum(0) as total_allowed_amount, 
sum(0) as total_paid_amount--, 
--count(distinct conf.conf_id) as conf_cnt, 
--count(*) as record_cnt
from optum_dod_medical m
join dev.dim_uth_claim_id_optum uthc on m.patid::text=uthc.member_id_src and m.clmid=uthc.claim_id_src and uthc.data_source='optd'
left join optum_dod_confinement conf on m.conf_id=conf.conf_id
--left join optum_dod.ref_admit_type rat on m.admit_type::text=rat.key::text
--left join optum_dod.ref_admit_channel rac on m.admit_chan::text=rac.key::text and case when m.admit_chan='4' then rac.type_id=4 else rac.type_id is null end
--where clmid='187810755'
group by 1, 2, 3, 4, 5;

/*
 * Scratch Space
 */
select data_source, count(*)
from dev.claim_header_optum
group by 1;


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




