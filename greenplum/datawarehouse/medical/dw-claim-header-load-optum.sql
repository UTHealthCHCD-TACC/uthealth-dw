drop table dev.claim_header_optum;
CREATE TABLE dev.claim_header_optum (
	data_source bpchar(4) NULL,
	uth_claim_id numeric NULL,
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

--Optum load: 
insert into data_warehouse.claim_header(data_source, member_id_src, claim_id_src, 
admit_id_src,
total_charge_amount, total_allowed_amount, total_paid_amount)
select 'optd', m.patid, m.clmid,
max(conf.conf_id) as conf_id, 
sum(m.charge) as total_charge_amount, 
sum(m.charge) as total_allowed_amount, 
sum(m.copay + m.coins) as total_paid_amount--, 
--count(distinct conf.conf_id) as conf_cnt, 
--count(*) as record_cnt
from optum_dod_medical m
left join optum_dod_confinement conf on m.conf_id=conf.conf_id
left join optum_dod.ref_admit_type rat on m.admit_type::varchar=rat.key::varchar
left join optum_dod.ref_admit_channel rac on m.admit_chan::varchar=rac.key::varchar and case when m.admit_chan='4' then rac.type_id=4 else rac.type_id is null end
--where clmid='187810755'
group by 1, 2, 3;

select count(*)
from dev.claim_header_optum;