--Scratch
analyze dev.claim_header;

select data_source, count(*), count(distinct uth_claim_id)
from dev.claim_header
group by 1;

create table dev.claim_header (like data_warehouse.claim_header)
WITH (appendonly=true, orientation=column)
distributed by (uth_member_id);

truncate table dev.claim_header;

--Inpatient
insert into dev.claim_header (data_source, uth_claim_id, uth_member_id, claim_id_src, member_id_src, table_id_src,
claim_type, place_of_service, admission_id_src,
year, from_date_of_service,
total_charge_amount, total_allowed_amount, total_paid_amount) 
select 'trvc', b.uth_claim_id, b.uth_member_id, a.msclmid, a.enrolid, 'ccaes', 
min(a.facprof), min(trunc(stdplac,0)::text), min(a.caseid),
min(extract(year from a.svcdate)), min(a.svcdate),
sum(null::int), sum(a.pay), sum(a.netpay)
from truven.ccaes a
join data_warehouse.dim_uth_claim_id b 
on b.data_source ='trvc'
and b.claim_id_src = a.msclmid::text
and b.member_id_src = a.enrolid::text
group by 1, 2, 3, 4, 5, 6;

--Outpatient
insert into dev.claim_header (data_source, uth_claim_id, uth_member_id, claim_id_src, member_id_src, table_id_src, admission_id_src,
claim_type, place_of_service,
year, from_date_of_service,
total_charge_amount, total_allowed_amount, total_paid_amount) 
select 'trvm', b.uth_claim_id, b.uth_member_id, a.msclmid, a.enrolid, 'mdcro', null,
min(a.facprof), min(trunc(stdplac,0)::text),
min(extract(year from a.svcdate)), min(a.svcdate),
sum(null::int), sum(a.pay), sum(a.netpay)
from truven.mdcro a
join data_warehouse.dim_uth_claim_id b 
on b.data_source ='trvm'
and b.claim_id_src = a.msclmid::text
and b.member_id_src = a.enrolid::text
group by 1, 2, 3, 4, 5, 6, 7;



