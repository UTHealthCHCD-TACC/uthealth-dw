
--Main table
drop table data_warehouse.claim_header;
create table data_warehouse.claim_header (
id bigserial NOT NULL,
	"source" bpchar(4),
	member_id_src varchar,
	uth_member_id varchar, 
	claim_id_src varchar,
	uth_claim_id varchar,
	claim_type varchar,
	in_network boolean,
	admit_id_src varchar,
	admit_date date,
	discharge_date date,
	discharge_status_src varchar,
	admit_type_src varchar,
	admit_channel_src varchar,
	total_cost numeric,
	total_paid numeric
) 
WITH (appendonly=true, orientation=column)
distributed randomly;

alter table data_warehouse.claim_header alter column member_id_src type varchar;

--Remove '.0' from id_src columns.  Ex. claim_id_src=3452345.0 -> 3452345
--The trailing .0 make joins to Truven raw data difficult
select cast(trunc(cast('12354.0' as numeric), 0) as varchar)
update data_warehouse.claim_header set member_id_src=cast(trunc(cast(member_id_src as numeric), 0) as varchar)
update data_warehouse.claim_header set claim_id_src=cast(trunc(cast(claim_id_src as numeric), 0) as varchar)



--Greenplum performance optimization for serial/sequence
alter sequence data_warehouse.claim_header_id_seq cache 100;

--Optum load: 
insert into data_warehouse.claim_header(source, member_id_src, claim_id_src, 
admit_id_src, admit_date, admit_type_src, admit_channel_src, discharge_date,
total_cost, total_paid)
select 'OPTD', m.patid, m.clmid,
max(conf.conf_id) as conf_id, 
min(conf.admit_date) as admit_date, 
max(rat.value) as admit_type_src,
max(rac.value_derived) as admit_channel_src,
min(conf.disch_date) as disch_date,
sum(m.charge) as total_cost, 
sum(m.copay + m.coins) as total_paid--, 
--count(distinct conf.conf_id) as conf_cnt, 
--count(*) as record_cnt
from optum_zip_medical m
left join optum_zip_confinement conf on m.conf_id=conf.conf_id
left join optum_zip.ref_admit_type rat on m.admit_type::varchar=rat.key::varchar
left join optum_zip.ref_admit_channel rac on m.admit_chan::varchar=rac.key::varchar and case when m.admit_chan='4' then rac.type_id=4 else rac.type_id is null end
--where clmid='187810755'
group by 1, 2, 3;

select count(*)
from data_warehouse.claim_header;


-- Diagnostics


/*
 * Truven 'medical' data is split between inpatient and outpatient data tables (ex. ccaei and ccaeo). 
 */
--Truven load Inpatient
insert into data_warehouse.claim_header(source, member_id_src, claim_id_src, 
admit_id_src, admit_date, admit_type_src, discharge_date, discharge_status_src, 
total_cost, total_paid)

select 'truc', s.enrolid, trunc(s.msclmid, 0),
max(i.caseid) as admit_id_src, 
max(i.admdate) as admit_date, 
max(atyp.value) as admit_type_src, 
max(i.disdate) as discharge_date,
max(ds.value) as discharge_status_src,
max(i.totpay) as total_cost,
max(i.totnet) as total_paid--,
--count(*) as record_cnt
from truven_ccaes s
left join truven.ccaei i on s.caseid=i.caseid and s.enrolid=i.enrolid
left join truven.ref_admit_type atyp on i.admtyp=atyp.key
left join truven.ref_discharge_status ds on i.dstatus=ds."key"
--where s.enrolid=14516012 and s.msclmid=266334
group by 1, 2, 3;


--Truven load Outpatient (Skipping for now)
insert into data_warehouse.claim_header(source, member_id_src, claim_id_src, 
total_cost, total_paid)

select 'truc', o.enrolid, trunc(o.msclmid, 0),
sum(o.pay) as total_cost,
sum(o.netpay) as total_paid
--count(*) as record_cnt
from truven_ccaeo o
--where o.enrolid=602902 and o.msclmid=1466020
group by 1, 2, 3;

--Verify
select source, count(*)
from data_warehouse.claim_header
group by 1;
