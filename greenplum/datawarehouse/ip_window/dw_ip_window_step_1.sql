

-- create tables 
drop table if exists dev.gm_dw_ip_window_step_1;
create table dev.gm_dw_ip_window_step_1  (
data_source bpchar(4),
uth_member_id int8,
uth_claim_id int8,
admit_date date,
discharge_date date,
from_date_of_service date,
to_date_of_service date,
discharge_status varchar,
bill_type varchar,
insert_ts timestamp(0) DEFAULT LOCALTIMESTAMP)
with (appendonly=true,orientation=column) distributed by (uth_member_id) ;


drop table if exists dev.gm_dw_ip_window_step_2;
create table dev.gm_dw_ip_window_step_2  (
data_source bpchar(4),
uth_member_id int8,
uth_claim_id int8,
admit_date date,
discharge_date date,
discharge_status varchar,
bill_type varchar,
pat_group int,
insert_ts timestamp(0) DEFAULT LOCALTIMESTAMP
)
with (appendonly=true,orientation=column) distributed by (uth_member_id) ;

DROP TABLE if exists dev.gm_dw_ip_admit;
CREATE UNLOGGED TABLE dev.gm_dw_ip_admit (
	data_source bpchar(4),
	uth_member_id int8 NULL,
	enc_id int4 NULL,
	admit_date date NULL,
	discharge_date date NULL,
	enc_discharge_status varchar null,
	admit_id varchar null,
	insert_ts timestamp(0) DEFAULT LOCALTIMESTAMP
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zlib
)
DISTRIBUTED BY (uth_member_id);


drop table if exists dev.gm_dw_ip_admit_claim;
CREATE UNLOGGED TABLE dev.gm_dw_ip_admit_claim (
	data_source bpchar(4),
	admit_id varchar,
	uth_member_id int8 NULL,
	enc_id int4 NULL,
	admit_date date NULL,
	discharge_date date NULL,
	enc_discharge_status varchar null,
	uth_claim_id varchar,
	from_date_of_service date,
	to_date_of_service date,
	claim_type varchar null,
	insert_ts timestamp(0) DEFAULT LOCALTIMESTAMP
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zlib
)
DISTRIBUTED BY (admit_id);

-- below query inserted 9,947,107 values  when the null discharge status were removed
-- when nulls were not removed 15,639,863 values inserted
-- and the data from optum_dod.medical had 10,102,872

with optum as (select
	data_source,
	uth_member_id,
	uth_claim_id ,
	claim_sequence_number,
	from_date_of_service,
	to_date_of_service,
	admit_date,
	discharge_date,
	discharge_status,
	concat(bill_type_inst, bill_type_class, bill_type_freq) as bill_type
from
	data_warehouse.claim_detail ch
where
	(bill_type_inst = '1'
		and bill_type_class = '1'
		and (bill_type_freq = '1'
			or bill_type_freq = '4'))
--	and data_source in ('optd', 'optz')
	and year between 2015 and 2021)
insert into dev.gm_dw_ip_window_step_1 
select data_source, uth_member_id, uth_claim_id, min(from_date_of_service) as admit_date,
max(to_date_of_service) discharge_date, min(from_date_of_service) as from_date_of_service,
max(to_date_of_service) to_date_of_service, discharge_status, bill_type from optum
group by data_source, uth_member_id, uth_claim_id ,discharge_status ,bill_type;


delete from dev.gm_dw_ip_window_step_1 where to_date_of_service is null;

--11 minutes? 43,573,765
insert into dev.gm_dw_ip_window_step_2(
	data_source,
	uth_member_id,
	uth_claim_id ,
	admit_date,
	discharge_date,
	discharge_status,
	bill_type,
	pat_group)
with all_inp as (
select
	data_source,
	uth_member_id,
	uth_claim_id,
	min(admit_date) as admit_date,
	max(discharge_date) as discharge_date,
	discharge_status,
	bill_type,
	dense_rank() over (order by data_source, uth_member_id) / 1000000 pat_group
from
	dev.gm_dw_ip_window_step_1
group by
	data_source,
	uth_member_id,
	uth_claim_id,
	bill_type,
	discharge_status
)
select
	distinct ip.data_source,
	ip.uth_member_id,
	ip.uth_claim_id,
	ac.admit_date,
	ac.discharge_date,
	ac.discharge_status,
	ac.bill_type,
	ip.pat_group
from
	all_inp ip
join dev.gm_dw_ip_window_step_1 ac on
	ip.uth_member_id = ac.uth_member_id and ip.uth_claim_id = ac.uth_claim_id
where
	ac.admit_date between ip.admit_date and ip.discharge_date;
