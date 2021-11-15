

---uth member id
drop table if exists data_warehouse.dim_uth_member_id;

create table data_warehouse.dim_uth_member_id (
	uth_member_id bigserial,
	member_id_src text, 
	data_source char(4), 
	unique ( uth_member_id)
) distributed by (uth_member_id)
;

alter sequence data_warehouse.dim_uth_member_id_uth_member_id_seq restart with 100000000; 
                                                                           
alter sequence data_warehouse.dim_uth_member_id_uth_member_id_seq cache 200;

analyze data_warehouse.dim_uth_member_id;

alter table data_warehouse.dim_uth_member_id owner to uthealth_dev;


---uth claim id 
drop table if exists data_warehouse.dim_uth_claim_id;

CREATE TABLE data_warehouse.dim_uth_claim_id (
	uth_claim_id bigserial NOT NULL,
	uth_member_id int8 null,
	data_source bpchar(4) NULL,
	claim_id_src text NOT NULL,
	member_id_src text NOT NULL,
	data_year int4 NOT NULL
)
WITH (appendonly=true, orientation=column)
DISTRIBUTED BY (uth_member_id);

alter sequence data_warehouse.dim_uth_claim_id_uth_claim_id_seq restart with 100000000;

alter sequence data_warehouse.dim_uth_claim_id_uth_claim_id_seq cache 200;

analyze data_warehouse.dim_uth_claim_id;

alter table data_warehouse.dim_uth_claim_id owner to uthealth_dev;

---uth admission id 
drop table if exists data_warehouse.dim_uth_admission_id;

create table data_warehouse.dim_uth_admission_id (
	data_source char(4),
	year int2,
	uth_admission_id bigserial,
	uth_member_id bigint,
	admission_id_src text,
	member_id_src text
) with (appendonly=true, orientation=column)
distributed by (uth_member_id)
;

alter sequence data_warehouse.dim_uth_admission_id_uth_admission_id_seq restart with 100000000;

alter sequence data_warehouse.dim_uth_admission_id_uth_admission_id_seq cache 200;

analyze data_warehouse.dim_uth_admission_id;

alter table data_warehouse.dim_uth_admission_id owner to uthealth_dev;


---uth rx claim id 
drop table if exists data_warehouse.dim_uth_rx_claim_id;

create table data_warehouse.dim_uth_rx_claim_id ( 
			data_source char(4),	
			year int2,
			uth_rx_claim_id bigserial,
			rx_claim_id_src text, 
			uth_member_id int8, 			
			member_id_src text
) 
with (appendonly=true, orientation = column)
distributed by (uth_member_id);
;

alter sequence data_warehouse.dim_uth_rx_claim_id_uth_rx_claim_id_seq restart with 100000000;

alter sequence data_warehouse.dim_uth_rx_claim_id_uth_rx_claim_id_seq cache 200;

analyze data_warehouse.dim_uth_rx_claim_id;

alter table data_warehouse.dim_uth_rx_claim_id owner to uthealth_dev;

