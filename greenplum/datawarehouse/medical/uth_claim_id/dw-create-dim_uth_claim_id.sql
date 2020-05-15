--This table is used to generate a de-identified claim id that will be used to populate claim_detail and claim_header tables
--The uth_claim_id column will be a sequence that is initially set to a 100,000,000

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