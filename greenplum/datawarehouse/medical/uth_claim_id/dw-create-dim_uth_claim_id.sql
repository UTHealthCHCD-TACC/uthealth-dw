--This table is used to generate a de-identified claim id that will be used to populate claim_detail and claim_header tables
--The generated_value column will be a sequence that is initially set to a 

drop table if exists dw_qa.dim_uth_claim_id;

CREATE TABLE dw_qa.dim_uth_claim_id (
	generated_value bigserial NOT NULL,
	data_source bpchar(4) NULL,
	claim_id_src text NOT NULL,
	member_id_src text NOT NULL,
	data_year int4 NOT NULL,
	uth_claim_id int8 NULL,
	uth_member_id int8 NULL
)
WITH (appendonly=true, orientation=column)
DISTRIBUTED BY (generated_value);

alter sequence dw_qa.dim_uth_claim_id_generated_value_seq restart with 100000000;

alter sequence dw_qa.dim_uth_claim_id_generated_value_seq cache 200;


