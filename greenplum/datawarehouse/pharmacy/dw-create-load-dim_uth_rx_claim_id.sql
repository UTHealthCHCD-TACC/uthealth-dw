drop table if exists data_warehouse.dim_uth_rx_claim_id; 



create table data_warehouse.dim_uth_rx_claim_id ( 
			uth_rx_claim_id bigserial,
			data_source char(4), 
			uth_member_id int8, 
			rx_claim_id_src text, 
			member_id_src text
) 
with (appendonly=true, orientation = column)
distributed by (uth_member_id);
;

alter sequence data_warehouse.dim_uth_rx_claim_id_uth_rx_claim_id_seq restart with 100000000;

alter sequence data_warehouse.dim_uth_rx_claim_id_uth_rx_claim_id_seq cache 200;


analyze data_warehouse.dim_uth_rx_claim_id;