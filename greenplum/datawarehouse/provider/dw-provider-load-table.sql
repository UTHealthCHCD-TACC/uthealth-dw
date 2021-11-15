/* ******************************************************************************************************
 *  create provider table in DW
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 * ******************************************************************************************************
 *  jw001  || 11/03/2021 || create script 
 * ***************************************************************************************************
 * */

--create provider
CREATE TABLE data_warehouse.provider (
	uth_provider_id bigserial NOT NULL,	
	data_source bpchar(4) NULL,
	provider_id_src text NOT null,
	provider_id_src_2 text null,
	npi varchar NULL,
	taxonomy1 varchar NULL,
	taxonomy2 varchar NULL,
	spclty_cd1 varchar NULL,
	spclty_cd2 varchar NULL,
	provcat varchar null,
	provider_type varchar NULL,
	address1 varchar NULL,
	address2 varchar NULL,
	address3 varchar NULL,
	city varchar NULL,
	state varchar NULL,
	zip varchar NULL,
	zip_5 varchar NULL
)
WITH (appendonly=true, orientation=column)
DISTRIBUTED BY (uth_provider_id);

--- do not run 
/*
drop table if exists data_warehouse.provider;
*/