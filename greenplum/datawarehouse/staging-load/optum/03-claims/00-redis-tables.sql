---uth claims for optd only distributed by member id
drop table if exists dw_staging.optd_uth_claim_id;

create table dw_staging.optd_uth_claim_id
with(appendonly=true,orientation=column)
as select * from data_warehouse.dim_uth_claim_id where data_source = 'optd'
distributed by (member_id_src);

analyze dw_staging.optd_uth_claim_id;

---uth claims for optd only distributed by member id
drop table if exists dw_staging.optz_uth_claim_id;

create table dw_staging.optz_uth_claim_id
with(appendonly=true,orientation=column)
as select * from data_warehouse.dim_uth_claim_id where data_source = 'optz'
distributed by (member_id_src);

analyze dw_staging.optz_uth_claim_id;