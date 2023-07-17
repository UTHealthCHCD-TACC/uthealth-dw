/**************************************
 * This script backs up just the Truven portion of the dim_uth tables. It does the following things:
 * 
 * 1) Copies dim_uth_member_id to backup (just Truven portion) 
 * 2) Copies dim_uth_claim_id to backup (just Truven portion)
 * 3) Copies dim_uth_rx_id to backup (just Truven portion)
 * 4) Grants uthealth_analyst access to these tables
 */

--timestamp
select 'Truven dim_uth tables backup started at ' || current_timestamp as message;

drop table if exists backup.dim_uth_member_id_truven;
drop table if exists backup.dim_uth_claim_id_truven;
drop table if exists backup.dim_uth_rx_claim_id_truven;

create table backup.dim_uth_member_id_truven as
select * from data_warehouse.dim_uth_member_id
where data_source = 'truv';

create table backup.dim_uth_claim_id_truven as
select * from data_warehouse.dim_uth_claim_id
where data_source = 'truv';

create table backup.dim_uth_rx_claim_id_truven as
select * from data_warehouse.dim_uth_rx_claim_id
where data_source = 'truv';

--timestamp
select 'Truven dim_uth tables backup completed at ' || current_timestamp as message;

--grant access to uthealth_analyst
grant select on backup.dim_uth_member_id_truven to uthealth_analyst;
grant select on backup.dim_uth_claim_id_truven to uthealth_analyst;
grant select on backup.dim_uth_rx_claim_id_truven to uthealth_analyst;









