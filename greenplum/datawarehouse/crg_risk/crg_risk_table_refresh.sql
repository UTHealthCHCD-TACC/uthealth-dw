/* 
Final steps to refresh crg_risk table for a data source.

Once the CRG scores are generated and are uploaded to a seperate table in Greenplum, 
we can start the process of adding the new CRG scores to the data warehouse table.
*/


-- Remove old CRG risk scores
delete from data_warehouse.crg_risk where data_source = '';

-- Insert new CRG risk scores
-- Note which table the new scores are stored in
insert into data_warehouse.crg_risk
(data_source, uth_member_id, crg_year, crg, aggregated_crg_3, prospective_crg, prospective_agg_crg_1, prospective_agg_crg_2, prospective_agg_crg_3,
concurrent_crg, concurrent_agg_crg_1, concurrent_agg_crg_2, concurrent_agg_crg_3, load_date)
select *
from dev.ip_truc_crg_risk
--where crg_year = 2021
;

-- Add source member ids
update data_warehouse.crg_risk a
set member_id_src = b.member_id_src
from data_warehouse.dim_uth_member_id b
where a.data_source = b.data_source
and a.uth_member_id = b.uth_member_id
and a.member_id_src is null;

vacuum analyze data_warehouse.crg_risk;

update data_warehouse.update_log
set data_last_updated = current_date,
	last_vacuum_analyze = current_date,
	details = 'Added truc CRG risk scores'
where table_name = 'crg_risk' and schema_name = 'data_warehouse';