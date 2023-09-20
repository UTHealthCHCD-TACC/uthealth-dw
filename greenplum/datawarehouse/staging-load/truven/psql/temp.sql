--timestamp a thing
--select 'Vacuum analyze pharm claims #1: ' || current_timestamp as message;

--vacuum analyze data_warehouse.dim_uth_rx_claim_id;

--timestamp a thing
select 'Update uth_member_ids: ' || current_timestamp as message;

update data_warehouse.dim_uth_rx_claim_id a
set uth_member_id = b.uth_member_id,
	data_source = b.data_source
from dw_staging.mcd_pharm_clms_subset_temp b
where a.data_source in ('mdcd', 'mhtw', 'mcpp') and
	a.uth_member_id is null and
	a.rx_claim_id_src = b.rx_claim_id_src;

--timestamp a thing
select 'Vacuum analyze dim uth rx claim id #2: ' || current_timestamp as message;
	
vacuum analyze data_warehouse.dim_uth_rx_claim_id;

--timestamp a thing
select 'Completed at: ' || current_timestamp as message;