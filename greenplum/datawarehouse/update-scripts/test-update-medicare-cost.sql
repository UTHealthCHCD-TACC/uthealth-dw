/*
*
* charge amount is supposed to be null 
* total_allowed_amount is supposed to be tot_rx_cst_amt
* use join conditions in script usually works 
*
1) make a copy of that part of the DW table in DEV 
----- check the mapping docs point 
2) test update copy
3) first just take limit 50000;
4) make a full dev table (just those data sources)
5) QA however makes sense 
6) whenever ur done let joe know look over it 
7) run it 
*/
--update table set blah blah = 
--where id = id and clmid = clmid;

drop table if exists dev.ip_medicare_rx_cost_update;

select *
  into dev.ip_medicare_rx_cost_update
  from data_warehouse.pharmacy_claims
 where data_source in ('mcrt', 'mcrn')
-- limit 100000000;
 
select 'total' as group, count(*) from dev.ip_medicare_rx_cost_update
union
select data_source, count(data_source) from dev.ip_medicare_rx_cost_update
group by 1;


-- updating allowed amounts and charged amounts
-- medicare texas 5m 36s
update dev.ip_medicare_rx_cost_update c
   set total_allowed_amount = a.tot_rx_cst_amt::numeric,
   		total_charge_amount = null
  from medicare_texas.pde_file a
  join data_warehouse.dim_uth_rx_claim_id b
    on b.data_source in ('mcrt')
   and a.bene_id = b.member_id_src 
   and a.pde_id = b.rx_claim_id_src
  where c.uth_rx_claim_id = b.uth_rx_claim_id;
 
-- medicare national 5m 24s
 update dev.ip_medicare_rx_cost_update c
   set total_allowed_amount = a.tot_rx_cst_amt::numeric,
   		total_charge_amount = null
  from medicare_national.pde_file a
  join data_warehouse.dim_uth_rx_claim_id b
    on b.data_source in ('mcrn')
   and a.bene_id = b.member_id_src 
   and a.pde_id = b.rx_claim_id_src
  where c.uth_rx_claim_id = b.uth_rx_claim_id;

-- QA counting claims that the costs were not updated. if updated correctly count should be 0/empty table
-- 5m
select b.data_source, count(b.data_source)--a.uth_rx_claim_id, a.total_charge_amount, a.total_allowed_amount, b.total_charge_amount, b.total_allowed_amount
from dev.ip_medicare_rx_cost_update a
join data_warehouse.pharmacy_claims b
on a.uth_rx_claim_id = b.uth_rx_claim_id
and (a.total_allowed_amount = b.total_allowed_amount 
 or a.total_charge_amount = b.total_charge_amount)
group by 1;