/*
 * updating allowed amounts and charged amounts for medicare rx claims
*/

-- medicare texas
update data_warehouse.pharmacy_claims c
   set total_allowed_amount = a.tot_rx_cst_amt::numeric,
   		total_charge_amount = null
  from medicare_texas.pde_file a
  join data_warehouse.dim_uth_rx_claim_id b
    on b.data_source in ('mcrt')
   and a.bene_id = b.member_id_src 
   and a.pde_id = b.rx_claim_id_src
  where c.uth_rx_claim_id = b.uth_rx_claim_id;
 
-- medicare national
 update data_warehouse.pharmacy_claims c
   set total_allowed_amount = a.tot_rx_cst_amt::numeric,
   		total_charge_amount = null
  from medicare_national.pde_file a
  join data_warehouse.dim_uth_rx_claim_id b
    on b.data_source in ('mcrn')
   and a.bene_id = b.member_id_src 
   and a.pde_id = b.rx_claim_id_src
  where c.uth_rx_claim_id = b.uth_rx_claim_id;