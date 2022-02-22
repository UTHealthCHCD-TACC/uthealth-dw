

/*
-------------------------------
------- DW UPDATE
------- 	update medicaid for claim_detail.table_id_src
-------------------------------
------- jw001 2/22/2022 
------- insert 'clm' and 'enc' from medicaid.[] tables 
-------------------------------
*/


/*
--------------------------------------------------------------
--DEV TEST 
--------------------------------------------------------------

--- make a copy of claim_detail in dev to test update code
drop table if exists dev.update_medicaid_tablesrc ;

create table dev.update_medicaid_tablesrc as
(
	select * 
	from data_warehouse.claim_detail 
	where data_source = 'mdcd'
	limit 30000000
) distributed by (uth_member_id) ;

vacuum analyze dev.update_medicaid_tablesrc ;

-------------------------------------------
--- update table_id_src from source table using claim_id_src 

update dev.update_medicaid_tablesrc z
   set table_id_src = 'clm'
  from data_warehouse.dim_uth_claim_id c
		  where z.data_source = 'mdcd'
		    and c.data_source = 'mdcd'
		   	and c.uth_claim_id = z.uth_claim_id
		  	and c.claim_id_src in
		  		(select icn from  medicaid.clm_detail);
/*
null 19086182
clm	 10913818
*/
-------------------------------------------------
--- update table_id_src from source table using claim_id_src 

  update dev.update_medicaid_tablesrc z
   set table_id_src = 'enc'
  from data_warehouse.dim_uth_claim_id c
		 where z.data_source = 'mdcd'
		   and c.data_source = 'mdcd'
		   and c.uth_claim_id = z.uth_claim_id
		   and c.claim_id_src in
		  		(select derv_enc from medicaid.enc_det);

select table_id_src, count(*) from dev.update_medicaid_tablesrc group by table_id_src;

/*
clm	10913818
enc	19086182
*/

*/


/*
--------------------------------------------------------------
UPDATE DATA WAREHOUSE
--------------------------------------------------------------

update data_warehouse.claim_detail z
   set table_id_src = 'clm'
  from data_warehouse.dim_uth_claim_id c
		  where z.data_source = 'mdcd'
		    and c.data_source = 'mdcd'
		   	and c.uth_claim_id = z.uth_claim_id
		  	and c.claim_id_src in
		  		(select icn from  medicaid.clm_detail);

-------------------------------------------------

	
  update data_warehouse.claim_detail z
   set table_id_src = 'enc'
  from data_warehouse.dim_uth_claim_id c
		 where z.data_source = 'mdcd'
		 	 and c.data_source = 'mdcd'
		   and c.uth_claim_id = z.uth_claim_id
		   and c.claim_id_src in
		  		(select derv_enc from medicaid.enc_det);
*/