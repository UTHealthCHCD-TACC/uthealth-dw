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
update table set blah blah = 
where id = id and clmid = clmid;


