/********************************
 * This script populates the dim_uth_rx_claim_id table with new uth_rx_claim_ids
 * if they do not exist for a particular claim
 * 
 * Author 	|| Date		|| Comment
 * ----------------------------------------------------
 * Various	  < 08/2023		Script created
 * ----------------------------------------------------
 * Xiaorui	|| 08/07/23	|| Modified to accomodate mhtw and mcpp
 * 						   Logic: Instead of trying to figure out what data source everyone is in,
 * 						   first assign everyone to data_source mdcd. After enrollment tables
 * 						   are done, then assign the data_source based on service date and member enrollment
 */

select 'Medicaid dim_uth_rx_claim_id script begin' as message;

/******************
 * Question: Are there duplicates?
 * 
select member_id_src, claim_id_src, count(*) from data_warehouse.dim_uth_rx_claim_id
where data_source in ('mdcd', 'mcpp', 'mhtw')
group by 1, 2
having count(*) > 1;

Answer: no, apparently? Interesting.
 */

/******************************
 * Grab distinct rx_claim_id/member_ids from chip_rx, ffs_rx, mco_rx, and htw_ffs_rx
 * 
 * Note: rx_fill_dt is pretty clean, no 1900-01-01s in it
 * 
select rx_fill_dt from medicaid.chip_rx
where extract (year from rx_fill_dt::date) not between 2011 and 2022;
 * 
 */
select 'Creating temp table: ' || current_timestamp as message;

--claim ids from clm_header
drop table if exists dw_staging.dim_rx_load_mdcd;

create table dw_staging.dim_rx_load_mdcd as 
select distinct * from (
select extract (year from a.rx_fill_dt::date) as cy,
       pcn || ndc || replace(rx_fill_dt::text, '-','') as rx_claim_id_src,
       a.pcn::text as member_id_src
  from medicaid.chip_rx a
  	where pcn is not null and ndc is not null and rx_fill_dt is not null
union all
select extract (year from a.rx_fill_dt::date) as cy,
       pcn || ndc || replace(rx_fill_dt::text, '-','') as rx_claim_id_src,
       a.pcn::text as member_id_src
  from medicaid.mco_rx a
  	where pcn is not null and ndc is not null and rx_fill_dt is not null
union all
select extract (year from a.rx_fill_dt::date) as cy,
       pcn || ndc || replace(rx_fill_dt::text, '-','') as rx_claim_id_src,
       a.pcn::text as member_id_src
  from medicaid.ffs_rx a
union all
select extract (year from a.rx_fill_dt::date) as cy,
       pcn || ndc || replace(rx_fill_dt::text, '-','') as rx_claim_id_src,
       a.pcn::text as member_id_src
  from medicaid.htw_ffs_rx a
  	where pcn is not null and ndc is not null and rx_fill_dt is not null
  ) a
  distributed by (rx_claim_id_src)
;

/****************************
 * Insert into dim_uth_rx_claim_id table
 */

select 'Inserting new records into dim_uth_rx_claim_id: ' || current_timestamp as message;

insert into data_warehouse.dim_uth_rx_claim_id (
			 data_source
			,year 
			,rx_claim_id_src
			,member_id_src )
select 'mdcd', 
       a.cy ,
       a.rx_claim_id_src ,
       a.member_id_src
  from dw_staging.dim_rx_load_mdcd a
  left outer join dev.dim_uth_rx_claim_id c 
    on c.member_id_src = a.member_id_src 
   and c.rx_claim_id_src = a.rx_claim_id_src 
   and c.data_source in ('mdcd', 'mhtw', 'mcpp')
 where c.uth_rx_claim_id is null 
;

/******************
 * Clean-up
 */
select 'Vacuum and cleanup: ' || current_timestamp as message;
vacuum analyze data_warehouse.dim_uth_rx_claim_id;

drop table if exists dw_staging.dim_rx_load_mdcd;

select 'Medicaid dim_uth_rx_claim_id script completed at: ' || current_timestamp as message;




