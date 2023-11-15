 /* ******************************************************************************************************
 * Author || Date      || Notes
 * ******************************************************************************************************
 * various authors  || <10/25/2022 || created
 * ******************************************************************************************************
 * jwozny 			|| 10/25/2022  || Several claims have dates of service outside of the valid date ranges.
										The corresponding claim in claim_header often has the correct date of service
										So we update claim detail from claim header and re-derive year
 * ******************************************************************************************************
 * xzhang  			|| 09/05/2023  || Changed table name from claim_detail to mcd_claim_detail
 * ******************************************************************************************************
 * xzhang			|| 10/20/2023  || Actually, all dates should be set to the from_dos from claim_header because
 * 									  there are typos in claim_details (e.g. 2022-10-15 becomes 2023-10-15)
 * 									  Code is for live tables so on next run, change to dw_staging tables
*/

 /****************************
  * New code that needs to be modified for next run here
  */
 --backup table
 create table backup.mcd_claim_detail as
 select * from data_warehouse.claim_detail
 where data_source in ('mdcd', 'mcpp', 'mhtw');
 
 --1 clean up from date of service from matching claim in claim_header 
update data_warehouse.claim_detail a
   set from_date_of_service = b.from_date_of_service,
   	   to_date_of_service = b.to_date_of_service,
   	   "year" = b."year",
   	   fiscal_year = b.fiscal_year
  from data_warehouse.claim_header b 
 where a.data_source in ('mdcd', 'mcpp', 'mhtw')
 	and b.data_source in ('mdcd', 'mcpp', 'mhtw')
 	and a.uth_member_id = b.uth_member_id 
    and a.uth_claim_id = b.uth_claim_id 
    and (a.from_date_of_service != b.from_date_of_service
   		or a.to_date_of_service != b.to_date_of_service);
 
 
 /******************************
  * Older code here
  */
 

--1 clean up from date of service from matching claim in claim_header 
update dw_staging.mcd_claim_detail a
   set from_date_of_service = b.from_date_of_service 
  from dw_staging.mcd_claim_header b 
 where a.uth_member_id = b.uth_member_id 
   and a.uth_claim_id = b.uth_claim_id 
   and a.from_date_of_service not between '2011-01-01' and current_date
   and b.from_date_of_service between '2011-01-01' and current_date; 
  
--2 clean up to date of service from matching claim in claim_header   
update dw_staging.mcd_claim_detail a
   set to_date_of_service  = b.to_date_of_service 
  from dw_staging.mcd_claim_header b 
 where a.uth_member_id = b.uth_member_id 
   and a.uth_claim_id = b.uth_claim_id 
   and a.to_date_of_service not between '2011-01-01' and current_date
   and b.to_date_of_service between '2011-01-01' and current_date; 
  
vacuum analyze dw_staging.mcd_claim_detail;

--3 update year and fiscal_year 
update dw_staging.mcd_claim_detail 
   set "year" = extract(year from from_date_of_service);

update dw_staging.mcd_claim_detail 
   set fiscal_year = dev.fiscal_year_func(from_date_of_service) 
 where fiscal_year not between 2011 and extract(year from current_date);

update dw_staging.mcd_claim_detail 
   set month_year_id  = 
   	  (
       extract(year from from_date_of_service)::text ||
	   lpad(extract(month from from_date_of_service)::text,2,'0')
	  )::int;

vacuum analyze dw_staging.mcd_claim_detail;
alter table dw_staging.mcd_claim_detail owner to uthealth_dev;
grant select on dw_staging.mcd_claim_detail to uthealth_analyst;
