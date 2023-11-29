 /* ******************************************************************************************************
 * Author || Date      || Notes
 * ******************************************************************************************************
 * various authors  || <10/25/2022  || created
 * ******************************************************************************************************
 * jwozny 			|| 10/25/2022   || Several claims have dates of service outside of the valid date ranges.
										The corresponding claim in claim_header often has the correct date of service
										So we update claim detail from claim header and re-derive year
 * ******************************************************************************************************
 * xzhang  			|| 09/05/2023   || Changed table name from claim_detail to mcd_claim_detail
 * ******************************************************************************************************
 * xzhang			|| 10/20/2023   || Actually, all dates should be set to the from_dos from claim_header because
 * 									   there are typos in claim_details (e.g. 2022-10-15 becomes 2023-10-15)
 * 									   Code is for live tables so on next run, change to dw_staging tables
 * ******************************************************************************************************
 * xzhang			|| 11/17/2023	|| Code changed to dw_staging tables and consolidated into single update statement
*/


 --1 clean up from date of service from matching claim in claim_header 
update dw_staging.mcd_claim_detail a
   set from_date_of_service = b.from_date_of_service,
   	   to_date_of_service = b.to_date_of_service,
   	   "year" = b."year",
   	   fiscal_year = b.fiscal_year,
   	   month_year_id = get_my_from_date(b.from_date_of_service)
  from dw_staging.mcd_claim_header b 
 where a.member_id_src = b.member_id_src 
    and a.claim_id_src = b.claim_id_src 
    and (a.from_date_of_service != b.from_date_of_service
   		or a.to_date_of_service != b.to_date_of_service);
 
vacuum analyze dw_staging.mcd_claim_detail;















