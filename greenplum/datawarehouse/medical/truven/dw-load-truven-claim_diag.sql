
/* ******************************************************************************************************
 *  claim diag load for truven
 * ******************************************************************************************************
*   jw001   || 9/24/2021  || need to put claim sequence number in insert and select statements. 
 * ******************************************************************************************************
 *  gmunoz  || 10/29/2021 || added fiscal year logic with function dev.fiscal_year_func
 * ******************************************************************************************************
 * */

delete from data_warehouse.claim_diag where data_source = 'truv' and data_year = 2019;


alter table data_warehouse.claim_diag add column data_year int2;

--create copy with matching distribution key
create table dev.wc_truven_claim_detail
with(appendonly=true,orientation=column,compresstype=zlib)
as select *
from data_warehouse.claim_detail cd 
where data_source = 'truv' and data_year = 2019
distributed by(member_id_src);

-------------------------------- truven commercial outpatient --------------------------------------
insert into data_warehouse.claim_diag (data_source, year, uth_member_id, uth_claim_id, claim_sequence_number
									  ,date, diag_cd, diag_position, icd_type, data_year, fiscal_year)  								        						              
select  'truv', d.year, d.uth_member_id, d.uth_claim_id, d.claim_sequence_number
		,d.from_date_of_service 
	    ,unnest(array[a.dx1, a.dx2, a.dx3, a.dx4]) as dx_cd
		,unnest(array[1,2,3,4]) as dx_pos 
		,a.dxver, a.year as data_yr,
    dev.fiscal_year_func(a.svcdate)
from truven.ccaeo a 
  join dev.wc_truven_claim_detail d 
    on d.member_id_src = a.enrolid::text 
   and d.claim_id_src = a.msclmid::text 
   and d.from_date_of_service = a.svcdate 
   and d.claim_sequence_number_src = a.seqnum::text
where a.year = 2019
  ;
  
  
-------------------------------- truven medicare outpatient -------------------------------------- 
insert into data_warehouse.claim_diag (data_source, year, uth_member_id, uth_claim_id, claim_sequence_number
									  ,date, diag_cd, diag_position, icd_type, data_year, fiscal_year) 								        						              
select  'truv', d.year, d.uth_member_id, d.uth_claim_id, d.claim_sequence_number
		,d.from_date_of_service 
	    ,unnest(array[a.dx1, a.dx2, a.dx3, a.dx4]) as dx_cd
		,unnest(array[1,2,3,4]) as dx_pos 
		,a.dxver , a.year as data_yr, dev.fiscal_year_func(a.fst_dt)
from truven.mdcro a 
  join dev.wc_truven_claim_detail d 
    on d.member_id_src = a.enrolid::text 
   and d.claim_id_src = a.msclmid::text 
   and d.from_date_of_service = a.svcdate 
   and d.claim_sequence_number_src = a.seqnum::text
 where a.year = 2019
  ;
  

  
 -------------------------------- truven commercial inpatient ------
insert into data_warehouse.claim_diag (data_source, year, uth_member_id, uth_claim_id, claim_sequence_number
									  ,date, diag_cd, diag_position, icd_type, data_year, dev.fiscal_year)  								        						              
select  'truv', d.year, d.uth_member_id, d.uth_claim_id, d.claim_sequence_number
		,d.from_date_of_service 
	    ,unnest(array[a.dx1, a.dx2, a.dx3, a.dx4]) as dx_cd
		,unnest(array[1,2,3,4]) as dx_pos 
		,a.dxver , a.year as data_yr,
    dev.fiscal_year_func(a.fst_dt)
from truven.ccaes a 
  join dev.wc_truven_claim_detail d 
    on d.member_id_src = a.enrolid::text 
   and d.claim_id_src = a.msclmid::text 
   and d.from_date_of_service = a.svcdate 
   and d.claim_sequence_number_src = a.seqnum::text
   where a.year = 2019
  ;
 
  
-------------------------------- truven medicare inpatient ------
insert into data_warehouse.claim_diag (data_source, year, uth_member_id, uth_claim_id, claim_sequence_number
									  ,date, diag_cd, diag_position, icd_type, data_year, dev,fiscal_year)  								        						              
select  'truv', d.year, d.uth_member_id, d.uth_claim_id, d.claim_sequence_number
		,d.from_date_of_service 
	    ,unnest(array[a.dx1, a.dx2, a.dx3, a.dx4]) as dx_cd
		,unnest(array[1,2,3,4]) as dx_pos 
		,a.dxver , a.year as data_yr,
    dev.fiscal_year_func(a.from_date_of_service)
from truven.mdcrs a
  join dev.wc_truven_claim_detail d 
    on d.member_id_src = a.enrolid::text 
   and d.claim_id_src = a.msclmid::text 
   and d.from_date_of_service = a.svcdate 
   and d.claim_sequence_number_src = a.seqnum::text
 where a.year = 2019
  ;  


--clean up null rows

delete from data_warehouse.claim_diag cd where diag_cd is null;
    
vacuum analyze data_warehouse.claim_diag;

drop table dev.wc_truven_claim_detail;
  