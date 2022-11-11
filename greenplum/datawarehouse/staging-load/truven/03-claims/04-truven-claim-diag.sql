
/* ******************************************************************************************************
 *  claim diag load for truven
 * ******************************************************************************************************
*   jw001   || 9/24/2021  || need to put claim sequence number in insert and select statements. 
 * ******************************************************************************************************
 *  gmunoz  || 10/29/2021 || added fiscal year logic with function dev.fiscal_year_func
 * ******************************************************************************************************
 *  gmunoz  || 11/05/2021 || removed the data_year column in insert on all four inserts
 * ******************************************************************************************************
 *  jwozny  || 1/03/2022  || removed icd_type, year, fiscal_year 
 * ******************************************************************************************************
 *  iperez  || 09/28/2022 || added claim id source and member id source to columns
 * ******************************************************************************************************
 *  iperez  || 09/30/2022 || removed claim id source and member id source to columns
 * ******************************************************************************************************
 * */

drop table if exists dev.claim_diag_truv;

--claim diag
create table dev.claim_diag_truv
(like data_warehouse.claim_diag including defaults) 
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
distributed by (uth_member_id)
partition by list(data_source)
 (partition optz values ('optz'),
  partition optd values ('optd'),
  partition truv values ('truv'),
  partition mdcd values ('mdcd'),
  partition mcrt values ('mcrt'),
  partition mcrn values ('mcrn')
 )
;

-------------------------------- truven commercial outpatient -------------------------------------- 22m
insert into dev.claim_diag_truv ( data_source, 
                                    uth_member_id, 
                                    uth_claim_id, 
                                    from_date_of_service,
                                    diag_cd, 
                                    diag_position, 
                                    poa_src,
                                    icd_version,
                                    claim_id_src,
                                    member_id_src,
                                    load_date
                                    )  							        						              
select  'truv', 
         b.uth_member_id, 
         b.uth_claim_id, 
		 a.svcdate,
	     unnest(array[a.dx1, a.dx2, a.dx3, a.dx4]) as dx_cd,
		 unnest(array[1,2,3,4]) as dx_pos,
         null,
         a.dxver,
         a.msclmid::text,
         a.enrolid::text,
         current_date
from truven.ccaeo a
join dev.truven_temp_detail  b 
  on b.member_id_src = a.enrolid 
 and b.claim_id_src = a.msclmid 
;


analyze dev.claim_diag_truv;



  
-------------------------------- truven medicare outpatient -------------------------------------- 4min
insert into dev.claim_diag_truv ( data_source, 
                                    uth_member_id, 
                                    uth_claim_id, 
                                    from_date_of_service,
                                    diag_cd, 
                                    diag_position, 
                                    poa_src,
                                    icd_version,
                                    claim_id_src,
                                    member_id_src,
                                    load_date
                                    )    								        						              
select  'truv', 
         b.uth_member_id, 
         b.uth_claim_id, 
		 a.svcdate,
	     unnest(array[a.dx1, a.dx2, a.dx3, a.dx4]) as dx_cd,
		 unnest(array[1,2,3,4]) as dx_pos,
         null,
         a.dxver,
         a.msclmid::text,
         a.enrolid::text,
         current_date
from truven.mdcro a
join dev.truven_temp_detail  b 
  on b.member_id_src = a.enrolid 
 and b.claim_id_src = a.msclmid 
;

vacuum analyze dev.claim_diag_truv;

  
 -------------------------------- truven commercial inpatient ------
insert into dev.claim_diag_truv ( data_source, 
                                    uth_member_id, 
                                    uth_claim_id, 
                                    from_date_of_service,
                                    diag_cd, 
                                    diag_position, 
                                    poa_src,
                                    icd_version,
                                    claim_id_src,
                                    member_id_src,
                                    load_date
                                    )  								        						              
select  'truv', 
         b.uth_member_id, 
         b.uth_claim_id, 
		 a.svcdate,
	     unnest(array[a.pdx, a.dx1, a.dx2, a.dx3, a.dx4]) as dx_cd,
		 unnest(array[1,2,3,4,5]) as dx_pos,
         null,
         a.dxver,
         a.msclmid::text,
         a.enrolid::text,
         current_date
from truven.ccaes a 
join dev.truven_temp_detail  b 
  on b.member_id_src = a.enrolid 
 and b.claim_id_src = a.msclmid 
;

  vacuum analyze dev.claim_diag_truv;
 
-------------------------------- truven medicare inpatient ------
insert into dev.claim_diag_truv ( data_source, 
                                    uth_member_id, 
                                    uth_claim_id, 
                                    from_date_of_service,
                                    diag_cd, 
                                    diag_position, 
                                    poa_src,
                                    icd_version,
                                    claim_id_src,
                                    member_id_src,
                                    load_date
                                    )  									        						              
select  'truv', 
         b.uth_member_id, 
         b.uth_claim_id, 
		 a.svcdate,
	     unnest(array[a.pdx, a.dx1, a.dx2, a.dx3, a.dx4]) as dx_cd,
		 unnest(array[1,2,3,4,5]) as dx_pos,
         null,
         a.dxver,
         a.msclmid::text,
         a.enrolid::text,
         current_date
from truven.mdcrs  a 
join dev.truven_temp_detail  b 
  on b.member_id_src = a.enrolid 
 and b.claim_id_src = a.msclmid 
;

  vacuum analyze dev.claim_diag_truv;

--clean up null rows

delete from dev.claim_diag_truv cd where diag_cd is null;
    
vacuum analyze dev.claim_diag_truv;
 
analyze dev.claim_diag_truv;

drop table if exists dev.truven_detail_lines ;
