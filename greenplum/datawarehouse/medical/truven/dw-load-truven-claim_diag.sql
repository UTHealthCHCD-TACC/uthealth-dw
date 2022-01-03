
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
 * */


create table dev.truven_detail_lines 
with (appendonly=true, orientation=column, compresstype=zlib) as 
select a.uth_claim_id, a.uth_member_id, a.claim_sequence_number_src, a.claim_sequence_number, b.member_id_src, b.claim_id_src
from dw_staging.claim_detail a 
   join data_warehouse.dim_uth_claim_id b 
      on a.uth_member_id = b.uth_member_id 
     and a.uth_claim_id = b.uth_claim_id 
     and a.data_source = 'truv' 
distributed by (member_id_src) 
;

analyze dev.truven_detail_lines;

-------------------------------- truven commercial outpatient --------------------------------------
insert into dw_staging.claim_diag ( data_source, 
                                    uth_member_id, 
                                    uth_claim_id, 
                                    claim_sequence_number,
                                    from_date_of_service,
                                    diag_cd, 
                                    diag_position, 
                                    poa_src
                                     )  								        						              
select  'truv', 
         b.uth_member_id, 
         b.uth_claim_id, 
         b.claim_sequence_number,
		 a.svcdate,
	     unnest(array[a.dx1, a.dx2, a.dx3, a.dx4]) as dx_cd,
		 unnest(array[1,2,3,4]) as dx_pos,
         null
   from truven.ccaeo a 
   join dev.truven_detail_lines  b 
     on b.member_id_src = a.member_id_src
    and b.claim_id_src = a.msclmid::text
    and b.claim_sequence_number_src = a.seqnum::text
;


select count(*) from dw_staging.claim_diag cd where data_source = 'truv';
  
  
-------------------------------- truven medicare outpatient -------------------------------------- 
insert into dw_staging.claim_diag ( data_source, 
                                    uth_member_id, 
                                    uth_claim_id, 
                                    claim_sequence_number,
                                    from_date_of_service,
                                    diag_cd, 
                                    diag_position, 
                                    poa_src
)  								        						              
select  'truv', 
         b.uth_member_id, 
         b.uth_claim_id, 
         b.claim_sequence_number,
		 a.svcdate,
	     unnest(array[a.dx1, a.dx2, a.dx3, a.dx4]) as dx_cd,
		 unnest(array[1,2,3,4]) as dx_pos,
         null
from truven.mdcro a 
   join dev.truven_detail_lines  b 
     on b.member_id_src = a.member_id_src
    and b.claim_id_src = a.msclmid::text
    and b.claim_sequence_number_src = a.seqnum::text
;



  
 -------------------------------- truven commercial inpatient ------
insert into dw_staging.claim_diag ( data_source, 
                                    uth_member_id, 
                                    uth_claim_id, 
                                    claim_sequence_number,
                                    from_date_of_service,
                                    diag_cd, 
                                    diag_position, 
                                    poa_src
                                    )  								        						              
select  'truv', 
         b.uth_member_id, 
         b.uth_claim_id, 
         b.claim_sequence_number,
		 a.svcdate,
	     unnest(array[a.pdx, a.dx1, a.dx2, a.dx3, a.dx4]) as dx_cd,
		 unnest(array[1,2,3,4,5]) as dx_pos,
         null
   from truven.ccaes a 
   join dev.truven_detail_lines  b 
     on b.member_id_src = a.member_id_src
    and b.claim_id_src = a.msclmid::text
    and b.claim_sequence_number_src = a.seqnum::text
;

  
-------------------------------- truven medicare inpatient ------
insert into dw_staging.claim_diag ( data_source, 
                                    uth_member_id, 
                                    uth_claim_id, 
                                    claim_sequence_number,
                                    from_date_of_service,
                                    diag_cd, 
                                    diag_position,
                                    poa_src
                                    )  								        						              
select  'truv', 
         b.uth_member_id, 
         b.uth_claim_id, 
         b.claim_sequence_number,
		 a.svcdate,
	     unnest(array[a.pdx, a.dx1, a.dx2, a.dx3, a.dx4]) as dx_cd,
		 unnest(array[1,2,3,4,5]) as dx_pos,
         null
from truven.mdcrs a 
   join dev.truven_detail_lines  b 
     on b.member_id_src = a.member_id_src
    and b.claim_id_src = a.msclmid::text
    and b.claim_sequence_number_src = a.seqnum::text
;




--clean up null rows

delete from dw_staging.claim_diag cd where diag_cd is null;
    
analyze dw_staging.claim_diag;

