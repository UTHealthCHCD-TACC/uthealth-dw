
/* ******************************************************************************************************
 *  claim diag load for truven
 * ******************************************************************************************************
*   jw001   || 9/24/2021  || need to put claim sequence number in insert and select statements. 
 * ******************************************************************************************************
 *  gmunoz  || 10/29/2021 || added fiscal year logic with function dev.fiscal_year_func
 * ******************************************************************************************************
 *  gmunoz  || 11/05/2021 || removed the data_year column in insert on all four inserts
 * ******************************************************************************************************
 * */




-------------------------------- truven commercial outpatient --------------------------------------
insert into dw_staging.claim_diag ( data_source, 
                                    year, 
                                    uth_member_id, 
                                    uth_claim_id, 
                                    claim_sequence_number,
                                    from_date_of_service,
                                    diag_cd, 
                                    diag_position, 
                                    icd_type, 
                                    poa_src,
                                    fiscal_year )  								        						              
select  'truv', 
         extract(year from a.svcdate) as yr,
         b.uth_member_id, 
         b.uth_claim_id, 
         a.seqnum,
		 a.svcdate,
	     unnest(array[a.dx1, a.dx2, a.dx3, a.dx4]) as dx_cd,
		 unnest(array[1,2,3,4]) as dx_pos,
         a.dxver, 
         null,
         dev.fiscal_year_func(a.svcdate)
from truven.ccaeo a 
   join dev.truven_dim_uth_claim_id b 
     on b.member_id_src = a.member_id_src
    and b.claim_id_src = a.msclmid::text
    and b.data_source  = 'truv'
    and b.data_year = a.year
;

  
  
-------------------------------- truven medicare outpatient -------------------------------------- 
insert into dw_staging.claim_diag ( data_source, 
                                    year, 
                                    uth_member_id, 
                                    uth_claim_id, 
                                    claim_sequence_number,
                                    from_date_of_service,
                                    diag_cd, 
                                    diag_position, 
                                    icd_type, 
                                    poa_src,
                                    fiscal_year )  								        						              
select  'truv', 
         extract(year from a.svcdate) as yr,
         b.uth_member_id, 
         b.uth_claim_id, 
         a.seqnum,
		 a.svcdate,
	     unnest(array[a.dx1, a.dx2, a.dx3, a.dx4]) as dx_cd,
		 unnest(array[1,2,3,4]) as dx_pos,
         a.dxver, 
         null,
         dev.fiscal_year_func(a.svcdate)
from truven.mdcro a 
   join dev.truven_dim_uth_claim_id b 
     on b.member_id_src = a.enrolid::text
    and b.claim_id_src = a.msclmid::text
    and b.data_source  = 'truv'
    and b.data_year = a.year
;

  
 -------------------------------- truven commercial inpatient ------
insert into dw_staging.claim_diag ( data_source, 
                                    year, 
                                    uth_member_id, 
                                    uth_claim_id, 
                                    claim_sequence_number,
                                    from_date_of_service,
                                    diag_cd, 
                                    diag_position, 
                                    icd_type, 
                                    poa_src,
                                    fiscal_year )  								        						              
select  'truv', 
         extract(year from a.svcdate) as yr,
         b.uth_member_id, 
         b.uth_claim_id, 
         a.seqnum,
		 a.svcdate,
	     unnest(array[a.pdx, a.dx1, a.dx2, a.dx3, a.dx4]) as dx_cd,
		 unnest(array[1,2,3,4,5]) as dx_pos,
         a.dxver, 
         null,
         dev.fiscal_year_func(a.svcdate)
from truven.ccaes a 
   join dev.truven_dim_uth_claim_id b 
     on b.member_id_src = a.enrolid::text
    and b.claim_id_src = a.msclmid::text
    and b.data_source  = 'truv'
    and b.data_year = a.year
;

 
  
-------------------------------- truven medicare inpatient ------
insert into dw_staging.claim_diag ( data_source, 
                                    year, 
                                    uth_member_id, 
                                    uth_claim_id, 
                                    claim_sequence_number,
                                    from_date_of_service,
                                    diag_cd, 
                                    diag_position, 
                                    icd_type, 
                                    poa_src,
                                    fiscal_year )  								        						              
select  'truv', 
         extract(year from a.svcdate) as yr,
         b.uth_member_id, 
         b.uth_claim_id, 
         a.seqnum,
		 a.svcdate,
	     unnest(array[a.pdx, a.dx1, a.dx2, a.dx3, a.dx4]) as dx_cd,
		 unnest(array[1,2,3,4,5]) as dx_pos,
         a.dxver, 
         null,
         dev.fiscal_year_func(a.svcdate)
from truven.mdcrs a 
   join dev.truven_dim_uth_claim_id b 
     on b.member_id_src = a.enrolid::text
    and b.claim_id_src = a.msclmid::text
    and b.data_source  = 'truv'
    and b.data_year = a.year
;



--clean up null rows

delete from dw_staging.claim_diag cd where diag_cd is null;
    
analyze dw_staging.claim_diag;

