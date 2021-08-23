/* ******************************************************************************************************
 * Loads claim_icd_proc with medicaid data
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wc001  || 1/01/2021 || script created 
 * ******************************************************************************************************
 *  wallingTACC || 8/23/2021 || updated comments.
 * ****************************************************************************************************** */


insert into data_warehouse.claim_icd_proc (data_source, year, uth_member_id, uth_claim_id, claim_sequence_number, 
                                       from_date_of_service, proc_cd, proc_position, icd_type,  fiscal_year)
select * 
from ( 
		select 'mdcd', extract(year from d.hdr_frm_dos::date), c.uth_member_id, c.uth_claim_id, '1' as seq, 
		       d.hdr_frm_dos::date, 
			   unnest(array[  trim(a.proc_icd_cd_1), trim(a.proc_icd_cd_2), trim(a.proc_icd_cd_3), trim(a.proc_icd_cd_4), trim(a.proc_icd_cd_5), trim(a.proc_icd_cd_6), 
			                  trim(a.proc_icd_cd_7), trim(a.proc_icd_cd_8), trim(a.proc_icd_cd_9), trim(a.proc_icd_cd_10), trim(a.proc_icd_cd_11), trim(a.proc_icd_cd_12), trim(a.proc_icd_cd_13), 
			                  trim(a.proc_icd_cd_14), trim(a.proc_icd_cd_15), trim(a.proc_icd_cd_16), trim(a.proc_icd_cd_17), trim(a.proc_icd_cd_18), trim(a.proc_icd_cd_19),
			                  trim(a.proc_icd_cd_20), trim(a.proc_icd_cd_21), trim(a.proc_icd_cd_22), trim(a.proc_icd_cd_23), trim(a.proc_icd_cd_24), trim(a.proc_icd_cd_25) ] )as proc_icd_cd,
			    unnest(array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25]) as proc_icd_pos, 
				a.proc_icd_qal_1, 
		        a.year_fy 
		from medicaid.clm_proc a
		  join data_warehouse.dim_uth_claim_id c 
		    on c.claim_id_src = a.icn 
		   and c.member_id_src = a.pcn 
		  join medicaid.clm_header d  
		     on d.icn = a.icn 
		    and d.year_fy = a.year_fy		  		    
) inr where proc_icd_cd <> ''
 ;



insert into data_warehouse.claim_icd_proc (data_source, year, uth_member_id, uth_claim_id, claim_sequence_number, 
                                       from_date_of_service, proc_cd, proc_position, icd_type,  fiscal_year)
select * 
from ( 
		select 'mdcd', extract(year from d.frm_dos), c.uth_member_id, c.uth_claim_id, '1' as seq, 
		       d.frm_dos, 
			   unnest(array[  trim(a.proc_icd_cd_1), trim(a.proc_icd_cd_2), trim(a.proc_icd_cd_3), trim(a.proc_icd_cd_4), trim(a.proc_icd_cd_5), trim(a.proc_icd_cd_6), 
			                  trim(a.proc_icd_cd_7), trim(a.proc_icd_cd_8), trim(a.proc_icd_cd_9), trim(a.proc_icd_cd_10), trim(a.proc_icd_cd_11), trim(a.proc_icd_cd_12), trim(a.proc_icd_cd_13), 
			                  trim(a.proc_icd_cd_14), trim(a.proc_icd_cd_15), trim(a.proc_icd_cd_16), trim(a.proc_icd_cd_17), trim(a.proc_icd_cd_18), trim(a.proc_icd_cd_19),
			                  trim(a.proc_icd_cd_20), trim(a.proc_icd_cd_21), trim(a.proc_icd_cd_22), trim(a.proc_icd_cd_23), trim(a.proc_icd_cd_24) ] )as proc_icd_cd,
			    unnest(array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24]) as proc_icd_pos, 
				a.proc_icd_qal_1, 
		        a.year_fy 
		from medicaid.enc_proc a 
		  join data_warehouse.dim_uth_claim_id c 
		    on c.claim_id_src = a.derv_enc 
		   and c.member_id_src = a.mem_id 
		  join medicaid.enc_header d 
		     on d.derv_enc = a.derv_enc 
		    and d.year_fy = a.year_fy			    		    
) inr where proc_icd_cd <> ''
 ;

vacuum analyze data_warehouse.claim_icd_proc;
