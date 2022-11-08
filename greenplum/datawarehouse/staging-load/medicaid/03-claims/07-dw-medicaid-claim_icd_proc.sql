/* ******************************************************************************************************
 * Loads dw_staging.claim_icd_proc with medicaid data
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wc001  || 1/01/2021 || script created 
 * ******************************************************************************************************
 *  wallingTACC || 8/23/2021 || updated comments.
 * ******************************************************************************************************
 *  wcough	    || 1/07/2022 || add icd_version back to table
 * ****************************************************************************************************** 
 * */

insert into dw_staging.claim_icd_proc (data_source, year, uth_member_id, fiscal_year, uth_claim_id, 
                                       from_date_of_service, proc_cd, proc_position, icd_version)
select * 
from ( 
		select distinct 'mdcd', extract(year from d.hdr_frm_dos::date), c.uth_member_id, d.year_fy, c.uth_claim_id, 
		       d.hdr_frm_dos::date, 
			   unnest(array[  trim(a.proc_icd_cd_1), trim(a.proc_icd_cd_2), trim(a.proc_icd_cd_3), trim(a.proc_icd_cd_4), trim(a.proc_icd_cd_5), trim(a.proc_icd_cd_6), 
			                  trim(a.proc_icd_cd_7), trim(a.proc_icd_cd_8), trim(a.proc_icd_cd_9), trim(a.proc_icd_cd_10), trim(a.proc_icd_cd_11), trim(a.proc_icd_cd_12), trim(a.proc_icd_cd_13), 
			                  trim(a.proc_icd_cd_14), trim(a.proc_icd_cd_15), trim(a.proc_icd_cd_16), trim(a.proc_icd_cd_17), trim(a.proc_icd_cd_18), trim(a.proc_icd_cd_19),
			                  trim(a.proc_icd_cd_20), trim(a.proc_icd_cd_21), trim(a.proc_icd_cd_22), trim(a.proc_icd_cd_23), trim(a.proc_icd_cd_24), trim(a.proc_icd_cd_25) ] )as proc_icd_cd,
			    unnest(array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25]) as proc_icd_pos,
			    unnest(array[  trim(a.proc_icd_qal_1), trim(a.proc_icd_qal_2), trim(a.proc_icd_qal_3), trim(a.proc_icd_qal_4), trim(a.proc_icd_qal_5), trim(a.proc_icd_qal_6), 
			                  trim(a.proc_icd_qal_7), trim(a.proc_icd_qal_8), trim(a.proc_icd_qal_9), trim(a.proc_icd_qal_10), trim(a.proc_icd_qal_11), trim(a.proc_icd_qal_12), trim(a.proc_icd_qal_13), 
			                  trim(a.proc_icd_qal_14), trim(a.proc_icd_qal_15), trim(a.proc_icd_qal_16), trim(a.proc_icd_qal_17), trim(a.proc_icd_qal_18), trim(a.proc_icd_qal_19),
			                  trim(a.proc_icd_qal_20), trim(a.proc_icd_qal_21), trim(a.proc_icd_qal_22), trim(a.proc_icd_qal_23), trim(a.proc_icd_qal_24), trim(a.proc_icd_qal_25) ] )as icd_version
		  from medicaid.clm_proc a 
		  join data_warehouse.dim_uth_claim_id c 
		    on c.claim_id_src = a.icn 
		   and c.member_id_src = a.pcn 
		   and c.data_source = 'mdcd'
		  join medicaid.clm_header d  
		     on d.icn = a.icn 
) inr where proc_icd_cd <> ''
 ;

analyze dw_staging.claim_icd_proc;

-----------------------enc----------------------------------------------------------

insert into dw_staging.claim_icd_proc (data_source, year, uth_member_id, fiscal_year, uth_claim_id, 
                                       from_date_of_service, proc_cd, proc_position, icd_version)
select * 
from ( 
		select distinct 'mdcd', extract(year from d.frm_dos::date), c.uth_member_id, d.year_fy , c.uth_claim_id,  
		       d.frm_dos, 
			   unnest(array[  trim(a.proc_icd_cd_1), trim(a.proc_icd_cd_2), trim(a.proc_icd_cd_3), trim(a.proc_icd_cd_4), trim(a.proc_icd_cd_5), trim(a.proc_icd_cd_6), 
			                  trim(a.proc_icd_cd_7), trim(a.proc_icd_cd_8), trim(a.proc_icd_cd_9), trim(a.proc_icd_cd_10), trim(a.proc_icd_cd_11), trim(a.proc_icd_cd_12), trim(a.proc_icd_cd_13), 
			                  trim(a.proc_icd_cd_14), trim(a.proc_icd_cd_15), trim(a.proc_icd_cd_16), trim(a.proc_icd_cd_17), trim(a.proc_icd_cd_18), trim(a.proc_icd_cd_19),
			                  trim(a.proc_icd_cd_20), trim(a.proc_icd_cd_21), trim(a.proc_icd_cd_22), trim(a.proc_icd_cd_23), trim(a.proc_icd_cd_24) ] )as proc_icd_cd,
			    unnest(array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24]) as proc_icd_pos,
		unnest(array[  trim(a.proc_icd_qal_1), trim(a.proc_icd_qal_2), trim(a.proc_icd_qal_3), trim(a.proc_icd_qal_4), trim(a.proc_icd_qal_5), trim(a.proc_icd_qal_6), 
			                  trim(a.proc_icd_qal_7), trim(a.proc_icd_qal_8), trim(a.proc_icd_qal_9), trim(a.proc_icd_qal_10), trim(a.proc_icd_qal_11), trim(a.proc_icd_qal_12), trim(a.proc_icd_qal_13), 
			                  trim(a.proc_icd_qal_14), trim(a.proc_icd_qal_15), trim(a.proc_icd_qal_16), trim(a.proc_icd_qal_17), trim(a.proc_icd_qal_18), trim(a.proc_icd_qal_19),
			                  trim(a.proc_icd_qal_20), trim(a.proc_icd_qal_21), trim(a.proc_icd_qal_22), trim(a.proc_icd_qal_23), trim(a.proc_icd_qal_24) ] )as icd_version
		from medicaid.enc_proc a 
		  join data_warehouse.dim_uth_claim_id c 
		    on c.claim_id_src = a.derv_enc 
		   and c.member_id_src = a.mem_id 
		   and c.data_source = 'mdcd'
		  join medicaid.enc_header d 
		     on d.derv_enc = a.derv_enc 
) inr where proc_icd_cd <> ''
 ;

vacuum analyze dw_staging.claim_icd_proc;

-------------- htw ----------------------------
insert into dw_staging.claim_icd_proc (data_source, year, uth_member_id, fiscal_year, uth_claim_id, 
                                       from_date_of_service, proc_cd, proc_position, icd_version)
select * 
from ( 
		select distinct 'mdcd', extract(year from d.hdr_frm_dos::date), c.uth_member_id, get_my_from_date(d.hdr_frm_dos::date), c.uth_claim_id, 
		       d.hdr_frm_dos::date, 
			   unnest(array[  trim(a.proc_icd_cd_1), trim(a.proc_icd_cd_2), trim(a.proc_icd_cd_3), trim(a.proc_icd_cd_4), trim(a.proc_icd_cd_5), trim(a.proc_icd_cd_6), 
			                  trim(a.proc_icd_cd_7), trim(a.proc_icd_cd_8), trim(a.proc_icd_cd_9), trim(a.proc_icd_cd_10), trim(a.proc_icd_cd_11), trim(a.proc_icd_cd_12), trim(a.proc_icd_cd_13), 
			                  trim(a.proc_icd_cd_14), trim(a.proc_icd_cd_15), trim(a.proc_icd_cd_16), trim(a.proc_icd_cd_17), trim(a.proc_icd_cd_18), trim(a.proc_icd_cd_19),
			                  trim(a.proc_icd_cd_20), trim(a.proc_icd_cd_21), trim(a.proc_icd_cd_22), trim(a.proc_icd_cd_23), trim(a.proc_icd_cd_24), trim(a.proc_icd_cd_25) ] )as proc_icd_cd,
			    unnest(array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25]) as proc_icd_pos,
			    unnest(array[  trim(a.proc_icd_qal_1), trim(a.proc_icd_qal_2), trim(a.proc_icd_qal_3), trim(a.proc_icd_qal_4), trim(a.proc_icd_qal_5), trim(a.proc_icd_qal_6), 
			                  trim(a.proc_icd_qal_7), trim(a.proc_icd_qal_8), trim(a.proc_icd_qal_9), trim(a.proc_icd_qal_10), trim(a.proc_icd_qal_11), trim(a.proc_icd_qal_12), trim(a.proc_icd_qal_13), 
			                  trim(a.proc_icd_qal_14), trim(a.proc_icd_qal_15), trim(a.proc_icd_qal_16), trim(a.proc_icd_qal_17), trim(a.proc_icd_qal_18), trim(a.proc_icd_qal_19),
			                  trim(a.proc_icd_qal_20), trim(a.proc_icd_qal_21), trim(a.proc_icd_qal_22), trim(a.proc_icd_qal_23), trim(a.proc_icd_qal_24), trim(a.proc_icd_qal_25) ] )as icd_version
		  from medicaid.htw_clm_proc a 
		  join data_warehouse.dim_uth_claim_id c 
		    on c.claim_id_src = a.icn 
		   and c.member_id_src = a.pcn 
		   and c.data_source = 'mdcd' 
		  join medicaid.htw_clm_header d  
		     on d.icn = a.icn 
) inr where proc_icd_cd <> '' 
 ;

update dw_staging.claim_icd_proc set load_date = current_date;
update dw_staging.claim_icd_proc set icd_version = null where icd_version not in ('0','9');
vacuum analyze dw_staging.claim_icd_proc;
grant select on dw_staging.claim_diag to uthealth_analyst;

