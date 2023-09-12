/* ******************************************************************************************************
 * Loads dw_staging.mcd_claim_diag with medicaid data
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wc001  || 1/01/2021 || script created 
 * ******************************************************************************************************
 *  wallingTACC || 8/23/2021 || updated comments.
 * ******************************************************************************************************
 *  wcough	    || 1/07/2022 || add icd_version back to table
 * ****************************************************************************************************** 
 * xzhang  		|| 09/05/2023 || Changed table name from claim_diag to mcd_claim_diag
 * 
 * */

drop table if exists dw_staging.mcd_claim_diag;

--claim diag
create table dw_staging.mcd_claim_diag
(like data_warehouse.claim_diag including defaults) 
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
distributed by (uth_member_id)
;


insert into dw_staging.mcd_claim_diag (data_source, year, uth_member_id, fiscal_year, uth_claim_id,  claim_id_src, member_id_src,
                                       from_date_of_service, diag_cd, diag_position, poa_src, icd_version)
select * 
from ( 
		select distinct 'mdcd', extract(year from d.hdr_frm_dos::date), 
		       c.uth_member_id, d.year_fy, c.uth_claim_id, b.icn, b.pcn,
		       d.hdr_frm_dos::date, 
			   unnest(array[ trim(a.prim_dx_cd), trim(a.dx_cd_1), trim(a.dx_cd_2), trim(a.dx_cd_3), trim(a.dx_cd_4), trim(a.dx_cd_5), trim(a.dx_cd_6), 
			                  trim(a.dx_cd_7), trim(a.dx_cd_8), trim(a.dx_cd_9), trim(a.dx_cd_10), trim(a.dx_cd_11), trim(a.dx_cd_12), trim(a.dx_cd_13), 
			                  trim(a.dx_cd_14), trim(a.dx_cd_15), trim(a.dx_cd_16), trim(a.dx_cd_17), trim(a.dx_cd_18), trim(a.dx_cd_19),
			                  trim(a.dx_cd_20), trim(a.dx_cd_21), trim(a.dx_cd_22), trim(a.dx_cd_23), trim(a.dx_cd_24), trim(a.dx_cd_25) ] )as dx_cd,
			    unnest(array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26]) as dx_pos, 
		 	    unnest(array[a.prm_dx_poa, a.dx_poa_1, a.dx_poa_2, a.dx_poa_3, a.dx_poa_4, a.dx_poa_5, a.dx_poa_6, 
		              a.dx_poa_7, a.dx_poa_8, a.dx_poa_9, a.dx_poa_10, a.dx_poa_11, a.dx_poa_12, a.dx_poa_13, 
		              a.dx_poa_14, a.dx_poa_15, a.dx_poa_16, a.dx_poa_17, a.dx_poa_18, a.dx_poa_19,
		              a.dx_poa_20, a.dx_poa_21, a.dx_poa_22, a.dx_poa_23, a.dx_poa_24, a.dx_poa_25 ]) as dx_poa,
		       		        unnest(array[a.prim_dx_qal, a.dx_cd_qual_1, a.dx_cd_qual_2, a.dx_cd_qual_3, a.dx_cd_qual_4, a.dx_cd_qual_5, a.dx_cd_qual_6,
		                     a.dx_cd_qual_7, a.dx_cd_qual_8, a.dx_cd_qual_9, a.dx_cd_qual_10, a.dx_cd_qual_11, a.dx_cd_qual_12, a.dx_cd_qual_13,
		                     a.dx_cd_qual_14, a.dx_cd_qual_15, a.dx_cd_qual_16, a.dx_cd_qual_17, a.dx_cd_qual_18, a.dx_cd_qual_19,
		                     a.dx_cd_qual_20, a.dx_cd_qual_21, a.dx_cd_qual_22, a.dx_cd_qual_23, a.dx_cd_qual_24, a.dx_cd_qual_25 ]) as icd_ver
		  from medicaid.clm_dx a 
		  join medicaid.clm_proc b 
		    on b.icn = a.icn 
		  join data_warehouse.dim_uth_claim_id c 
		    on c.claim_id_src = b.icn 
		   and c.member_id_src = b.pcn 
		   and c.data_source = 'mdcd'
		  join medicaid.clm_header d  
		     on d.icn = b.icn 
) inr where dx_cd <> ''
 ;

analyze dw_staging.mcd_claim_diag;

insert into dw_staging.mcd_claim_diag (data_source, year, uth_member_id, fiscal_year, uth_claim_id,  claim_id_src, member_id_src,
                                       from_date_of_service, diag_cd, diag_position, poa_src, icd_version)
select * 
from ( 
		select distinct 'mdcd', extract(year from d.frm_dos::date), 
		       c.uth_member_id, d.year_fy, c.uth_claim_id, b.derv_enc, b.mem_id,
		       d.frm_dos, 
			   unnest(array[ trim(a.prim_dx_cd), trim(a.dx_cd_1), trim(a.dx_cd_2), trim(a.dx_cd_3), trim(a.dx_cd_4), trim(a.dx_cd_5), trim(a.dx_cd_6), 
			                  trim(a.dx_cd_7), trim(a.dx_cd_8), trim(a.dx_cd_9), trim(a.dx_cd_10), trim(a.dx_cd_11), trim(a.dx_cd_12), trim(a.dx_cd_13), 
			                  trim(a.dx_cd_14), trim(a.dx_cd_15), trim(a.dx_cd_16), trim(a.dx_cd_17), trim(a.dx_cd_18), trim(a.dx_cd_19),
			                  trim(a.dx_cd_20), trim(a.dx_cd_21), trim(a.dx_cd_22), trim(a.dx_cd_23), trim(a.dx_cd_24) ] )as dx_cd,
			    unnest(array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25]) as dx_pos, 
		 	    unnest(array[a.prm_dx_poa, a.dx_poa_1, a.dx_poa_2, a.dx_poa_3, a.dx_poa_4, a.dx_poa_5, a.dx_poa_6, 
		              a.dx_poa_7, a.dx_poa_8, a.dx_poa_9, a.dx_poa_10, a.dx_poa_11, a.dx_poa_12, a.dx_poa_13, 
		              a.dx_poa_14, a.dx_poa_15, a.dx_poa_16, a.dx_poa_17, a.dx_poa_18, a.dx_poa_19,
		              a.dx_poa_20, a.dx_poa_21, a.dx_poa_22, a.dx_poa_23, a.dx_poa_24]) as dx_poa,
		        unnest(array[a.prim_dx_qal, a.dx_cd_qal_1, a.dx_cd_qal_2, a.dx_cd_qal_3, a.dx_cd_qal_4, a.dx_cd_qal_5, a.dx_cd_qal_6,
		                     a.dx_cd_qal_7, a.dx_cd_qal_8, a.dx_cd_qal_9, a.dx_cd_qal_10, a.dx_cd_qal_11, a.dx_cd_qal_12, a.dx_cd_qal_13,
		                     a.dx_cd_qal_14, a.dx_cd_qal_15, a.dx_cd_qal_16, a.dx_cd_qal_17, a.dx_cd_qal_18, a.dx_cd_qal_19,
		                     a.dx_cd_qal_20, a.dx_cd_qal_21, a.dx_cd_qal_22, a.dx_cd_qal_23, a.dx_cd_qal_24 ]) as icd_ver
		from medicaid.enc_dx a
		  join medicaid.enc_proc b
		    on trim(b.derv_enc) = trim(a.derv_enc)
		  join data_warehouse.dim_uth_claim_id c 
		    on c.member_id_src = b.mem_id
		   and c.claim_id_src = b.derv_enc 		   
		   and c.data_source = 'mdcd'
		  join medicaid.enc_header d  
		     on d.derv_enc = b.derv_enc 	   		    
) inr where dx_cd <> ''
 ;

vacuum analyze dw_staging.mcd_claim_diag;


insert into dw_staging.mcd_claim_diag (data_source, year, uth_member_id, fiscal_year, 
								   uth_claim_id, claim_id_src, member_id_src,
                                   from_date_of_service, diag_cd, diag_position, poa_src, icd_version)
select * 
from ( 
		select distinct 'mdcd', extract(year from d.hdr_frm_dos::date), 
		       c.uth_member_id, dev.fiscal_year_func(d.hdr_frm_dos::date),  
		       c.uth_claim_id, b.icn, b.pcn,
		       d.hdr_frm_dos::date, 
			   unnest(array[ trim(a.prim_dx_cd), trim(a.dx_cd_1), trim(a.dx_cd_2), trim(a.dx_cd_3), trim(a.dx_cd_4), trim(a.dx_cd_5), trim(a.dx_cd_6), 
			                  trim(a.dx_cd_7), trim(a.dx_cd_8), trim(a.dx_cd_9), trim(a.dx_cd_10), trim(a.dx_cd_11), trim(a.dx_cd_12), trim(a.dx_cd_13), 
			                  trim(a.dx_cd_14), trim(a.dx_cd_15), trim(a.dx_cd_16), trim(a.dx_cd_17), trim(a.dx_cd_18), trim(a.dx_cd_19),
			                  trim(a.dx_cd_20), trim(a.dx_cd_21), trim(a.dx_cd_22), trim(a.dx_cd_23), trim(a.dx_cd_24), trim(a.dx_cd_25) ] )as dx_cd,
			    unnest(array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26]) as dx_pos, 
		 	    unnest(array[a.prm_dx_poa, a.dx_poa_1, a.dx_poa_2, a.dx_poa_3, a.dx_poa_4, a.dx_poa_5, a.dx_poa_6, 
		              a.dx_poa_7, a.dx_poa_8, a.dx_poa_9, a.dx_poa_10, a.dx_poa_11, a.dx_poa_12, a.dx_poa_13, 
		              a.dx_poa_14, a.dx_poa_15, a.dx_poa_16, a.dx_poa_17, a.dx_poa_18, a.dx_poa_19,
		              a.dx_poa_20, a.dx_poa_21, a.dx_poa_22, a.dx_poa_23, a.dx_poa_24, a.dx_poa_25 ]) as dx_poa,
		       		        unnest(array[a.prim_dx_qal, a.dx_cd_qual_1, a.dx_cd_qual_2, a.dx_cd_qual_3, a.dx_cd_qual_4, a.dx_cd_qual_5, a.dx_cd_qual_6,
		                     a.dx_cd_qual_7, a.dx_cd_qual_8, a.dx_cd_qual_9, a.dx_cd_qual_10, a.dx_cd_qual_11, a.dx_cd_qual_12, a.dx_cd_qual_13,
		                     a.dx_cd_qual_14, a.dx_cd_qual_15, a.dx_cd_qual_16, a.dx_cd_qual_17, a.dx_cd_qual_18, a.dx_cd_qual_19,
		                     a.dx_cd_qual_20, a.dx_cd_qual_21, a.dx_cd_qual_22, a.dx_cd_qual_23, a.dx_cd_qual_24, a.dx_cd_qual_25 ]) as icd_ver
		from medicaid.htw_clm_dx a
		  join medicaid.htw_clm_proc b
		    on b.icn = a.icn 
		  join data_warehouse.dim_uth_claim_id c 
		    on c.claim_id_src = b.icn 
		   and c.member_id_src = b.pcn 
		   and c.data_source = 'mdcd'
		  join medicaid.htw_clm_header d  
		     on d.icn = b.icn 
) inr where dx_cd <> ''
 ;
 
update dw_staging.mcd_claim_diag set icd_version = null where icd_version not in ('0','9');
update dw_staging.mcd_claim_diag set load_date = current_date;
vacuum full analyze dw_staging.mcd_claim_diag;
grant select on dw_staging.mcd_claim_diag to uthealth_analyst;
alter table dw_staging.mcd_claim_diag owner to uthealth_dev;


