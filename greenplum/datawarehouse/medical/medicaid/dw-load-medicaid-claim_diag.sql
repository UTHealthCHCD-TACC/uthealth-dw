

select * from medicaid.clm_dx cd 


insert into data_warehouse.claim_diag (data_source, year, uth_member_id, uth_claim_id, claim_sequence_number, 
                                       from_date_of_service, diag_cd, diag_position, icd_type, poa_src, fiscal_year)
select * 
from ( 
		select 'mdcd', extract(year from d.hdr_frm_dos::date), c.uth_member_id, c.uth_claim_id, '1' as seq, 
		       d.hdr_frm_dos::date, 
			   unnest(array[ trim(a.prim_dx_cd), trim(a.dx_cd_1), trim(a.dx_cd_2), trim(a.dx_cd_3), trim(a.dx_cd_4), trim(a.dx_cd_5), trim(a.dx_cd_6), 
			                  trim(a.dx_cd_7), trim(a.dx_cd_8), trim(a.dx_cd_9), trim(a.dx_cd_10), trim(a.dx_cd_11), trim(a.dx_cd_12), trim(a.dx_cd_13), 
			                  trim(a.dx_cd_14), trim(a.dx_cd_15), trim(a.dx_cd_16), trim(a.dx_cd_17), trim(a.dx_cd_18), trim(a.dx_cd_19),
			                  trim(a.dx_cd_20), trim(a.dx_cd_21), trim(a.dx_cd_22), trim(a.dx_cd_23), trim(a.dx_cd_24), trim(a.dx_cd_25) ] )as dx_cd,
			    unnest(array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25]) as dx_pos, 
				a.prim_dx_qal, 
		 	    unnest(array[a.prm_dx_poa, a.dx_poa_1, a.dx_poa_2, a.dx_poa_3, a.dx_poa_4, a.dx_poa_5, a.dx_poa_6, 
		              a.dx_poa_7, a.dx_poa_8, a.dx_poa_9, a.dx_poa_10, a.dx_poa_11, a.dx_poa_12, a.dx_poa_13, 
		              a.dx_poa_14, a.dx_poa_15, a.dx_poa_16, a.dx_poa_17, a.dx_poa_18, a.dx_poa_19,
		              a.dx_poa_20, a.dx_poa_21, a.dx_poa_22, a.dx_poa_23, a.dx_poa_24, a.dx_poa_25 ]) as dx_poa,
		        a.year_fy 
		from medicaid.clm_dx a
		  join medicaid.clm_proc b
		    on b.icn = a.icn 
		   and b.year_fy = a.year_fy 
		  join data_warehouse.dim_uth_claim_id c 
		    on c.claim_id_src = b.icn 
		   and c.member_id_src = b.pcn 
		  join medicaid.clm_header d  
		     on d.icn = b.icn 
		    and d.year_fy = b.year_fy
) inr where dx_cd <> ''
 ;


insert into data_warehouse.claim_diag (data_source, year, uth_member_id, uth_claim_id, claim_sequence_number, 
                                       from_date_of_service, diag_cd, diag_position, icd_type, poa_src, fiscal_year)
select * 
from ( 
		select 'mdcd', extract(year from d.frm_dos), c.uth_member_id, c.uth_claim_id, '1' as seq, 
		       d.frm_dos, 
			   unnest(array[ trim(a.prim_dx_cd), trim(a.dx_cd_1), trim(a.dx_cd_2), trim(a.dx_cd_3), trim(a.dx_cd_4), trim(a.dx_cd_5), trim(a.dx_cd_6), 
			                  trim(a.dx_cd_7), trim(a.dx_cd_8), trim(a.dx_cd_9), trim(a.dx_cd_10), trim(a.dx_cd_11), trim(a.dx_cd_12), trim(a.dx_cd_13), 
			                  trim(a.dx_cd_14), trim(a.dx_cd_15), trim(a.dx_cd_16), trim(a.dx_cd_17), trim(a.dx_cd_18), trim(a.dx_cd_19),
			                  trim(a.dx_cd_20), trim(a.dx_cd_21), trim(a.dx_cd_22), trim(a.dx_cd_23), trim(a.dx_cd_24) ] )as dx_cd,
			    unnest(array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25]) as dx_pos, 
				a.prim_dx_qal, 
		 	    unnest(array[a.prm_dx_poa, a.dx_poa_1, a.dx_poa_2, a.dx_poa_3, a.dx_poa_4, a.dx_poa_5, a.dx_poa_6, 
		              a.dx_poa_7, a.dx_poa_8, a.dx_poa_9, a.dx_poa_10, a.dx_poa_11, a.dx_poa_12, a.dx_poa_13, 
		              a.dx_poa_14, a.dx_poa_15, a.dx_poa_16, a.dx_poa_17, a.dx_poa_18, a.dx_poa_19,
		              a.dx_poa_20, a.dx_poa_21, a.dx_poa_22, a.dx_poa_23, a.dx_poa_24]) as dx_poa,
		        a.year_fy 
		from medicaid.enc_dx a
		  join medicaid.enc_proc b
		    on trim(b.derv_enc) = trim(a.derv_enc)
		   and b.year_fy = a.year_fy 
		  join data_warehouse.dim_uth_claim_id c 
		    on c.member_id_src = b.mem_id
		   and c.claim_id_src = b.derv_enc 
		  join medicaid.enc_header d  
		     on d.derv_enc = b.derv_enc 
		    and d.year_fy = b.year_fy		   		    
) inr where dx_cd <> ''
 ;
