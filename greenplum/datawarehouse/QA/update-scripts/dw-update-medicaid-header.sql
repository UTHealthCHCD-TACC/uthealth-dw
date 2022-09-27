/*
 * updating claim type for medicaid headers
 */
-- medicaid claims
with cte_pos as (select max(pos) as pos, icn
				   from medicaid.clm_detail 
				   group by icn)
update data_warehouse.claim_header c
   set claim_type = case when pos.pos = '1' then 'P' else 'F' end
  from medicaid.clm_header h
  join medicaid.clm_proc p
    on h.icn = p.icn
  join data_warehouse.dim_uth_claim_id u
    on u.claim_id_src = p.icn
   and u.member_id_src = p.pcn
   and u.data_source = 'mdcd'
  left outer join cte_pos as pos
    on h.icn = pos.icn
 where c.uth_claim_id = u.uth_claim_id;
 
-- medicaid encounters
update data_warehouse.claim_header c
   set claim_type = case when h.tx_cd = 'P' then 'P'
   						when h.tx_cd = 'I' then 'F'
   						else null end
  from medicaid.enc_header h
  join medicaid.enc_proc p
    on h.derv_enc = p.derv_enc
  join data_warehouse.dim_uth_claim_id u
    on u.claim_id_src = p.derv_enc
   and u.member_id_src = p.mem_id
   and u.data_source = 'mdcd'
 where c.uth_claim_id = u.uth_claim_id;