# Stage pregnancy_claims_tbl

CREATE TABLE tableau.pregnancy_claims_tbl
	WITH (
		appendonly=true, orientation=column
	)
as
SELECT ch.id AS claim_header_id, ch.source AS data_source, ch.member_id_src, ch.uth_claim_id, ch.claim_id_src, 
ch.uth_member_id, ch.claim_type, ch.in_network, ch.admit_date, ch.discharge_date, ch.discharge_status_src, 
ch.admit_type_src, ch.total_cost, ch.total_paid, cd.id AS claim_detail_id, cd.proc_code, cd.proc_mod, cd.cost, cd.paid, 
cd.service_date, cd.paid_date, dx.id AS diagnosis_id, dx.diag_code, icd10.icd_10, icd10.description
   FROM dev.claim_header_dw ch
   JOIN dev.claim_detail_dw cd ON cd.claim_header_id = ch.id
   JOIN dev.claim_detail_diag_dw dx ON dx.claim_detail_id = cd.id
   JOIN reference_tables.icd_10 icd10 ON dx.diag_code::text = icd10.icd_10::text
  WHERE dx.diag_code::text ~~ 'O%'::text;