insert
	into
	dev.gm_dw_ip_admit_claim 
(data_source,
	admit_id,
	uth_member_id,
	enc_id,
	admit_date,
	discharge_date,
	enc_discharge_status ,
	uth_claim_id,
	from_date_of_service,
	to_date_of_service,
	claim_type)
select
	dia.data_source,
	admit_id,
	dia.uth_member_id,
	enc_id,
	admit_date,
	discharge_date,
	enc_discharge_status,
	uth_claim_id,
	from_date_of_service,
	to_date_of_service,
	claim_type
from
	dev.gm_dw_ip_admit dia
inner join data_warehouse.claim_header ch 
on
	dia.uth_member_id = ch.uth_member_id
	and 
(from_date_of_service between admit_date and discharge_date)
	and
(to_date_of_service between admit_date and discharge_date);