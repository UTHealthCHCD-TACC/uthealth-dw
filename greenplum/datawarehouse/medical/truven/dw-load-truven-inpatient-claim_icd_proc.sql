--- Claim icd proc


-------------------------------- truven commercial inpatient --------------------------------------	 
insert into data_warehouse.claim_icd_proc (data_source, year, uth_claim_id, uth_member_id, date, 
										   proc_cd, proc_position, icd_type)  
select  'truv', h.year, h.uth_claim_id, h.uth_member_id, h.from_date_of_service,
		unnest(array[proc1, proc2, proc3, proc4, proc5, proc6, proc7, proc8, proc9,
                      proc10, proc11, proc12, proc13, proc14, proc15])  as proc_cd,
		unnest(array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]) as proc_pos,
		 i.dxver
from data_warehouse.claim_header  h
  join truven.ccaei i 
	 on i.caseid::text = h.admission_id_src 
	and i.enrolid::text = h.member_id_src 
;


-------------------------------- truven medicare inpatient --------------------------------------	 
insert into data_warehouse.claim_icd_proc (data_source, year, uth_claim_id, uth_member_id, date, 
										   proc_cd, proc_position, icd_type)  
select  'truv', h.year, h.uth_claim_id, h.uth_member_id, h.from_date_of_service,
		unnest(array[proc1, proc2, proc3, proc4, proc5, proc6, proc7, proc8, proc9,
                      proc10, proc11, proc12, proc13, proc14, proc15])  as proc_cd,
		unnest(array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]) as proc_pos,
		 i.dxver
from data_warehouse.claim_header  h
  join truven.mdcri i 
	 on i.caseid::text = h.admission_id_src 
	and i.enrolid::text = h.member_id_src 
;

--cleanup

delete from data_warehouse.claim_icd_proc where data_source = 'truv' and proc_cd is null;

vacuum full data_warehouse.claim_icd_proc;

analyze data_warehouse.claim_icd_proc;

