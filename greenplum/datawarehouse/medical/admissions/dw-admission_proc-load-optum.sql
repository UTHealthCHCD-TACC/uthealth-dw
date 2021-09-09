
--Optum load: 
insert into dw_qa.admission_icd_proc(data_source, year, uth_admission_id, uth_member_id, admit_date, proc_cd, proc_position, icd_type)
select distinct a.data_source, c.year, a.uth_admission_id, a.uth_member_id, c.admit_date, c.proc4, 4, c.icd_flag
from data_warehouse.dim_uth_admission_id a
join optum_zip.confinement c on c.conf_id=a.admission_id_src
where a.data_source='optz';

/*
SCRATCH SPACE
*/