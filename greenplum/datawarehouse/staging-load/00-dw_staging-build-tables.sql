

---(2) drop existing tables
drop table if exists dw_staging.member_enrollment_monthly;
drop table if exists dw_staging.member_enrollment_yearly;
drop table if exists dw_staging.medicaid_program_enrollment;
drop table if exists dw_staging.claim_header;
drop table if exists dw_staging.claim_detail;
drop table if exists dw_staging.claim_diag;
drop table if exists dw_staging.claim_icd_proc;
drop table if exists dw_staging.pharmacy_claims;



--enrollment yearly
create table dw_staging.member_enrollment_yearly 
(like data_warehouse.member_enrollment_yearly including defaults) 
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
distributed by (uth_member_id)
partition by list(data_source)
 (partition optz values ('optz'),
  partition optd values ('optd'),
  partition truv values ('truv'),
  partition mdcd values ('mdcd'),
  partition mcrt values ('mcrt'),
  partition mcrn values ('mcrn')
 )
;


--claim header
create table dw_staging.claim_header
(like data_warehouse.claim_header including defaults) 
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
distributed by (uth_member_id)
partition by list(data_source)
 (partition optz values ('optz'),
  partition optd values ('optd'),
  partition truv values ('truv'),
  partition mdcd values ('mdcd'),
  partition mcrt values ('mcrt'),
  partition mcrn values ('mcrn')
 )
; 



--claim diag
create table dw_staging.claim_diag
(like data_warehouse.claim_diag including defaults) 
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
distributed by (uth_member_id)
partition by list(data_source)
 (partition optz values ('optz'),
  partition optd values ('optd'),
  partition truv values ('truv'),
  partition mdcd values ('mdcd'),
  partition mcrt values ('mcrt'),
  partition mcrn values ('mcrn')
 )
;


--claim icd proc 
create table dw_staging.claim_icd_proc
(like data_warehouse.claim_icd_proc including defaults) 
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
distributed by (uth_member_id)
partition by list(data_source)
 (partition optz values ('optz'),
  partition optd values ('optd'),
  partition truv values ('truv'),
  partition mdcd values ('mdcd'),
  partition mcrt values ('mcrt'),
  partition mcrn values ('mcrn')
 )
;


--pharmacy claims 
create table dw_staging.pharmacy_claims 
(like data_warehouse.pharmacy_claims including defaults) 
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
distributed by (uth_member_id)
partition by list(data_source)
 (partition optz values ('optz'),
  partition optd values ('optd'),
  partition truv values ('truv'),
  partition mdcd values ('mdcd'),
  partition mcrt values ('mcrt'),
  partition mcrn values ('mcrn')
 )
;



--(4)
---enrollment monthly - adding row_number sequence

create table dw_staging.member_enrollment_monthly  
(like data_warehouse.member_enrollment_monthly including defaults) 
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
distributed by (uth_member_id)
partition by list(data_source)
 (partition optz values ('optz'),
  partition optd values ('optd'),
  partition truv values ('truv'),
  partition mdcd values ('mdcd'),
  partition mcrt values ('mcrt'),
  partition mcrn values ('mcrn')
 )
;

alter table  dw_staging.member_enrollment_monthly add column row_id bigserial;
alter sequence dw_staging.member_enrollment_monthly_row_id_seq cache 200;


--(4)
---create a copy of production claim detail table - adding row_number sequence


create table dw_staging.claim_detail
(like data_warehouse.claim_detail including defaults) 
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
distributed by (uth_member_id)
partition by list(data_source)
 (partition optz values ('optz'),
  partition optd values ('optd'),
  partition truv values ('truv'),
  partition mdcd values ('mdcd'),
  partition mcrt values ('mcrt'),
  partition mcrn values ('mcrn')
 )
;

---(5) 
alter table dw_staging.member_enrollment_monthly owner to uthealth_dev;
alter table dw_staging.member_enrollment_yearly owner to uthealth_dev;
alter table dw_staging.claim_header owner to uthealth_dev;
alter table dw_staging.claim_detail owner to uthealth_dev;
alter table dw_staging.claim_diag owner to uthealth_dev;
alter table dw_staging.claim_icd_proc owner to uthealth_dev;
alter table dw_staging.pharmacy_claims owner to uthealth_dev;
---/(5)

---(6) 

vacuum full dw_staging.member_enrollment_monthly;
vacuum full dw_staging.member_enrollment_yearly;
vacuum full dw_staging.claim_header;
vacuum full dw_staging.claim_detail;
vacuum full dw_staging.claim_diag;
vacuum full dw_staging.claim_icd_proc;
vacuum full dw_staging.pharmacy_claims;


analyze dw_staging.member_enrollment_monthly;
analyze dw_staging.member_enrollment_yearly;
analyze dw_staging.claim_header;
analyze dw_staging.claim_detail;
analyze dw_staging.claim_diag;
analyze dw_staging.claim_icd_proc;
analyze dw_staging.pharmacy_claims;

grant select on dw_staging.claim_detail to uthealth_analyst;
grant select on dw_staging.claim_header to uthealth_analyst;
grant select on dw_staging.member_enrollment_monthly  to uthealth_analyst;
grant select on dw_staging.member_enrollment_yearly  to uthealth_analyst;
grant select on dw_staging.claim_diag to uthealth_analyst;
grant select on dw_staging.claim_icd_proc to uthealth_analyst;
grant select on dw_staging.pharmacy_claims to uthealth_analyst;


