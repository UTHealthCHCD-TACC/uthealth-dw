-- Create target table for DX records

drop table if exists dev.claim_detail_proc_mdcr;

create table dev.claim_detail_proc_mdcr (
id bigserial NOT NULL,
	uth_detail_id int8,
	uth_claim_id numeric,
	claim_sequence_number int,
	proc_cd varchar,
	proc_position int,
	"date" date
) 
WITH (appendonly=true, orientation=column)
distributed randomly;

--Greenplum performance optimization for serial/sequence
alter sequence dev.claim_detail_proc_mdcr_id_seq cache 100;

-- Insert Inpatient proc codes
insert into dev.claim_detail_proc_mdcr (uth_claim_id, claim_sequence_number, proc_cd, proc_position)
select sq.uth_claim_id, sq.claim_sequence_number,
sq.proc, sq.proc_position
from (
select cd.uth_claim_id, cd.claim_sequence_number,
procs.proc_position, procs.proc
from (
select bene_id, clm_id,
unnest(array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25]) as proc_position,
unnest(array[icd_prcdr_cd1,icd_prcdr_cd2,icd_prcdr_cd3,icd_prcdr_cd4,icd_prcdr_cd5,icd_prcdr_cd6,icd_prcdr_cd7,icd_prcdr_cd8,
icd_prcdr_cd9,icd_prcdr_cd10,icd_prcdr_cd11,icd_prcdr_cd12,icd_prcdr_cd13,icd_prcdr_cd14,icd_prcdr_cd15,icd_prcdr_cd16,icd_prcdr_cd17,
icd_prcdr_cd18,icd_prcdr_cd19,icd_prcdr_cd20,icd_prcdr_cd21,icd_prcdr_cd22,icd_prcdr_cd23,icd_prcdr_cd24,icd_prcdr_cd25]) as proc
from medicare_texas.inpatient_base_claims_k) procs
join data_warehouse.claim_detail cd on cd.member_id_src = procs.bene_id and cd.claim_id_src = procs.clm_id
where proc is not null) sq


-- Outpatient Proc codes

insert into dev.claim_detail_proc_mdcr (uth_claim_id, claim_sequence_number, proc_cd, proc_position)
select sq.uth_claim_id, sq.claim_sequence_number,
sq.proc, sq.proc_position
from (
select cd.uth_claim_id, cd.claim_sequence_number,
procs.proc_position, procs.proc
from (
select bene_id, clm_id,
unnest(array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25]) as proc_position,
unnest(array[icd_prcdr_cd1,icd_prcdr_cd2,icd_prcdr_cd3,icd_prcdr_cd4,icd_prcdr_cd5,icd_prcdr_cd6,icd_prcdr_cd7,icd_prcdr_cd8,
icd_prcdr_cd9,icd_prcdr_cd10,icd_prcdr_cd11,icd_prcdr_cd12,icd_prcdr_cd13,icd_prcdr_cd14,icd_prcdr_cd15,icd_prcdr_cd16,icd_prcdr_cd17,
icd_prcdr_cd18,icd_prcdr_cd19,icd_prcdr_cd20,icd_prcdr_cd21,icd_prcdr_cd22,icd_prcdr_cd23,icd_prcdr_cd24,icd_prcdr_cd25]) as proc
from medicare_texas.outpatient_base_claims_k) procs
join data_warehouse.claim_detail cd on cd.member_id_src = procs.bene_id and cd.claim_id_src = procs.clm_id
where proc is not null) sq

-- Load into DW

insert into data_warehouse.claim_icd_proc (data_source, "year", uth_member_id, uth_claim_id, claim_sequence_number, "date", proc_cd, proc_position, icd_type)
select 'mcrt', cd."year", cd.uth_member_id, cd.uth_claim_id, cdpm.claim_sequence_number, cdpm."date", cdpm.proc_cd, cdpm.proc_position, null
from dev.claim_detail_proc_mdcr cdpm 
join data_warehouse.claim_detail cd on cdpm.uth_claim_id = cd.uth_claim_id;
