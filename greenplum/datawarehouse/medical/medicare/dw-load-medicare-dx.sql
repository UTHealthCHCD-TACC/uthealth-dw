-- Create target table for DX records

drop table if exists dev.claim_detail_diag_mdcr;

create table dev.claim_detail_diag_mdcr (
id bigserial NOT NULL,
	uth_claim_id numeric,
	uth_member_id int8,
	claim_sequence_number int,
	diag_cd varchar,
	diag_position int,
	poa_src varchar,
	"date" date,
	"year" int2
) 
WITH (appendonly=true, orientation=column)
distributed randomly;

-- Insert Inpatient DX codes
insert into dev.claim_detail_diag_mdcr (uth_claim_id, uth_member_id, claim_sequence_number, diag_cd, diag_position, poa_src, "date", "year")
select sq.uth_claim_id, sq.uth_member_id, sq.claim_sequence_number,
sq.dx, sq.dx_position, sq.poa, "date", "year"
from (

select DISTINCT ch.uth_claim_id, ch.uth_member_id, null::int as claim_sequence_number,
dxs.dx_position, dxs.dx, dxs.poa, dxs,"date", dxs."year"
from (select bene_id, clm_id, clm_from_dt::date as "date", LEFT(clm_from_dt, 4)::int as "year",
unnest(array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25]) as dx_position,
unnest(array[icd_dgns_cd1,icd_dgns_cd2,icd_dgns_cd3,icd_dgns_cd4,icd_dgns_cd5,icd_dgns_cd6,icd_dgns_cd7,icd_dgns_cd8,
icd_dgns_cd9,icd_dgns_cd10,icd_dgns_cd11,icd_dgns_cd12,icd_dgns_cd13,icd_dgns_cd14,icd_dgns_cd15,icd_dgns_cd16,icd_dgns_cd17,
icd_dgns_cd18,icd_dgns_cd19,icd_dgns_cd20,icd_dgns_cd21,icd_dgns_cd22,icd_dgns_cd23,icd_dgns_cd24,icd_dgns_cd25]) as dx,
unnest (array[clm_poa_ind_sw1,clm_poa_ind_sw2,clm_poa_ind_sw3,clm_poa_ind_sw4,clm_poa_ind_sw5,clm_poa_ind_sw6,clm_poa_ind_sw7,
clm_poa_ind_sw8,clm_poa_ind_sw9,clm_poa_ind_sw10,clm_poa_ind_sw11,clm_poa_ind_sw12,clm_poa_ind_sw13,clm_poa_ind_sw14,
clm_poa_ind_sw15,clm_poa_ind_sw16,clm_poa_ind_sw17,clm_poa_ind_sw18,clm_poa_ind_sw19,clm_poa_ind_sw20,clm_poa_ind_sw21,
clm_poa_ind_sw22,clm_poa_ind_sw23,clm_poa_ind_sw24,clm_poa_ind_sw25]) as poa
from medicare.inpatient_base_claims_k) dxs
join data_warehouse.claim_header ch on ch.member_id_src = dxs.bene_id and ch.claim_id_src = dxs.clm_id
where dx is not null) sq


-- Outpatient DX codes

insert into dev.claim_detail_diag_mdcr (uth_claim_id, uth_member_id, claim_sequence_number, diag_cd, diag_position, "date", "year")
select sq.uth_claim_id, sq.uth_member_id, sq.claim_sequence_number,
sq.dx, sq.dx_position, "date", "year"
from (

select distinct ch.uth_claim_id, ch.uth_member_id, null::int as claim_sequence_number,
dxs.dx_position, dxs.dx, dxs,"date", dxs."year"
from (
select bene_id, clm_id, clm_from_dt::date as "date", LEFT(clm_from_dt, 4)::int as "year",
unnest(array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25]) as dx_position,
unnest(array[icd_dgns_cd1,icd_dgns_cd2,icd_dgns_cd3,icd_dgns_cd4,icd_dgns_cd5,icd_dgns_cd6,icd_dgns_cd7,icd_dgns_cd8,
icd_dgns_cd9,icd_dgns_cd10,icd_dgns_cd11,icd_dgns_cd12,icd_dgns_cd13,icd_dgns_cd14,icd_dgns_cd15,icd_dgns_cd16,icd_dgns_cd17,
icd_dgns_cd18,icd_dgns_cd19,icd_dgns_cd20,icd_dgns_cd21,icd_dgns_cd22,icd_dgns_cd23,icd_dgns_cd24,icd_dgns_cd25]) as dx
from medicare.outpatient_base_claims_k) dxs
join data_warehouse.claim_header ch on ch.member_id_src = dxs.bene_id and ch.claim_id_src = dxs.clm_id
where dx is not null) sq

-- DME DX codes

insert into dev.claim_detail_diag_mdcr (uth_claim_id, uth_member_id, claim_sequence_number, diag_cd, diag_position, "date", "year")
select sq.uth_claim_id, sq.uth_member_id, sq.claim_sequence_number,
sq.dx, sq.dx_position, "date", "year"
from (

select distinct ch.uth_claim_id, ch.uth_member_id, null::int as claim_sequence_number,
dxs.dx_position, dxs.dx, dxs,"date", dxs."year"
from (
select bene_id, clm_id, clm_from_dt::date as "date", LEFT(clm_from_dt, 4)::int as "year",
unnest(array[1,2,3,4,5,6,7,8,9,10,11,12/*,13,14,15,16,17,18,19,20,21,22,23,24,25*/]) as dx_position,
unnest(array[icd_dgns_cd1,icd_dgns_cd2,icd_dgns_cd3,icd_dgns_cd4,icd_dgns_cd5,icd_dgns_cd6,icd_dgns_cd7,icd_dgns_cd8,
icd_dgns_cd9,icd_dgns_cd10,icd_dgns_cd11,icd_dgns_cd12/*,icd_dgns_cd13,icd_dgns_cd14,icd_dgns_cd15,icd_dgns_cd16,icd_dgns_cd17,
icd_dgns_cd18,icd_dgns_cd19,icd_dgns_cd20,icd_dgns_cd21,icd_dgns_cd22,icd_dgns_cd23,icd_dgns_cd24,icd_dgns_cd25*/]) as dx
from medicare.dme_claims_k) dxs
join data_warehouse.claim_header ch on ch.member_id_src = dxs.bene_id and ch.claim_id_src = dxs.clm_id
where dx is not null) sq


-- Confirm count

select count(*)
from dev.claim_detail_diag_mdcr;

-- Load into DW

insert into data_warehouse.claim_diag (data_source, "year", uth_member_id, uth_claim_id, claim_sequence_number, "date", diag_cd, diag_position, icd_type, poa_src)
select 'mdcr', cddm."year", cddm.uth_member_id, cddm.uth_claim_id, cddm.claim_sequence_number, cddm."date", cddm.diag_cd, cddm.diag_position, null, cddm.poa_src 
from dev.claim_detail_diag_mdcr cddm;


-- Confirm load

select "year", count(*)
from data_warehouse.claim_diag cd 
where cd.data_source = 'mdcr'
group by "year";
