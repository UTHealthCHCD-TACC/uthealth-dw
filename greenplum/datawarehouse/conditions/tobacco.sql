/* ******************************************************************************************************
 *  Script to create condition flags for tobacco use (tob)
 * Logic: https://docs.google.com/spreadsheets/d/1MwvlsStAPe0sTtU-uHxoGg7y2a72a-vN/edit#gid=384457528
 * 1. restrict to members aged 13+
 * 2. check diags for year
 * 3. check procedures
 * 4. check rx 
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wallingTACC  || 2/16/2022   || script created
 * ******************************************************************************************************
 */

/*
 * Basic information
 */
 select * from conditions.condition_desc where condition_cd = 'tob';
 select * from conditions.codeset where condition_cd='tob';
 select * from conditions.diagnosis_codes_list dcl whre condition_cd='tob';
 select * from conditions.condition_ndc where condition_cd = 'tob';

/*
 * Core script
 */
--Create results table
drop table dev.tob_person_prof;
create table dev.tob_person_prof (like conditions.person_profile_stage)
with(appendonly=true, orientation=column)
distributed by (uth_member_id);

--Check Diags/ICD codes
insert into dev.tob_person_prof (data_source, uth_member_id, condition_cd, carry_forward, year)
select distinct ch.data_source, cd.uth_member_id, d.condition_cd, d.carry_forward, mey.year
from data_warehouse.member_enrollment_yearly mey
join data_warehouse.claim_header ch on ch.uth_member_id = mey.uth_member_id and ch.year=mey.year
join data_warehouse.claim_diag cd on cd.uth_member_id = ch.uth_member_id and cd.uth_claim_id = ch.uth_claim_id 
join conditions.diagnosis_codes_list d on d.diag_cd = cd.diag_cd
where d.condition_cd = 'tob'
and mey.age_derived >= 13;

--Check Details CPT/HCPCS codes
insert into dev.tob_person_prof (data_source, uth_member_id, condition_cd, carry_forward, year)
select distinct ch.data_source, cd.uth_member_id, d.condition_cd, 0, mey.year --Manual 0 prevents additional join
from data_warehouse.member_enrollment_yearly mey
join data_warehouse.claim_header ch on ch.uth_member_id = mey.uth_member_id and ch.year=mey.year
join data_warehouse.claim_detail cd on cd.uth_member_id = ch.uth_member_id and cd.uth_claim_id = ch.uth_claim_id 
join conditions.codeset d on d.cd_value=cd.cpt_hcpcs_cd 
left outer join dev.tob_person_prof tpp on ch.uth_member_id = tpp.uth_member_id and ch.year = tpp.year --Exclude alread added members
where d.condition_cd = 'tob'
AND d.cd_type in ('CPT', 'HCPCS')
and mey.age_derived >= 13
and tpp.uth_member_id is null;


--Check RX
insert into dev.tob_person_prof (data_source, uth_member_id, condition_cd, carry_forward, year)
select distinct pc.data_source, pc.uth_member_id, ndc.condition_cd, 0, pc.year --Manual 0 prevents another join
from data_warehouse.member_enrollment_yearly mey
join data_warehouse.pharmacy_claims pc on pc.uth_member_id = mey.uth_member_id and pc.uth_member_id = mey.year
join conditions.condition_ndc ndc on pc.ndc=ndc.ndc
left outer join dev.tob_person_prof tpp on mey.uth_member_id = tpp.uth_member_id and mey.year = tpp.year --Exclude alread added members
where ndc.condition_cd = 'tob'
and mey.age_derived >= 13
and tpp.uth_member_id is null;

/*
 * Scratch
 */

select distinct cd_type
from conditions.codeset;

select distinct claim_type
from dev.tob_claim_header tch ;

with t as (
select cd_value 
from conditions.codeset c 
where cd_type not like 'ICD%' and cd_type not like 'DC%'
group by 1
having count(distinct cd_type) > 1 
)
select c.cd_value, cd_type
from conditions.codeset c
join t on t.cd_value=c.cd_value
order by 1, 2;

--Relevant codes
select *
from conditions.diagnosis_codes_list
where condition_cd = 'tob';

select data_source, condition_cd, count(*)
from conditions.person_profile_stage
--where year is null
group by 1, 2;

select count(*)
from data_warehouse.member_enrollment_yearly mey 
where year is null;

select * from conditions.condition_desc 
where additional_logic_flag = '1'

--David will start working on logic for the following conditions: del, dem, lb, lbpreg, opi, preg, and tob
select c.condition_cd, count(*)
from conditions.condition_desc c
join conditions.person_prof p on c.condition_cd = p.condition_cd 
where c.additional_logic_flag = '1'
group by 1
order by 1;

select distinct condition_cd 
from conditions.codeset;