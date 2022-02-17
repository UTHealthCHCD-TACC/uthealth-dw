/* ******************************************************************************************************
 *  Script to create condition flags for dementia (dem)
 * Logic: https://docs.google.com/spreadsheets/d/1MwvlsStAPe0sTtU-uHxoGg7y2a72a-vN/edit#gid=236213189
 * 1. Find minimum year for given person in claim_diags
 * 2. Carry forward for every future year for user
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wallingTACC  || 2/16/2022   || script created
 * ******************************************************************************************************
 */

/*
 * Basic information
 */
 select * from conditions.condition_desc where condition_cd = 'dem';
 select * from conditions.condition_ndc where condition_cd = 'dem';
 select * from conditions.codeset where condition_cd='dem';
 select * from conditions.diagnosis_codes_list dcl where condition_cd='dem';


--Create results table
drop table dev.dem_person_prof;
create table dev.dem_person_prof (like conditions.person_profile_stage)
with(appendonly=true, orientation=column)
distributed by (uth_member_id);

--Check Diags/ICD codes
insert into dev.dem_person_prof (data_source, uth_member_id, condition_cd, carry_forward, year)
select ch.data_source, cd.uth_member_id, d.condition_cd, d.carry_forward, min(ch.year) as year
from data_warehouse.claim_diag cd
join data_warehouse.claim_header ch on ch.uth_member_id = cd.uth_member_id and ch.uth_claim_id = cd.uth_claim_id
join conditions.diagnosis_codes_list d on cd.diag_cd = d.diag_cd
where d.condition_cd = 'dem'
group by 1, 2, 3, 4;

--Carry forward to future years
insert into dev.dem_person_prof (data_source, uth_member_id, condition_cd, carry_forward, year)
select dpp.data_source, dpp.uth_member_id, dpp.condition_cd, dpp.carry_forward, mey.year
from dev.dem_person_prof dpp
join data_warehouse.member_enrollment_yearly mey on dpp.uth_member_id=mey.uth_member_id and mey.year > dpp.year;

/*
 * Scratch
 */
SELECT count(distinct uth_member_id)
FROM dev.dem_person_prof

select count(*)
from data_warehouse.dim_uth_member_id;
