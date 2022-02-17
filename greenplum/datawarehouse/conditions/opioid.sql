/* ******************************************************************************************************
 *  Script to create condition flags for opioid dependency (opi)
 * Logic: https://docs.google.com/spreadsheets/d/1MwvlsStAPe0sTtU-uHxoGg7y2a72a-vN/edit#gid=2111897454
 * 1. exclude cancer patients, so process that condition first
 * 2. check claim diag codes
 * 3. check rx claims for ndc of opiates with >= 90 total supply in the given year
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wallingTACC  || 2/16/2022   || script created
 * ******************************************************************************************************
 */


/*
 * Basic information
 */
 select * from conditions.condition_desc where condition_cd = 'opi';
 select * from conditions.condition_ndc where condition_cd = 'opi';
 select * from conditions.codeset where condition_cd='opi';
 select * from conditions.diagnosis_codes_list dcl where condition_cd='opi';


--Create results table
drop table dev.opi_person_prof;
create table dev.opi_person_prof (like conditions.person_profile_stage)
with(appendonly=true, orientation=column)
distributed by (uth_member_id);

--Check Diags/ICD codes
insert into dev.opi_person_prof (data_source, uth_member_id, condition_cd, carry_forward, year)
select distinct ch.data_source, cd.uth_member_id, d.condition_cd, d.carry_forward, ch.year
from data_warehouse.claim_diag cd
join data_warehouse.claim_header ch on ch.uth_member_id = cd.uth_member_id and ch.uth_claim_id = cd.uth_claim_id
join conditions.diagnosis_codes_list d on cd.diag_cd = d.diag_cd
--Exclude cancer for given year
left outer join conditions.conditions_member_enrollment_yearly cancer on ch.uth_member_id = cancer.uth_member_id and ch.year=cancer."year" and cancer.ca = 1
where d.condition_cd = 'opi'
and cancer.uth_member_id is null;

--Check RX records, for users with matching ndc and total supply >= 90 days
with yearly_member_supply as (
select pc.data_source, pc.uth_member_id, pc."year", ndc.condition_cd, cd2.carry_forward, sum(pc.days_supply) as total_supply
from data_warehouse.pharmacy_claims pc
join conditions.condition_ndc ndc on pc.ndc = ndc.ndc 
join conditions.condition_desc cd2 on ndc.condition_cd = cd2.condition_cd 
where ndc.condition_cd = 'opi'
group by 1, 2, 3, 4, 5
having sum(pc.days_supply)>=90
)
insert into dev.opi_person_prof (data_source, uth_member_id, condition_cd, carry_forward, year)
select distinct yms.data_source, yms.uth_member_id, yms.condition_cd, yms.carry_forward, yms.year
from yearly_member_supply yms
--Exclude cancer
left outer join conditions.conditions_member_enrollment_yearly cancer on yms.uth_member_id = cancer.uth_member_id and yms.year=cancer."year" and cancer.ca = 1
--Exclude those already added
left outer join dev.opi_person_prof pp on yms.uth_member_id = pp.uth_member_id and yms.year=pp.year
where pp.uth_member_id is null
;


/*
 * Scratch
 */
SELECT count(distinct uth_member_id)
FROM dev.opi_person_prof
ORDER BY uth_member_id, YEAR;


/*
 * Scratch
 */
