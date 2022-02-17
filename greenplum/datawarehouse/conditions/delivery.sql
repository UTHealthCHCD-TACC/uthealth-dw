/* ******************************************************************************************************
 *  Script to create condition flags for delivery (del)
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wallingTACC  || 2/16/2022   || script created, pending full implementation for working out details on 'maternity' conditions in general
 * ******************************************************************************************************
 */


https://docs.google.com/spreadsheets/d/1MwvlsStAPe0sTtU-uHxoGg7y2a72a-vN/edit#gid=384457528

/*
 * Basic information
 */
 select * from conditions.condition_desc where condition_cd = 'del';
 select * from conditions.codeset where condition_cd='del';
 select * from conditions.diagnosis_codes_list dcl whre condition_cd='del';


--Create results table
drop table dev.del_person_prof;
create table dev.del_person_prof (like conditions.person_prof)
with(appendonly=true, orientation=column)
distributed by (uth_member_id);

--Check Diags/ICD codes
insert into dev.del_person_prof (data_source, uth_member_id, condition_cd, carry_forward, year)
select ch.data_source, cd.uth_member_id, d.condition_cd, d.carry_forward, min(ch.year) as year
from dev.del_claim_diag cd
join dev.del_claim_header ch on ch.uth_member_id = cd.uth_member_id and ch.uth_claim_id = cd.uth_claim_id
join conditions.diagnosis_codes_list d on cd.diag_cd = d.diag_cd
where d.condition_cd = 'del'
group by 1, 2, 3, 4;

--Check Details CPT/HCPCS codes
insert into dev.del_person_prof (data_source, uth_member_id, condition_cd, carry_forward, year)
select ch.data_source, cd.uth_member_id, d.condition_cd, d.carry_forward, min(ch.year) as year
from dev.del_claim_detail cd
join dev.del_claim_header ch on ch.uth_member_id = cd.uth_member_id and ch.uth_claim_id = cd.uth_claim_id
join conditions.codeset d on cd.cpt_hcpcs_cd=d.cd_value 
where d.condition_cd = 'del'
AND d.cd_type = ''
group by 1, 2, 3, 4;


/*
 * Scratch
 */

select distinct cd_type
from conditions.codeset;

select distinct claim_type
from dev.del_claim_header tch ;

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
where condition_cd = 'del';

select data_source, condition_cd, count(*)
from conditions.person_prof
where year is null
group by 1, 2;

select count(*)
from data_warehouse.member_enrollment_yearly mey 
where year is null;

select * from conditions.condition_desc 
where additional_logic_flag = '1'

--David will start working on logic for the following conditions: del, dem, lb, lbpreg, opi, preg, and del
select c.condition_cd, count(*)
from conditions.condition_desc c
join conditions.person_prof p on c.condition_cd = p.condition_cd 
where c.additional_logic_flag = '1'
group by 1
order by 1;

select distinct condition_cd 
from conditions.codeset;