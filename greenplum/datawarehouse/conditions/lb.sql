/*
 * Basic information
 */
 select * from conditions.condition_desc where condition_cd = 'lb';
 select * from conditions.codeset where condition_cd='del';
 select * from conditions.diagnosis_codes_list dcl whre condition_cd='del';


/*
 * Dev Setup - randomly sample 100k members aged 15+
 */
--Create a randome selection of uth_member_ids
drop table dev.tob_sample_uth_member_ids;
create table dev.tob_sample_uth_member_ids (uth_member_id int8)
with(appendonly=true, orientation=column)
distributed by (uth_member_id);

insert into dev.tob_sample_uth_member_ids
select * from 
  (select distinct uth_member_id from data_warehouse.member_enrollment_yearly where age_derived >= 15) table_alias
ORDER BY random()
limit 100000; --10k random members

-- Create smaller versions of large tables for dev
drop table dev.tob_claim_header;
create table dev.tob_claim_header (like data_warehouse.claim_header)
with(appendonly=true, orientation=column)
distributed by (uth_member_id);

insert into dev.tob_claim_header
select ch.* from data_warehouse.claim_header ch
join dev.tob_sample_uth_member_ids i on ch.uth_member_id = i.uth_member_id;

drop table dev.tob_claim_diag;
create table dev.tob_claim_diag (like data_warehouse.claim_diag)
with(appendonly=true, orientation=column)
distributed by (uth_member_id);

insert into dev.tob_claim_diag
select ch.* from data_warehouse.claim_diag ch
join dev.tob_sample_uth_member_ids i on ch.uth_member_id = i.uth_member_id;

--Create results table
drop table dev.tob_person_prof;
create table dev.tob_person_prof (like conditions.person_prof)
with(appendonly=true, orientation=column)
distributed by (uth_member_id);

--Check Diags/ICD codes
insert into dev.tob_person_prof (data_source, uth_member_id, condition_cd, carry_forward, year)
select ch.data_source, cd.uth_member_id, d.condition_cd, d.carry_forward, min(ch.year) as year
from dev.tob_claim_diag cd
join dev.tob_claim_header ch on ch.uth_member_id = cd.uth_member_id and ch.uth_claim_id = cd.uth_claim_id
join conditions.diagnosis_codes_list d on cd.diag_cd = d.diag_cd
where d.condition_cd = 'tob'
group by 1, 2, 3, 4;

--Check Details CPT/HCPCS codes
insert into dev.tob_person_prof (data_source, uth_member_id, condition_cd, carry_forward, year)
select ch.data_source, cd.uth_member_id, d.condition_cd, d.carry_forward, min(ch.year) as year
from dev.tob_claim_detail cd
join dev.tob_claim_header ch on ch.uth_member_id = cd.uth_member_id and ch.uth_claim_id = cd.uth_claim_id
join conditions.codeset d on cd.cpt_hcpcs_cd=d.cd_value 
where d.condition_cd = 'tob'
AND d.cd_type = ''
group by 1, 2, 3, 4;


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
from conditions.person_prof
where year is null
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