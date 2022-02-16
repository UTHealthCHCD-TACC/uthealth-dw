

/*
 * Dev Setup - randomly sample 100k members aged 15+
 */
--Create a randome selection of uth_member_ids
drop table dev.preg_sample_uth_member_ids;
create table dev.preg_sample_uth_member_ids (uth_member_id int8)
with(appendonly=true, orientation=column)
distributed by (uth_member_id);

insert into dev.preg_sample_uth_member_ids
select * from 
  (select distinct uth_member_id from data_warehouse.member_enrollment_yearly) table_alias
ORDER BY random()
limit 100000; --10k random members

-- Create smaller versions of large tables for dev
drop table dev.preg_claim_header;
create table dev.preg_claim_header (like data_warehouse.claim_header)
with(appendonly=true, orientation=column)
distributed by (uth_member_id);

insert into dev.preg_claim_header
select ch.* from data_warehouse.claim_header ch
join dev.preg_sample_uth_member_ids i on ch.uth_member_id = i.uth_member_id;

drop table dev.preg_claim_diag;
create table dev.preg_claim_diag (like data_warehouse.claim_diag)
with(appendonly=true, orientation=column)
distributed by (uth_member_id);

insert into dev.preg_claim_diag
select ch.* from data_warehouse.claim_diag ch
join dev.preg_sample_uth_member_ids i on ch.uth_member_id = i.uth_member_id;

--Create results table
drop table dev.preg_person_prof;
create table dev.preg_person_prof (like conditions.person_prof)
with(appendonly=true, orientation=column)
distributed by (uth_member_id);

--Check Diags
insert into dev.preg_person_prof (data_source, uth_member_id, condition_cd, carry_forward, year)
select distinct ch.data_source, cd.uth_member_id, d.condition_cd, d.carry_forward, ch.year
from dev.preg_claim_diag cd
join dev.preg_claim_header ch on ch.uth_member_id = cd.uth_member_id and ch.uth_claim_id = cd.uth_claim_id
join conditions.diagnosis_codes_list d on cd.diag_cd = d.diag_cd
where d.condition_cd = 'preg';

/*
 * Scratch
 */

select *
from dev.preg_person_prof ppp 
order by uth_member_id 
limit 10;

select *
from dev.preg_claim_diag pcd 
where uth_member_id =358403757;
--Relevant codes
select *
from conditions.diagnosis_codes_list
where condition_cd = 'preg';

select data_source, condition_cd, count(*)
from conditions.person_prof
where year is null
group by 1, 2;

select count(*)
from data_warehouse.member_enrollment_yearly mey 
where year is null;

select * from conditions.condition_desc 
where additional_logic_flag = '1'

select c.condition_cd, count(*)
from conditions.condition_desc c
join conditions.person_prof p on c.condition_cd = p.condition_cd 
where c.additional_logic_flag = '1'
group by 1
order by 1;

select distinct condition_cd 
from conditions.codeset;