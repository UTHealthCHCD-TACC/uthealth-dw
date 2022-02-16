/*
 * Basic information
 */
 select * from conditions.condition_desc where condition_cd = 'lbpreg';
 select * from conditions.codeset where condition_cd='lbpreg';
 select * from conditions.diagnosis_codes_list dcl whre condition_cd='lbpreg';

/*
 * Dev Setup - randomly sample 100k members aged 15+
 */
--Create a randome selection of uth_member_ids
--drop table dev.lbpreg_sample_uth_member_ids;
create table dev.lbpreg_sample_uth_member_ids (uth_member_id int8)
with(appendonly=true, orientation=column)
distributed by (uth_member_id);

insert into dev.lbpreg_sample_uth_member_ids
select * from 
  (select distinct uth_member_id from data_warehouse.member_enrollment_yearly) table_alias
ORDER BY random()
limit 100000; --10k random members

-- Create smaller versions of large tables for dev
--drop table dev.lbpreg_claim_header;
create table dev.lbpreg_claim_header (like data_warehouse.claim_header)
with(appendonly=true, orientation=column)
distributed by (uth_member_id);

insert into dev.lbpreg_claim_header
select ch.* from data_warehouse.claim_header ch
join dev.lbpreg_sample_uth_member_ids i on ch.uth_member_id = i.uth_member_id;

--drop table dev.lbpreg_claim_diag;
create table dev.lbpreg_claim_diag (like data_warehouse.claim_diag)
with(appendonly=true, orientation=column)
distributed by (uth_member_id);

insert into dev.lbpreg_claim_diag
select ch.* from data_warehouse.claim_diag ch
join dev.lbpreg_sample_uth_member_ids i on ch.uth_member_id = i.uth_member_id;

--Create results table
--drop table dev.lbpreg_person_prof;
create table dev.lbpreg_person_prof (like conditions.person_prof)
with(appendonly=true, orientation=column)
distributed by (uth_member_id);

--Restrict to Females that where enrolled in first 6 months of a given year, and then continously enrolled for at least 9 months
drop table dev.lbpreg_person_prof;
create table dev.lbpreg_person_prof
as
select distinct mem1.data_source, mem1."year", mem1.uth_member_id, dcl.condition_cd, dcl.carry_forward
--from dev.lbpreg_sample_uth_member_ids m
--join data_warehouse.member_enrollment_monthly mem1 on mem1.uth_member_id=m.uth_member_id
from data_warehouse.member_enrollment_monthly mem1
join data_warehouse.member_enrollment_monthly mem2 on mem1.uth_member_id = mem2.uth_member_id 
	and mem1.month_year_id < mem2.month_year_id 
	and (mem2.month_year_id - mem1.month_year_id) = 8
	and (mem2.consecutive_enrolled_months - mem1.consecutive_enrolled_months) = 8
join data_warehouse.claim_diag cd on cd.uth_member_id=mem1.uth_member_id and extract(year from cd.from_date_of_service) = mem1.year and extract(month from cd.from_date_of_service)=right(mem1.month_year_id::text, 2)::int
join conditions.diagnosis_codes_list dcl on dcl.diag_cd = cd.diag_cd
where mem1.gender_cd = 'F'
and right(mem1.month_year_id::text, 2) in ('01', '02', '03', '04', '05', '06')
and dcl.condition_cd = 'lbpreg';

--Given our cohort, find ones who where preg in those first six month
select *
from dev.lbpreg_cohort lc 
join dev.lbpreg_claim_detail cd on cd.uth_member_id=lc.uth_member_id and cd.year = lc.year and month(cd.from_date_of_service)=right(mem1.month_year_id::text, 2)::int
join 
--Check Diags
insert into dev.lbpreg_person_prof (data_source, uth_member_id, condition_cd, carry_forward, year)
select distinct ch.data_source, cd.uth_member_id, d.condition_cd, d.carry_forward, ch.year
from dev.lbpreg_claim_diag cd
join dev.lbpreg_claim_header ch on ch.uth_member_id = cd.uth_member_id and ch.uth_claim_id = cd.uth_claim_id
join conditions.diagnosis_codes_list d on cd.diag_cd = d.diag_cd
where d.condition_cd = 'lbpreg';


/*
 * Scratch
 */
select extract(year from cd.from_date_of_service)
from dev.lbpreg_claim_diag cd 
join data_warehouse.member_enrollment_monthly mem on cd.uth_member_id = mem.uth_member_id
	and	extract(year from cd.from_date_of_service) = mem.year
--join conditions.diagnosis_codes_list dcl on dcl.condition_cd = cd.diag_cd
where mem.gender_cd = 'F'
and right(mem.month_year_id::text, 2) in ('01', '02', '03', '04', '05', '06')
--and dcl.condition_cd = 'lbpreg';

select distinct cd.diag_cd
from dev.lbpreg_claim_diag cd
order by 1;

select di
from conditions.diagnosis_codes_list dcl 
where condition_cd = 'lbpreg'
order by 1;
select *
from dev.lbpreg_person_prof ppp 
order by uth_member_id 
limit 10;

select *
from dev.lbpreg_claim_diag pcd 
where uth_member_id =358403757;
--Relevant codes
select *
from conditions.diagnosis_codes_list
where condition_cd = 'lbpreg';

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