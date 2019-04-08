--Main table
drop table data_warehouse.patient;
create table data_warehouse.patient (
mbr_id bigint, gndr_cd char(1), mbr_dob smallint, source char(2)
) 
WITH (appendonly=true, orientation=column)
distributed randomly;

--Optum load
insert into data_warehouse.patient
select patid, gdr_cd, yrdob, 'od'
from optum_dod.member;

-- Are there dulicates
select patid, count(*)
from optum_dod.member
group by 1
order by 2 desc
limit 10;

select * from optum_dod."member"
where patid=33040008958
order by eligeff, eligend;

-- Different family_id???  Yes, 33046809670 has 20 distinct records
select patid, count(distinct family_id)
from optum_dod."member"
group by 1
having count(distinct family_id) > 1
order by 2 desc
limit 10;

-- Different states???  Yes, 33025997064 has 8 distinct states
select patid, count(distinct state)
from optum_dod."member"
group by 1
having count(distinct state) > 1
order by 2 desc
limit 10;

-- Different date of birth???  No
select patid, count(distinct yrdob)
from optum_dod."member"
group by 1
having count(distinct yrdob) > 1
order by 2 desc
limit 10;

--Truven load
insert into data_warehouse.patient
select enrolid, 'M', dobyr, 't'
from truven.ccaet
where sex=1;

insert into data_warehouse.patient
select enrolid, 'F', dobyr, 't'
from truven.ccaet
where sex=2;

--Duplicate enrolid??? Yes, 
select enrolid, count(*)
from truven.ccaet
group by 1
order by 2 desc
limit 10;

--Different dobyr? No
select enrolid, count(distinct dobyr)
from truven.ccaet
group by 1
having count(distinct dobyr)>1
order by 2 desc
limit 10;

--Different sex? No
select enrolid, count(distinct sex)
from truven.ccaet
group by 1
having count(distinct sex)>1
order by 2 desc
limit 10;
