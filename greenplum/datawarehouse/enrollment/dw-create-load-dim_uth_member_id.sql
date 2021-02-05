--- this table contains a map of all source members numbers mapped to uth_member_id values

drop table if exists data_warehouse.dim_uth_member_id;

create table data_warehouse.dim_uth_member_id (
	uth_member_id bigserial,
	member_id_src text, 
	data_source char(4), 
	unique ( uth_member_id)
) distributed by (uth_member_id);

---
select * from medicare_national.mbsf_abcd_summary mas where bene_id = 'ggggggjyygjgnBu'

select * from optum_dod.mbr_co_enroll where patid = 33008549751

select *--count(*), data_source 
from data_warehouse.dim_uth_member_id 
where data_source = 'mcrn' and uth_member_id not in (select uth_member_id from data_warehouse.member_enrollment_yearly)

group by data_source 
;

select count(*), data_source 
from data_warehouse.dim_uth_member_id 
group by data_source;


select count(distinct uth_member_id), data_source 
from data_warehouse.member_enrollment_yearly 
group by data_source 


select count(distinct uth_member_id), data_source 
from data_warehouse.member_enrollment_monthly 
group by data_source 
;

alter sequence data_warehouse.dim_uth_member_id_uth_member_id_seq restart with 100000000; 
                                                                           
alter sequence data_warehouse.dim_uth_member_id_uth_member_id_seq cache 200;

vacuum analyze data_warehouse.dim_uth_member_id;


------ load dim_uth_member_id

vacuum analyze optum_dod.mbr_enroll;

select count(distinct patid) from optum_dod.mbr_enroll

--Optum DoD 
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
	select distinct patid as v_member_id, 'optd' as v_raw_data
	from optum_dod.mbr_enroll
	 left outer join data_warehouse.dim_uth_member_id b 
	              on b.data_source = 'optd'
	             and b.member_id_src = patid::text
	where b.member_id_src is null 	
)
select v_member_id, v_raw_data, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member 
;


vacuum analyze optum_dod.mbr_enroll;

select count(distinct patid) from optum_dod.mbr_enroll

---Optum Zip 
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
	select distinct patid as v_member_id, 'optz' as v_raw_data
	from optum_dod.mbr_enroll
	 left outer join data_warehouse.dim_uth_member_id b 
              on b.data_source = 'optz'
             and b.member_id_src = patid::text
	where b.member_id_src is null 
)
select v_member_id, v_raw_data, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member 
;


select * from data_warehouse.dim_uth_member_id where member_id_src is null;

vacuum analyze truven.ccaet;

select count(distinct enrolid) from truven.ccaet;

vacuum analyze data_warehouse.dim_uth_member_id

update data_warehouse.dim_uth_member_id set data_source = 'truv' where data_source = 'trv'

---Truven Commercial  
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
	select distinct enrolid as v_member_id, 'truv' as v_raw_data
	from truven.ccaet
	 left outer join data_warehouse.dim_uth_member_id b 
          on b.data_source = 'truv'
         and b.member_id_src = enrolid::text
	where b.member_id_src is null 	
)
select v_member_id, v_raw_data, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member 
;


vacuum analyze truven.mdcrt;

select count(distinct enrolid) from truven.mdcrt;

---Truven Medicare 
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
	select distinct enrolid as v_member_id, 'truv' as v_raw_data
	from truven.mdcrt
	 left outer join data_warehouse.dim_uth_member_id b 
      on b.data_source = 'truv'
     and b.member_id_src = enrolid::text
where b.member_id_src is null 
)
select v_member_id, v_raw_data, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member 
;



update  medicare_texas.mbsf_abcd_summary 
set bene_enrollmt_ref_yr = trunc(bene_enrollmt_ref_yr::numeric,0)::text
where bene_enrollmt_ref_yr = '2016.0'


--- Medicare Texas
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
	select distinct bene_id as v_member_id, 'mcrt' as v_raw_data
	from medicare_texas.mbsf_abcd_summary
	 left outer join data_warehouse.dim_uth_member_id b 
      on b.data_source = 'mcrt'
     and b.member_id_src = bene_id::text
    where b.member_id_src is null 
)
select v_member_id, v_raw_data, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member
;

update data_warehouse.dim_uth_member_id set data_source = 'mcrt' where data_source = 'mdcr';

select count(*), count(distinct bene_id), mas.bene_enrollmt_ref_yr  
from medicare_texas.mbsf_abcd_summary mas 
group by mas.bene_enrollmt_ref_yr

select count(*) from data_warehouse.dim_uth_member_id where data_source = 'mcrt';

select count(distinct bene_id) from medicare_texas.mbsf_abcd_summary mas ;



--- Medicare National
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
	select distinct bene_id as v_member_id, 'mcrn' as v_raw_data
	from medicare_national.mbsf_abcd_summary
	 left outer join data_warehouse.dim_uth_member_id b 
      on b.data_source = 'mcrn'
     and b.member_id_src = bene_id::text
    where b.member_id_src is null 
)
select v_member_id, v_raw_data, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member
;

select count(*) from data_warehouse.dim_uth_member_id where data_source = 'mcrn';

select count(distinct bene_id) from medicare_texas.mbsf_abcd_summary mas ;

---******************************** Pharmacy tables---------*****************************


--medicare rx
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
    select distinct bene_id as v_member_id
    from medicare_texas.pde_file
    left outer join data_warehouse.dim_uth_member_id 
      on data_source = 'mdcr'
     and member_id_src = bene_id::text 
    where member_id_src is null 
) 
select v_member_id, 'mdcr', nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member
;


--medicare National rx
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
    select distinct bene_id as v_member_id
    from medicare_texas.pde_file
    left outer join data_warehouse.dim_uth_member_id 
      on data_source = 'mcrn'
     and member_id_src = bene_id::text 
    where member_id_src is null 
) 
select v_member_id, 'mcrn', nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member
;


--trvc rx
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
    select distinct enrolid as v_member_id
    from truven.ccaed 
    left outer join data_warehouse.dim_uth_member_id 
      on data_source = 'truv'
     and member_id_src = enrolid::text 
    where member_id_src is null 
) 
select v_member_id, 'truv', nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member
;

--trvm rx
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
    select distinct enrolid as v_member_id
    from truven.mdcrd 
    left outer join data_warehouse.dim_uth_member_id 
      on data_source = 'truv'
     and member_id_src = enrolid::text 
    where member_id_src is null 
) 
select v_member_id, 'truv', nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member
;


--optd rx 
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
    select distinct patid as v_member_id
    from optum_dod.rx
    left outer join data_warehouse.dim_uth_member_id 
      on data_source = 'optd'
     and member_id_src = patid::text 
    where member_id_src is null 
) 
select v_member_id, 'optd', nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member
;


--optz rx 
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
    select distinct patid as v_member_id
    from optum_dod.rx
    left outer join data_warehouse.dim_uth_member_id 
      on data_source = 'optz'
     and member_id_src = patid::text 
    where member_id_src is null 
) 
select v_member_id, 'optz', nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member
;


----Validate
vacuum analyze data_warehouse.dim_uth_member_id;

select count(*), data_source 
from data_warehouse.dim_uth_member_id
group by data_source ;


