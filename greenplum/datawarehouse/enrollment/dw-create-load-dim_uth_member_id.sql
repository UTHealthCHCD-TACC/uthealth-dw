--- this table contains a map of all source members numbers mapped to uth_member_id values

drop table if exists data_warehouse.dim_uth_member_id;

create table data_warehouse.dim_uth_member_id (
	uth_member_id bigserial,
	member_id_src text, 
	data_source char(4), 
	unique ( uth_member_id)
) distributed by (uth_member_id);

---




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


vacuum analyze optum_zip.mbr_enroll;

select count(distinct patid) from optum_zip.mbr_enroll

---Optum Zip 
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
	select distinct patid as v_member_id, 'optz' as v_raw_data
	from optum_zip.mbr_enroll
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

select count(distinct uth_member_id) from data_warehouse.dim_uth_member_id where data_source = 'trvc';

---Truven Commercial  
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
	select distinct enrolid as v_member_id, 'trvc' as v_raw_data
	from truven.ccaet
	 left outer join data_warehouse.dim_uth_member_id b 
          on b.data_source = 'trvc'
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
	select distinct enrolid as v_member_id, 'trvm' as v_raw_data
	from truven.mdcrt
	 left outer join data_warehouse.dim_uth_member_id b 
      on b.data_source = 'trvm'
     and b.member_id_src = enrolid::text
where b.member_id_src is null 
)
select v_member_id, v_raw_data, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member 
;



--- Medicare
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
	select distinct bene_id as v_member_id, 'mdcr' as v_raw_data
	from medicare.mbsf_abcd_summary
	 left outer join data_warehouse.dim_uth_member_id b 
      on b.data_source = 'mdcr'
     and b.member_id_src = bene_id::text
    where b.member_id_src is null 
)
select v_member_id, v_raw_data, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member
;


---******************************** Pharmacy tables---------*****************************


--medicare rx
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
    select distinct bene_id as v_member_id
    from medicare.pde_file
    left outer join data_warehouse.dim_uth_member_id 
      on data_source = 'mdcr'
     and member_id_src = bene_id::text 
    where member_id_src is null 
) 
select v_member_id, 'mdcr', nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member
;


--trvc rx
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
    select distinct enrolid as v_member_id
    from truven.ccaed 
    left outer join data_warehouse.dim_uth_member_id 
      on data_source = 'trvc'
     and member_id_src = enrolid::text 
    where member_id_src is null 
) 
select v_member_id, 'trvc', nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member
;

--trvm rx
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
    select distinct enrolid as v_member_id
    from truven.mdcrd 
    left outer join data_warehouse.dim_uth_member_id 
      on data_source = 'trvm'
     and member_id_src = enrolid::text 
    where member_id_src is null 
) 
select v_member_id, 'trvm', nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
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
    from optum_zip.rx
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

select count(*), count(distinct uth_member_id) 
from data_warehouse.dim_uth_member_id;


