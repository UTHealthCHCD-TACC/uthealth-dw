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

-----**********************************************************************************************
------ load dim_uth_member_id------------------------------------------------



---optd
vacuum analyze optum_dod.mbr_enroll_r;

delete from data_warehouse.dim_uth_member_id where data_source = 'optz'

--Optum DoD load
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
	select distinct patid as v_member_id, 'optd' as v_raw_data
	from optum_dod.mbr_enroll_r 
	 left outer join data_warehouse.dim_uth_member_id b 
	              on b.data_source = 'optd'
	             and b.member_id_src = patid::text
	where b.member_id_src is null 	
)
select v_member_id, v_raw_data, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member 
;

select count(*), count(distinct uth_member_id ), count(distinct member_id_src) from data_warehouse.dim_uth_member_id where data_source = 'optd';

---cleanup
delete from data_warehouse.dim_uth_member_id mem 
    using( 
select a.uth_member_id
from data_warehouse.dim_uth_member_id a 
   left outer join optum_dod.mbr_enroll_r b 
     on a.member_id_src = b.patid::text 
where a.data_source = 'optd' 
  and b.patid is null 
 )  del 
where mem.uth_member_id = del.uth_member_id 
;


---optz
vacuum analyze optum_zip.mbr_enroll;

select count(distinct patid) from optum_zip.mbr_enroll

---Optum Zip load
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

---cleanup optz
delete from data_warehouse.dim_uth_member_id mem 
    using( 
select a.uth_member_id
from data_warehouse.dim_uth_member_id a 
   left outer join optum_zip.mbr_enroll b 
     on a.member_id_src = b.patid::text 
where a.data_source = 'optz' 
  and b.patid is null 
 )  del 
where mem.uth_member_id = del.uth_member_id 
;

select count(*), count(distinct uth_member_id) from data_warehouse.dim_uth_member_id where data_source = 'optz';


-----


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

---cleanup truven 
delete from data_warehouse.dim_uth_member_id mem 
    using(     
  with cte_truv as (   
       select distinct enrolid::text as mem_id 
             from ( select enrolid from truven.ccaet 
                      union 
                    select enrolid from truven.mdcrt 
                  ) inr 
               ) 
select a.uth_member_id
from data_warehouse.dim_uth_member_id a 
   left outer join cte_truv b 
     on a.member_id_src = b.mem_id  
where a.data_source = 'truv' 
  and b.mem_id is null 
 )  del 
where mem.uth_member_id = del.uth_member_id 
;

--validate truv
select count(distinct uth_member_id) from data_warehouse.dim_uth_member_id where data_source = 'truv';


select count(distinct enrolid) 
from ( select enrolid from truven.ccaet 
                      union 
                    select enrolid from truven.mdcrt 
                  ) inr 
;


select  count(distinct uth_member_id) from data_warehouse.member_enrollment_monthly where data_source = 'truv';


select a.*, b.* 
from data_warehouse.member_enrollment_monthly a 
  left outer join data_warehouse.dim_uth_member_id b 
     on a.uth_member_id = b.uth_member_id 
    and a.data_source = b.data_source 
where b.uth_member_id is null 
  and a.data_source = 'truv' 
;


select a.*,b.*
from data_warehouse.dim_uth_member_id a 
  left outer join data_warehouse.member_enrollment_monthly b 
     on a.uth_member_id = b.uth_member_id 
    and a.data_source = b.data_source 
where b.uth_member_id is null 
  and a.data_source = 'truv' 
;


---truven cleanup from dw 
delete from data_warehouse.member_enrollment_yearly mem 
    using(     
with cte_del as ( select distinct a.uth_member_id 
					from data_warehouse.member_enrollment_yearly a 
					  left outer join data_warehouse.dim_uth_member_id b 
					     on a.uth_member_id = b.uth_member_id 
					    and a.data_source = b.data_source 
					where b.uth_member_id is null 
					  and a.data_source = 'truv'  
			     )
select uth_member_id
from  cte_del
 )  del 
where mem.uth_member_id = del.uth_member_id 
;

----------------

----**********************************************************************************************
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
      on b.data_source in ('mcrn','mcrt')
     and b.member_id_src = bene_id::text
    where b.member_id_src is null 
)
select v_member_id, v_raw_data, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member
;

select count(*) from data_warehouse.dim_uth_member_id where data_source = 'mcrn';

select count(distinct bene_id) from medicare_texas.mbsf_abcd_summary mas ;


----------- Medicaid --------- 

---medicaid enrl
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id )
with cte_distinct_member as ( 
   select distinct client_nbr as v_member_id, 'mdcd' as v_raw_data 
   from medicaid.enrl  
    left outer join data_warehouse.dim_uth_member_id 
      on data_source = 'mdcd' 
     and member_id_src = client_nbr 
    where member_id_src is null 
) 
select v_member_id, v_raw_data, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member
;


--medicaid chip
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id )
with cte_distinct_member as ( 
   select distinct client_nbr as v_member_id, 'mdcd' as v_raw_data 
   from medicaid.chip_uth
    left outer join data_warehouse.dim_uth_member_id 
      on data_source = 'mdcd' 
     and member_id_src = client_nbr 
    where member_id_src is null 
) 
select v_member_id, v_raw_data, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member
;





---******************************** Pharmacy tables---------*****************************
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
    from medicare_national.pde_file
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
    from optum_zip.rx
    left outer join data_warehouse.dim_uth_member_id 
      on data_source = 'optz'
     and member_id_src = patid::text 
    where member_id_src is null 
) 
select v_member_id, 'optz', nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member
;


----Finalize---------------------------------------------------------------------
vacuum analyze data_warehouse.dim_uth_member_id;

select count(*), data_source 
from data_warehouse.dim_uth_member_id
group by data_source ;


