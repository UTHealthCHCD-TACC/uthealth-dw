/*
 * 
 */

----- create dim_uth_member_id
drop table data_warehouse.dim_uth_member_id;

create table data_warehouse.dim_uth_member_id (
	generated_serial bigserial,
	member_id_src text, 
	data_source char(4), 
	uth_member_id int8,
	unique (generated_serial, member_id_src, data_source)
) distributed by (generated_serial);

---

alter sequence data_warehouse.dim_uth_member_id_generated_serial_seq restart with 100000000; 


analyze data_warehouse.dim_uth_member_id;


select dbo.set_all_perms();

------ load dim_uth_member_id

--Optum DoD 17 minutes
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
	select distinct patid as v_member_id, 'optd' as v_raw_data
	from optum_dod.member 
	 left outer join data_warehouse.dim_uth_member_id b 
	              on b.data_source = 'optd'
	             and b.member_id_src = patid::text
	where b.member_id_src is null 
)
select v_member_id, v_raw_data, ( d.data_source_cd::text || (nextval('data_warehouse.dim_uth_member_id_generated_serial_seq'))::text )::bigint
from cte_distinct_member 
  join reference_tables.ref_data_source d 
    on d.data_source = v_raw_data
;


---Optum ZIP 12m
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
	select distinct patid as v_member_id, 'optz' as v_raw_data
	from optum_zip.member
	 left outer join data_warehouse.dim_uth_member_id b 
              on b.data_source = 'optz'
             and b.member_id_src = patid::text
	where b.member_id_src is null 
)
select v_member_id, v_raw_data, ( d.data_source_cd::text || (nextval('data_warehouse.dim_uth_member_id_generated_serial_seq'))::text )::bigint
from cte_distinct_member 
  join reference_tables.ref_data_source d 
    on d.data_source = v_raw_data
;



---Truven Commercial  23m
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
	select distinct enrolid as v_member_id, 'trvc' as v_raw_data
	from truven.ccaet
	 left outer join data_warehouse.dim_uth_member_id b 
          on b.data_source = 'trvc'
         and b.member_id_src = enrolid::text
	where b.member_id_src is null 
)
select v_member_id, v_raw_data, ( d.data_source_cd::text || (nextval('data_warehouse.dim_uth_member_id_generated_serial_seq'))::text )::bigint
from cte_distinct_member 
  join reference_tables.ref_data_source d 
    on d.data_source = v_raw_data
;


---Truven Medicare 3m
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
	select distinct enrolid as v_member_id, 'trvm' as v_raw_data
	from truven.mdcrt
	 left outer join data_warehouse.dim_uth_member_id b 
      on b.data_source = 'trvc'
     and b.member_id_src = enrolid::text
where b.member_id_src is null 
)
select v_member_id, v_raw_data, ( d.data_source_cd::text || (nextval('data_warehouse.dim_uth_member_id_generated_serial_seq'))::text )::bigint
from cte_distinct_member 
  join reference_tables.ref_data_source d 
    on d.data_source = v_raw_data
;


--- Medicare 1m
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
	select distinct bene_id as v_member_id, 'mdcr' as v_raw_data
	from medicare.mbsf_abcd_summary
	 left outer join data_warehouse.dim_uth_member_id b 
      on b.data_source = 'mdcr'
     and b.member_id_src = bene_id::text
    where b.member_id_src is null 
)
select v_member_id, v_raw_data, ( d.data_source_cd::text || (nextval('data_warehouse.dim_uth_member_id_generated_serial_seq'))::text )::bigint
from cte_distinct_member
  join reference_tables.ref_data_source d
    on d.data_source = v_raw_data
;




select * 
from truven.ccaet a 
  join truven.ccaet b 
  on a.dtstart = b.dtstart and a.enrolid = b.enrolid and a.dobyr <> b.dobyr



create index uth_id_index on data_warehouse.dim_uth_member_id (uth_member_id);


select * from data_warehouse.dim_uth_member_id;
