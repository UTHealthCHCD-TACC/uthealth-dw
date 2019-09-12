/*
 * 
 */

----- create dim_member_id_src
drop table data_warehouse.dim_member_id_src;

create table data_warehouse.dim_member_id_src (
	member_id_src text, 
	data_source char(4), 
	uth_member_id int8 unique
);

---
drop sequence uth_serial;

create sequence uth_serial start 100000000;


------ load dim_member_id_src

--Optum DoD
insert into data_warehouse.dim_member_id_src (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
	select distinct patid as v_member_id, 'optd' as v_raw_data
	from optum_dod.member
)
select v_member_id, v_raw_data, ( d.data_source_cd::text || (nextval('uth_serial'))::text )::bigint
from cte_distinct_member 
  join reference_tables.ref_data_source d 
    on d.data_source = v_raw_data
where v_member_id::text not in (
	select m.member_id_src
	from data_warehouse.dim_member_id_src m
	)
;


---Optum ZIP
insert into data_warehouse.dim_member_id_src (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
	select distinct patid as v_member_id, 'optz' as v_raw_data
	from optum_zip.member
)
select v_member_id, v_raw_data, ( d.data_source_cd::text || (nextval('uth_serial'))::text )::bigint
from cte_distinct_member 
  join reference_tables.ref_data_source d 
    on d.data_source = v_raw_data
where v_member_id::text not in (
	select m.member_id_src
	from data_warehouse.dim_member_id_src m
	)
;



---Truven Commercial
insert into data_warehouse.dim_member_id_src (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
	select distinct enrolid as v_member_id, 'trvc' as v_raw_data
	from truven.ccaet
)
select v_member_id, v_raw_data, ( d.data_source_cd::text || (nextval('uth_serial'))::text )::bigint
from cte_distinct_member 
  join reference_tables.ref_data_source d 
    on d.data_source = v_raw_data
where v_member_id::text not in (
	select m.member_id_src
	from data_warehouse.dim_member_id_src m
	)
;


---Truven Medicare
insert into data_warehouse.dim_member_id_src (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
	select distinct enrolid as v_member_id, 'trvm' as v_raw_data
	from truven.mdcrt
)
select v_member_id, v_raw_data, ( d.data_source_cd::text || (nextval('uth_serial'))::text )::bigint
from cte_distinct_member 
  join reference_tables.ref_data_source d 
    on d.data_source = v_raw_data
where v_member_id::text not in (
	select m.member_id_src
	from data_warehouse.dim_member_id_src m
	)
;



create index uth_id_index on data_warehouse.dim_member_id_src (uth_member_id);


select * from data_warehouse.dim_member_id_src;
