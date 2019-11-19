/*
 * 
 */

----- create dim_uth_member_id
drop table data_warehouse.dim_uth_member_id;

create table data_warehouse.dim_uth_member_id (
	generated_serial bigserial,
	member_id_src text, 
	data_source char(4), 
	uth_member_id int8
)with (appendonly=true, orientation = column) 
distributed by (generated_serial);

---

alter sequence data_warehouse.dim_uth_member_id_generated_serial_seq restart with 100000000; 


analyze data_warehouse.dim_uth_member_id;



------ load dim_uth_member_id

--Optum DoD
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
	select distinct patid as v_member_id, 'optd' as v_raw_data
	from optum_dod.member
)
select v_member_id, v_raw_data, ( d.data_source_cd::text || (nextval('data_warehouse.dim_uth_member_id_generated_serial_seq'))::text )::bigint
from cte_distinct_member 
  join reference_tables.ref_data_source d 
    on d.data_source = v_raw_data
where v_member_id::text not in (
	select m.member_id_src
	from data_warehouse.dim_uth_member_id m
	)
;


---Optum ZIP
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
	select distinct patid as v_member_id, 'optz' as v_raw_data
	from optum_zip.member
)
select v_member_id, v_raw_data, ( d.data_source_cd::text || (nextval('data_warehouse.dim_uth_member_id_generated_serial_seq'))::text )::bigint
from cte_distinct_member 
  join reference_tables.ref_data_source d 
    on d.data_source = v_raw_data
where v_member_id::text not in (
	select m.member_id_src
	from data_warehouse.dim_uth_member_id m
	)
;



---Truven Commercial
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
	select distinct enrolid as v_member_id, 'trvc' as v_raw_data
	from truven.ccaet
)
select v_member_id, v_raw_data, ( d.data_source_cd::text || (nextval('data_warehouse.dim_uth_member_id_generated_serial_seq'))::text )::bigint
from cte_distinct_member 
  join reference_tables.ref_data_source d 
    on d.data_source = v_raw_data
where v_member_id::text not in (
	select m.member_id_src
	from data_warehouse.dim_uth_member_id m
	)
;


---Truven Medicare
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
	select distinct enrolid as v_member_id, 'trvm' as v_raw_data
	from truven.mdcrt
)
select v_member_id, v_raw_data, ( d.data_source_cd::text || (nextval('data_warehouse.dim_uth_member_id_generated_serial_seq'))::text )::bigint
from cte_distinct_member 
  join reference_tables.ref_data_source d 
    on d.data_source = v_raw_data
where v_member_id::text not in (
	select m.member_id_src
	from data_warehouse.dim_uth_member_id m
	)
;

--- Medicare
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
	select distinct bene_id as v_member_id, 'mdcr' as v_raw_data
	from medicare.mbsf_abcd_summary
)
select v_member_id, v_raw_data, ( d.data_source_cd::text || (nextval('data_warehouse.dim_uth_member_id_generated_serial_seq'))::text )::bigint
from cte_distinct_member
  join reference_tables.ref_data_source d
    on d.data_source = v_raw_data
where v_member_id::text not in ( 
	select m.member_id_src
	from data_warehouse.dim_uth_member_id m 
	)
;







create index uth_id_index on data_warehouse.dim_uth_member_id (uth_member_id);


select * from data_warehouse.dim_uth_member_id;
