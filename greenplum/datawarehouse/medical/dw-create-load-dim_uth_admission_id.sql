drop table if exists data_warehouse.dim_uth_admission_id;

create table data_warehouse.dim_uth_admission_id (
	data_source char(4),
	year int2,
	uth_admission_id bigserial,
	uth_member_id bigint,
	admission_id_src text,
	member_id_src text
) with (appendonly=true, orientation=column)
distributed by (uth_member_id)
;

alter sequence data_warehouse.dim_uth_admission_id_uth_admission_id_seq restart with 100000000;

alter sequence data_warehouse.dim_uth_admission_id_uth_admission_id_seq cache 200;

vacuum analyze data_warehouse.dim_uth_admission_id;



insert into data_warehouse.dim_uth_admission_id (data_source, year, uth_admission_id, uth_member_id, admission_id_src, member_id_src )
select 'optz', a.year, nextval('data_warehouse.dim_uth_admission_id_uth_admission_id_seq'), b.uth_member_id, a.conf_id, a.patid::text 
from optum_zip.confinement a 
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'optz'
   and b.member_id_src = a.patid::text 
  left join data_warehouse.dim_uth_admission_id c
     on  b.data_source = c.data_source
    and a.conf_id = c.admission_id_src 
    and a.patid::text = c.member_id_src 
where a.conf_id is not null 
and a.patid is not null 
and c.uth_admission_id is null 
;


insert into data_warehouse.dim_uth_admission_id (data_source, year, uth_admission_id, uth_member_id, admission_id_src, member_id_src )
select 'optd', a.year, nextval('data_warehouse.dim_uth_admission_id_uth_admission_id_seq'), b.uth_member_id, a.conf_id, a.patid::text 
from optum_dod.confinement a 
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'optd'
   and b.member_id_src = a.patid::text 
  left join data_warehouse.dim_uth_admission_id c
     on  b.data_source = c.data_source
    and a.conf_id = c.admission_id_src 
    and a.patid::text = c.member_id_src 
where a.conf_id is not null 
and a.patid is not null 
and c.uth_admission_id is null 
;


insert into data_warehouse.dim_uth_admission_id (data_source, year, uth_admission_id, uth_member_id, admission_id_src, member_id_src )
select 'trvc', a.year, nextval('data_warehouse.dim_uth_admission_id_uth_admission_id_seq'), b.uth_member_id, a.caseid::text, a.enrolid::text 
from truven.ccaef a 
  join data_warehouse.dim_uth_member_id b 
    on data_source = 'trvc'
   and b.member_id_src = a.enrolid::text 
where a.caseid is not null 
  and a.enrolid is not null 
;

insert into data_warehouse.dim_uth_admission_id (data_source, year, uth_admission_id, uth_member_id, admission_id_src, member_id_src )
select 'trvc', a.year, nextval('data_warehouse.dim_uth_admission_id_uth_admission_id_seq'), b.uth_member_id, a.caseid::text, a.enrolid::text  
from truven.ccaei a 
  join data_warehouse.dim_uth_member_id b 
    on data_source = 'trvc'
   and b.member_id_src = a.enrolid::text 
where a.caseid is not null 
  and a.enrolid is not null 
;

insert into data_warehouse.dim_uth_admission_id (data_source, year, uth_admission_id, uth_member_id, admission_id_src, member_id_src )
select 'trvm', a.year, nextval('data_warehouse.dim_uth_admission_id_uth_admission_id_seq'), b.uth_member_id, a.caseid::text, a.enrolid::text 
from truven.mdcrf a 
  join data_warehouse.dim_uth_member_id b 
    on data_source = 'trvm'
   and b.member_id_src = a.enrolid::text 
where a.caseid is not null 
  and a.enrolid is not null 
;

insert into data_warehouse.dim_uth_admission_id (data_source, year, uth_admission_id, uth_member_id, admission_id_src, member_id_src )
select 'trvm', a.year, nextval('data_warehouse.dim_uth_admission_id_uth_admission_id_seq'), b.uth_member_id, a.caseid::text, a.enrolid::text  
from truven.mdcri a 
  join data_warehouse.dim_uth_member_id b 
    on data_source = 'trvm'
   and b.member_id_src = a.enrolid::text 
where a.caseid is not null 
  and a.enrolid is not null 
;


	