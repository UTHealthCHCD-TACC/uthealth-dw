/* ******************************************************************************************************
 *  This table is used to generate a de-identified uth_admission_id that will be used to populate admission_diag and admission_proc_header tables
 *	The uth_claim_id column will be a sequence that is initially set to a 100,000,000
 *  This code can be re-run as new data comes in, logic is in place to prevent duplicate entries into table
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wc001  || 8/16/2021 || script created 
 * ******************************************************************************************************
 *  wallingTACC  || 8/23/2021 || updated comments.
 * ****************************************************************************************************** */

--- ***** Optum Zip *****
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

--- ***** Optum DoD *****
insert into data_warehouse.dim_uth_admission_id (data_source, year, uth_admission_id, uth_member_id, admission_id_src, member_id_src )
select 'optd', a.year, nextval('data_warehouse.dim_uth_admission_id_uth_admission_id_seq'), b.uth_member_id, a.conf_id, a.patid::text 
from optum_zip.confinement a 
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
select distinct on (caseid, enrolid, year) 
 'truv', a.year, nextval('data_warehouse.dim_uth_admission_id_uth_admission_id_seq'), b.uth_member_id, a.caseid::text, a.enrolid::text 
from truven.ccaef a 
  join data_warehouse.dim_uth_member_id b 
    on data_source = 'truv'
   and b.member_id_src = a.enrolid::text 
  left join data_warehouse.dim_uth_admission_id c
     on  b.data_source = c.data_source
    and a.caseid::text = c.admission_id_src 
    and a.enrolid::text = c.member_id_src 
    and a.year = c."year" 
where a.caseid is not null 
  and a.enrolid is not null 
  and c.uth_admission_id is null 
;


insert into data_warehouse.dim_uth_admission_id (data_source, year, uth_admission_id, uth_member_id, admission_id_src, member_id_src )
select distinct on (caseid, enrolid, year) 
 'truv', a.year, nextval('data_warehouse.dim_uth_admission_id_uth_admission_id_seq'), b.uth_member_id, a.caseid::text, a.enrolid::text 
from truven.ccaei a 
  join data_warehouse.dim_uth_member_id b 
    on data_source = 'truv'
   and b.member_id_src = a.enrolid::text 
  left join data_warehouse.dim_uth_admission_id c
     on  b.data_source = c.data_source
    and a.caseid::text = c.admission_id_src 
    and a.enrolid::text = c.member_id_src 
where a.caseid is not null 
  and a.enrolid is not null 
  and c.uth_admission_id is null  
;

insert into data_warehouse.dim_uth_admission_id (data_source, year, uth_admission_id, uth_member_id, admission_id_src, member_id_src )
select distinct on (caseid, enrolid, year) 
 'truv', a.year, nextval('data_warehouse.dim_uth_admission_id_uth_admission_id_seq'), b.uth_member_id, a.caseid::text, a.enrolid::text 
from truven.mdcrf a 
  join data_warehouse.dim_uth_member_id b 
    on data_source = 'truv'
   and b.member_id_src = a.enrolid::text 
  left join data_warehouse.dim_uth_admission_id c
     on  b.data_source = c.data_source
    and a.caseid::text = c.admission_id_src 
    and a.enrolid::text = c.member_id_src 
where a.caseid is not null 
  and a.enrolid is not null 
  and c.uth_admission_id is null 
;

insert into data_warehouse.dim_uth_admission_id (data_source, year, uth_admission_id, uth_member_id, admission_id_src, member_id_src )
select distinct on (caseid, enrolid, year) 
 'truv', a.year, nextval('data_warehouse.dim_uth_admission_id_uth_admission_id_seq'), b.uth_member_id, a.caseid::text, a.enrolid::text 
from truven.mdcri a 
  join data_warehouse.dim_uth_member_id b 
    on data_source = 'truv'
   and b.member_id_src = a.enrolid::text 
  left join data_warehouse.dim_uth_admission_id c
     on  b.data_source = c.data_source
    and a.caseid::text = c.admission_id_src 
    and a.enrolid::text = c.member_id_src 
where a.caseid is not null 
  and a.enrolid is not null 
  and c.uth_admission_id is null 
;


--medicare texas
insert into data_warehouse.dim_uth_admission_id (data_source, year, uth_admission_id, uth_member_id, admission_id_src, member_id_src )
select 'mcrt' , a.year::int2, nextval('data_warehouse.dim_uth_admission_id_uth_admission_id_seq'), b.uth_member_id, a.admit_id , a.pers_id 
from uthealth/medicare_national.admit a
  join data_warehouse.dim_uth_member_id b 
    on data_source = 'mcrt'
   and b.member_id_src = a.pers_id 
  left join data_warehouse.dim_uth_admission_id c
     on  b.data_source = c.data_source
    and a.admit_id  = c.admission_id_src 
    and a.pers_id = c.member_id_src 
where c.uth_admission_id is null 
;


select * from uthealth/medicare_national.admit;

select * from uthealth/medicare_national.admit_clm;


update data_warehouse.claim_header a set admission_id_src = admit_id 
from uthealth/medicare_national.admit_clm b 
 where a.member_id_src = b.pers_id 
   and a.claim_id_src = b.clm_id 
   and a.data_source = 'mcrt'
   and a."year" = b."year"::int2
;

---va 
vacuum analyze data_warehouse.dim_uth_admission_id;



/* Original Table Create
 
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

**/


