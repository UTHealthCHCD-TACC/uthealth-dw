
--Main table
drop table data_warehouse.enrollment;
create table data_warehouse.enrollment (
id bigserial, source char(2), mbr_id bigint, gndr_cd char(1), mbr_dob smallint, fam_id bigint, state char(2), cov_eff_dt date, cov_term_dt date, plan_ty_cd varchar
) 
WITH (appendonly=true, orientation=column)
distributed randomly;

--Greenplum performance optimization for serial/sequence
alter sequence data_warehouse.enrollment_id_seq cache 200;

--Optum load
insert into data_warehouse.enrollment(source, mbr_id, gndr_cd, mbr_dob, fam_id, state, cov_eff_dt, cov_term_dt, plan_ty_cd)
select 'od', patid, gdr_cd, yrdob, family_id, state, eligeff, eligend, product
from optum_dod.member;

-- State
select distinct state
from optum_dod.member;


--Truven load
insert into data_warehouse.enrollment(source, mbr_id, gndr_cd, mbr_dob, fam_id, state, cov_eff_dt, cov_term_dt, plan_ty_cd)
select 't', enrolid, 'M', dobyr, efamid, s.postal_code, dtstart, dtend, plantyp
from truven.ccaet c
join dev.truven_state_codes s on c.egeoloc=s.truven_code
where sex=1;

insert into data_warehouse.enrollment(source, mbr_id, gndr_cd, mbr_dob, fam_id, state, cov_eff_dt, cov_term_dt, plan_ty_cd)
select 't', enrolid, 'F', dobyr, efamid, s.postal_code, dtstart, dtend, plantyp
from truven.ccaet c
join dev.truven_state_codes s on c.egeoloc=s.truven_code
where sex=2;


-- State
select distinct egeoloc
from truven.ccaet;

-- Need lookup table for Truven state codes
create external table truven_state_codes
(
truven_code smallint, description varchar, state varchar, abbr varchar, postal_code char(2)
) 
LOCATION ( 
'gpfdist://c252-140:8801/state_codes.csv'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

create table dev.truven_state_codes
as 
select *
from truven_state_codes;


