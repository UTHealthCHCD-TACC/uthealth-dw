--Creat demographic table (hacking in a version from scripts for creating data_warehouse.member_enrollment_monthly, but ignoring dim_uth_member_id)
create table dev.member_demographics_dw
with(appendonly=true, orientation=column)
as
select 'TRUV' as data_source, b.month_year_id, trunc(m.enrolid, 0)::text as member_id_src,
       c.gender_cd, case when length(s.abbr) > 2 then '' else s.abbr end as state, trunc(m.empzip,0)::text as zip3,
       b.year_int - dobyr as age_derived, (trunc(dobyr,0)::varchar || '-12-31')::date as dob_derived,
       d.plan_type, 'COM' as bus_cd 
from truven_ccaet m
  join reference_tables.ref_truven_state_codes s 
    on m.egeoloc=s.truven_code
  join reference_tables.ref_month_year b 
    on b.start_of_month between date_trunc('month', m.dtstart) and m.dtend
    and b.year_int between 2015 and 2017
  left outer join reference_tables.ref_gender c
    on c.data_source = 'trv'
   and c.gender_cd_src = m.sex::text
left outer join reference_tables.ref_plan_type d
    on d.data_source = 'trv'
  and d.plan_type_src::int = m.plantyp;
  
--Create base data
drop table if exists dev.ptk;
create table dev.ptk
with(appendonly=true, orientation=column)
as
SELECT ch.id AS claim_header_id, ch.source AS data_source, ch.member_id_src, ch.claim_id_src, 
ch.claim_type, ch.in_network, ch.admit_date, ch.discharge_date, ch.discharge_status_src, 
ch.admit_type_src, ch.total_cost, ch.total_paid, cd.id AS claim_detail_id, cd.proc_code, cd.proc_mod, cd.cost, cd.paid, 
cd.service_date, cd.paid_date, dx.id AS diagnosis_id, dx.diag_code, icd10.icd_10, icd10.description,
m.month_year_id,
m.age_derived, m.gender_cd, m.state, m.zip3
   FROM dev.claim_header_dw ch
   JOIN dev.claim_detail_dw cd ON cd.claim_header_id = ch.id
   JOIN dev.claim_detail_diag_dw dx ON dx.claim_detail_id = cd.id
   JOIN reference_tables.icd_10 icd10 ON dx.diag_code::text = icd10.icd_10::text
   join dev.member_demographics_dw m on m.member_id_src=ch.member_id_src and m.month_year_id=get_my_from_date(cd.service_date)
  WHERE dx.diag_code::text ~~ 'O%'::text;

  
 --Sample query
 select p.icd_10, p.description, p.state, count(distinct p.claim_detail_id) as claim_detail_count
 from dev.ptk p
 group by 1, 2, 3
 order by 1, 4 desc;
 order by 1, 3 desc;