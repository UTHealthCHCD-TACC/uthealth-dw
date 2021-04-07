
drop table if exists dev.wc_mdand_vacc_claims;

---get vacc from inpatient services
select a.enrolid, extract(year from a.svcdate) as yr,
	   case when PROC1 in ('90649','90650','90651') then 'HPV'
			    when PROC1 in ('90733','90734')  then 'MEN'
			    when PROC1 in ('90714','90715')  then 'TDAP' end as vacc_type
into dev.wc_mdand_vacc_claims
from truven.ccaes a 
where a.proc1 in ('90649','90650','90651','90733','90734','90714','90715')
  and extract(year from a.svcdate) between 2014 and 2018
;

---get vacc for outpatient
insert into dev.wc_mdand_vacc_claims
select a.enrolid, extract(year from a.svcdate) as yr,
	   case when PROC1 in ('90649','90650','90651') then 'HPV'
			    when PROC1 in ('90733','90734')  then 'MEN'
			    when PROC1 in ('90714','90715')  then 'TDAP' end as vacc_type
from truven.ccaeo a
where a.proc1 in ('90649','90650','90651','90733','90734','90714','90715')
 and extract(year from a.svcdate) between 2014 and 2018
;


---consolidate
drop table if exists dev.wc_mdand_vacc;
select distinct enrolid, yr , vacc_type
into dev.wc_mdand_vacc
from dev.wc_mdand_vacc_claims
;


drop table if exists dev.wc_mdand_cohort;


select b.member_id_src, a.gender_cd, a.zip3,  a.age_derived, a."year" 
into dev.wc_mdand_cohort
from data_warehouse.member_enrollment_yearly a 
   join data_warehouse.dim_uth_member_id b  
      on a.uth_member_id = b.uth_member_id
   --join reference_tables.ref_zip_code z 
      --on substring(z.zip,1,3) = a.zip3       
where a.data_source = 'truv'
  and a."year" between 2014 and 2018 
  and a.age_derived between 13 and 17 
  and a.state = 'TX'
;

---zip3
select a.year, zip3, 
		count(a.member_id_src), count(b.enrolid) as hpv_count, count(c.enrolid) as men_count, count(d.enrolid) as tdap_count,
       count(b.enrolid)::float / count(a.member_id_src) as hpv_prev,
       count(c.enrolid)::float / count(a.member_id_src) as men_prev,
       count(d.enrolid)::float / count(a.member_id_src) as tdap_prev
       into dev.wc_mdand_extract_zip
from dev.wc_mdand_cohort a 
  left outer join dev.wc_mdand_vacc b 
     on a.member_id_src = b.enrolid::text 
    and a.year = b.yr 
    and b.vacc_type = 'HPV'
  left outer join dev.wc_mdand_vacc c
     on a.member_id_src = c.enrolid::text 
    and a.year = c.yr 
    and c.vacc_type = 'MEN'  
  left outer join dev.wc_mdand_vacc d
     on a.member_id_src = d.enrolid::text 
    and a.year = d.yr 
    and d.vacc_type = 'TDAP'     
group by year ,  zip3
order by year , zip3
;


--zip+gen
select a.year, zip3, gender_cd, 
		count(a.member_id_src), count(b.enrolid) as hpv_count, count(c.enrolid) as men_count, count(d.enrolid) as tdap_count,
       count(b.enrolid)::float / count(a.member_id_src) as hpv_prev,
       count(c.enrolid)::float / count(a.member_id_src) as men_prev,
       count(d.enrolid)::float / count(a.member_id_src) as tdap_prev
       into dev.wc_mdand_extract_gender
from dev.wc_mdand_cohort a 
  left outer join dev.wc_mdand_vacc b 
     on a.member_id_src = b.enrolid::text 
    and a.year = b.yr 
    and b.vacc_type = 'HPV'
  left outer join dev.wc_mdand_vacc c
     on a.member_id_src = c.enrolid::text 
    and a.year = c.yr 
    and c.vacc_type = 'MEN'  
  left outer join dev.wc_mdand_vacc d
     on a.member_id_src = d.enrolid::text 
    and a.year = d.yr 
    and d.vacc_type = 'TDAP'     
group by year , gender_cd, zip3
order by year , zip3, gender_cd
;

select * from data_warehouse.member_enrollment_yearly where data_source = 'truv';

