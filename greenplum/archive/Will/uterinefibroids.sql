---uterine fibroids worksheet 

---exclusions
select uth_member_id 
into dev.wc_uterine_exclusions
from data_warehouse.claim_diag a 
where a.diag_cd in ('Z90710','Z90712')
  and year between 2016 and 2017 
  and data_source in ('truv','optz')
;


---uterine fibroids
select uth_member_id 
into dev.wc_uterine_clms
from data_warehouse.claim_diag a 
where a.diag_cd like 'D25%'
  and year between 2016 and 2017 
  and data_source in ('truv','optz')
;


---denom1
select a.data_source , case when a.age_derived between 0 and 19 then 1 
            when a.age_derived between 20 and 34 then 2 
            when a.age_derived between 35 and 44 then 3 
            when a.age_derived between 45 and 54 then 4 
            when a.age_derived between 55 and 64 then 5 
            when a.age_derived between 65 and 74 then 6 
            else 7 end as age_grp , count(*)
--into dev.wc_uterine_denom1
from data_warehouse.member_enrollment_yearly a 
where data_source in ('truv','optz')
  and year = 2017 
  and gender_cd = 'F'
  and age_derived between 20 and 74
  and enrolled_dec is true 
  and state = 'TX'
and a.uth_member_id not in ( select uth_member_id from dev.wc_uterine_exclusions)
group by a.data_source ,case when a.age_derived between 0 and 19 then 1 
            when a.age_derived between 20 and 34 then 2 
            when a.age_derived between 35 and 44 then 3 
            when a.age_derived between 45 and 54 then 4 
            when a.age_derived between 55 and 64 then 5 
            when a.age_derived between 65 and 74 then 6 
            else 7 end
order by a.data_source ,case when a.age_derived between 0 and 19 then 1 
            when a.age_derived between 20 and 34 then 2 
            when a.age_derived between 35 and 44 then 3 
            when a.age_derived between 45 and 54 then 4 
            when a.age_derived between 55 and 64 then 5 
            when a.age_derived between 65 and 74 then 6 
            else 7 end
;



---numer1
select a.data_source , case when a.age_derived between 0 and 19 then 1 
            when a.age_derived between 20 and 34 then 2 
            when a.age_derived between 35 and 44 then 3 
            when a.age_derived between 45 and 54 then 4 
            when a.age_derived between 55 and 64 then 5 
            when a.age_derived between 65 and 74 then 6 
            else 7 end as age_grp , count(*)
--into dev.wc_uterine_denom1
from data_warehouse.member_enrollment_yearly a 
where data_source in ('truv','optz')
  and year = 2017 
  and gender_cd = 'F'
  and age_derived between 20 and 74
  and enrolled_dec is true 
  and state = 'TX'
and a.uth_member_id not in ( select uth_member_id from dev.wc_uterine_exclusions)
and a.uth_member_id in (select uth_member_id from dev.wc_uterine_clms)
group by a.data_source ,case when a.age_derived between 0 and 19 then 1 
            when a.age_derived between 20 and 34 then 2 
            when a.age_derived between 35 and 44 then 3 
            when a.age_derived between 45 and 54 then 4 
            when a.age_derived between 55 and 64 then 5 
            when a.age_derived between 65 and 74 then 6 
            else 7 end
order by a.data_source ,case when a.age_derived between 0 and 19 then 1 
            when a.age_derived between 20 and 34 then 2 
            when a.age_derived between 35 and 44 then 3 
            when a.age_derived between 45 and 54 then 4 
            when a.age_derived between 55 and 64 then 5 
            when a.age_derived between 65 and 74 then 6 
            else 7 end
;


---denom2_temp
select a.uth_member_id 
    into dev.wc_uterine_denom2_temp
from data_warehouse.member_enrollment_yearly a 
where data_source in ('truv','optz')
  and year = 2017 
  and gender_cd = 'F'
  and age_derived between 20 and 74
  and enrolled_dec is true 
  and state = 'TX'
and a.uth_member_id not in ( select uth_member_id from dev.wc_uterine_exclusions)
and a.uth_member_id in (select uth_member_id from dev.wc_uterine_clms)
;


---denom2
select distinct a.uth_member_id 
    into dev.wc_uterine_denom2
from data_warehouse.member_enrollment_yearly a 
where uth_member_id in ( select uth_member_id from dev.wc_uterine_denom2_temp) 
  and exists ( select 1 from data_warehouse.member_enrollment_yearly b where b.uth_member_id = a.uth_member_id and b.year = 2018 and b.total_enrolled_months = 12 )
  and exists ( select 1 from data_warehouse.member_enrollment_yearly b where b.uth_member_id = a.uth_member_id and b.year = 2019 and b.total_enrolled_months = 12 )
;


---denom1
select a.data_source , case when a.age_derived between 0 and 19 then 1 
            when a.age_derived between 20 and 34 then 2 
            when a.age_derived between 35 and 44 then 3 
            when a.age_derived between 45 and 54 then 4 
            when a.age_derived between 55 and 64 then 5 
            when a.age_derived between 65 and 74 then 6 
            else 7 end as age_grp , count(*)
from data_warehouse.member_enrollment_yearly a 
where data_source in ('truv','optz')
  and year = 2017 
and a.uth_member_id in ( select uth_member_id from dev.wc_uterine_denom2)
group by a.data_source ,case when a.age_derived between 0 and 19 then 1 
            when a.age_derived between 20 and 34 then 2 
            when a.age_derived between 35 and 44 then 3 
            when a.age_derived between 45 and 54 then 4 
            when a.age_derived between 55 and 64 then 5 
            when a.age_derived between 65 and 74 then 6 
            else 7 end
order by a.data_source ,case when a.age_derived between 0 and 19 then 1 
            when a.age_derived between 20 and 34 then 2 
            when a.age_derived between 35 and 44 then 3 
            when a.age_derived between 45 and 54 then 4 
            when a.age_derived between 55 and 64 then 5 
            when a.age_derived between 65 and 74 then 6 
            else 7 end
;

---hysterectomy
--icdproc
select distinct a.uth_member_id 
into dev.wc_uterine_hyster
from data_warehouse.claim_icd_proc a
   join dev.wc_uterine_denom2 b  
     on a.uth_member_id = b.uth_member_id
where year between 2018 and 2019 
and a.data_source in ('optz','truv')
and a.proc_cd like '0U59%';


---hyster cpt/hcpcs
insert into dev.wc_uterine_hyster 
select distinct a.uth_member_id
from data_warehouse.claim_detail a 
   join dev.wc_uterine_denom2 b  
     on a.uth_member_id = b.uth_member_id
where year between 2018 and 2019 
  and a.data_source in ('optz','truv')
  and ( a.cpt_hcpcs between '58260' and '58270' 
     or a.cpt_hcpcs between '58275' and '58280' 
     or a.cpt_hcpcs between '58290' and '58294' 
     or a.cpt_hcpcs between '58541' and '58544' 
     or a.cpt_hcpcs between '58550' and '58554' 
     or a.cpt_hcpcs between '58570' and '58573' )
 
     
---hyster counts
select a.data_source , case when a.age_derived between 0 and 19 then 1 
            when a.age_derived between 20 and 34 then 2 
            when a.age_derived between 35 and 44 then 3 
            when a.age_derived between 45 and 54 then 4 
            when a.age_derived between 55 and 64 then 5 
            when a.age_derived between 65 and 74 then 6 
            else 7 end as age_grp , count(*)
from data_warehouse.member_enrollment_yearly a 
where data_source in ('truv','optz')
  and year = 2017 
and a.uth_member_id in ( select uth_member_id from dev.wc_uterine_hyster )
group by a.data_source ,case when a.age_derived between 0 and 19 then 1 
            when a.age_derived between 20 and 34 then 2 
            when a.age_derived between 35 and 44 then 3 
            when a.age_derived between 45 and 54 then 4 
            when a.age_derived between 55 and 64 then 5 
            when a.age_derived between 65 and 74 then 6 
            else 7 end
order by a.data_source ,case when a.age_derived between 0 and 19 then 1 
            when a.age_derived between 20 and 34 then 2 
            when a.age_derived between 35 and 44 then 3 
            when a.age_derived between 45 and 54 then 4 
            when a.age_derived between 55 and 64 then 5 
            when a.age_derived between 65 and 74 then 6 
            else 7 end
;



---myomectomy
--icdproc
select a.uth_member_id 
into dev.wc_uterine_myomectomy
from data_warehouse.claim_icd_proc a
   join dev.wc_uterine_denom2 b  
     on a.uth_member_id = b.uth_member_id
where year between 2018 and 2019 
and a.data_source in ('optz','truv')
and a.proc_cd like '0UT9%'
;


---myomectomy cpt/hcpcs
insert into dev.wc_uterine_myomectomy
select a.uth_member_id
from data_warehouse.claim_detail a 
   join dev.wc_uterine_denom2 b  
     on a.uth_member_id = b.uth_member_id
where year between 2018 and 2019 
  and a.data_source in ('optz','truv')
  and a.cpt_hcpcs in ('58140','58145','58146','58545','58546')
  ;
 
 
 ---myomectomy counts
select a.data_source , case when a.age_derived between 0 and 19 then 1 
            when a.age_derived between 20 and 34 then 2 
            when a.age_derived between 35 and 44 then 3 
            when a.age_derived between 45 and 54 then 4 
            when a.age_derived between 55 and 64 then 5 
            when a.age_derived between 65 and 74 then 6 
            else 7 end as age_grp , count(*)
from data_warehouse.member_enrollment_yearly a 
where data_source in ('truv','optz')
  and year = 2017 
and a.uth_member_id in ( select uth_member_id from dev.wc_uterine_myomectomy)
group by a.data_source ,case when a.age_derived between 0 and 19 then 1 
            when a.age_derived between 20 and 34 then 2 
            when a.age_derived between 35 and 44 then 3 
            when a.age_derived between 45 and 54 then 4 
            when a.age_derived between 55 and 64 then 5 
            when a.age_derived between 65 and 74 then 6 
            else 7 end
order by a.data_source ,case when a.age_derived between 0 and 19 then 1 
            when a.age_derived between 20 and 34 then 2 
            when a.age_derived between 35 and 44 then 3 
            when a.age_derived between 45 and 54 then 4 
            when a.age_derived between 55 and 64 then 5 
            when a.age_derived between 65 and 74 then 6 
            else 7 end
;



--endometrial ablation
--icdproc
select distinct a.uth_member_id 
into dev.wc_uterine_endo
from data_warehouse.claim_icd_proc a
   join dev.wc_uterine_denom2 b  
     on a.uth_member_id = b.uth_member_id
where year between 2018 and 2019 
and a.data_source in ('optz','truv')
and a.proc_cd in ('0U5B0ZZ','0U5B3ZZ','0U5B4ZZ','0U5B7ZZ','0U5B8ZZ','0UDB7ZZ','0UDB8ZZ')


---endo cpt/hcpcs
insert into dev.wc_uterine_endo 
select distinct a.uth_member_id
from data_warehouse.claim_detail a 
   join dev.wc_uterine_denom2 b  
     on a.uth_member_id = b.uth_member_id
where year between 2018 and 2019 
  and a.data_source in ('optz','truv')
  and  a.cpt_hcpcs in ('58353','58356','58653')
 

     ---endo counts
select a.data_source , case when a.age_derived between 0 and 19 then 1 
            when a.age_derived between 20 and 34 then 2 
            when a.age_derived between 35 and 44 then 3 
            when a.age_derived between 45 and 54 then 4 
            when a.age_derived between 55 and 64 then 5 
            when a.age_derived between 65 and 74 then 6 
            else 7 end as age_grp , count(*)
from data_warehouse.member_enrollment_yearly a 
where data_source in ('truv','optz')
  and year = 2017 
and a.uth_member_id in ( select uth_member_id from dev.wc_uterine_endo)
group by a.data_source ,case when a.age_derived between 0 and 19 then 1 
            when a.age_derived between 20 and 34 then 2 
            when a.age_derived between 35 and 44 then 3 
            when a.age_derived between 45 and 54 then 4 
            when a.age_derived between 55 and 64 then 5 
            when a.age_derived between 65 and 74 then 6 
            else 7 end
order by a.data_source ,case when a.age_derived between 0 and 19 then 1 
            when a.age_derived between 20 and 34 then 2 
            when a.age_derived between 35 and 44 then 3 
            when a.age_derived between 45 and 54 then 4 
            when a.age_derived between 55 and 64 then 5 
            when a.age_derived between 65 and 74 then 6 
            else 7 end
; 
     
     
     
--uterine firboid embolization  ufe
--icdproc
select distinct a.uth_member_id 
into dev.wc_uterine_ufe
from data_warehouse.claim_icd_proc a
   join dev.wc_uterine_denom2 b  
     on a.uth_member_id = b.uth_member_id
where year between 2018 and 2019 
and a.data_source in ('optz','truv')
and a.proc_cd in ('04LE3DT','04LE3ZT','04LF3DU','04LF3ZU')
;

---ufe cpt/hcpcs
insert into dev.wc_uterine_ufe
select distinct a.uth_member_id
from data_warehouse.claim_detail a 
   join dev.wc_uterine_denom2 b  
     on a.uth_member_id = b.uth_member_id
where year between 2018 and 2019 
  and a.data_source in ('optz','truv')
  and a.cpt_hcpcs = '37210'
 ;


--ufe counts
select a.data_source , case when a.age_derived between 0 and 19 then 1 
            when a.age_derived between 20 and 34 then 2 
            when a.age_derived between 35 and 44 then 3 
            when a.age_derived between 45 and 54 then 4 
            when a.age_derived between 55 and 64 then 5 
            when a.age_derived between 65 and 74 then 6 
            else 7 end as age_grp , count(*)
from data_warehouse.member_enrollment_yearly a 
where data_source in ('truv','optz')
  and year = 2017 
and a.uth_member_id in ( select uth_member_id from dev.wc_uterine_ufe)
group by a.data_source ,case when a.age_derived between 0 and 19 then 1 
            when a.age_derived between 20 and 34 then 2 
            when a.age_derived between 35 and 44 then 3 
            when a.age_derived between 45 and 54 then 4 
            when a.age_derived between 55 and 64 then 5 
            when a.age_derived between 65 and 74 then 6 
            else 7 end
order by a.data_source ,case when a.age_derived between 0 and 19 then 1 
            when a.age_derived between 20 and 34 then 2 
            when a.age_derived between 35 and 44 then 3 
            when a.age_derived between 45 and 54 then 4 
            when a.age_derived between 55 and 64 then 5 
            when a.age_derived between 65 and 74 then 6 
            else 7 end
; 
