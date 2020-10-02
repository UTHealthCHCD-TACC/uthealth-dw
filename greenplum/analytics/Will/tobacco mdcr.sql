---cohorts
drop table  dev.wc_tobacco_cohort_mdcr
  
select a.uth_member_id , a.data_source , a.bus_cd , a.age_derived , a.year, a.gender_cd, a.zip3 
into dev.wc_tobacco_cohort_mdcr
from data_warehouse.member_enrollment_yearly a 
where a.data_source in ('mdcr')
and a.year = 2017
and a.age_derived >= 15
and a.enrolled_dec = true
and a.state = 'TX' 
  and a.zip3 between '750' and '799'
;

---find smokers
drop table dev.wc_tobacco_diags_mdcr

---icd
select distinct d.uth_member_id 
into dev.wc_tobacco_diags_mdcr
from data_warehouse.claim_diag d 
where ( d.diag_cd like'F1720%'
		or d.diag_cd like'F1721%'
		or d.diag_cd like'F1722%'
		or d.diag_cd like'F1729%'
		or d.diag_cd like'Z716%'
		or d.diag_cd like'O9933%'
		or d.diag_cd like'T652%'
		or d.diag_cd in ('Z716','Z720','Z87891','P9681','P042') ) 
and d.uth_member_id in ( select uth_member_id from dev.wc_tobacco_cohort_mdcr )
and d.year between 2015 and 2017
;

---cpt
insert into dev.wc_tobacco_diags_mdcr
select distinct uth_member_id 
from data_warehouse.claim_detail a 
where a.procedure_cd in ( '99406','99407','G0436','G0437','G9016','S9453','S4995','G9276','G9458',
				  '1034F','4004F','4001F','G9906','G9907','G9908','G9909')
and uth_member_id in ( select uth_member_id from dev.wc_tobacco_cohort_mdcr )
and year between 2015 and 2017
;			  

---rx tbl
create table dev.wc_tobacco_ndcs (ndc_cd text);

select count(*) from dev.wc_tobacco_ndcs 

--rxss
insert into dev.wc_tobacco_diags_mdcr
select distinct a.uth_member_id  
from data_warehouse.pharmacy_claims a 
where a.ndc in ( select ndc_cd from dev.wc_tobacco_ndcs)
and uth_member_id in ( select uth_member_id from dev.wc_tobacco_cohort_mdcr )
  and a.year between 2016 and 2017	 
  ;
 

---populate tobacco use 
alter table dev.wc_tobacco_cohort_mdcr add column tobacco_flag int2 default 0;

drop table dev.wc_tobacco_users_mdcr;

select distinct uth_member_id 
into dev.wc_tobacco_users_mdcr
from dev.wc_tobacco_diags_mdcr ;

update dev.wc_tobacco_cohort_mdcr a set tobacco_flag = 1
  from dev.wc_tobacco_users_mdcr b 
    where b.uth_member_id = a.uth_member_id
 ;


---prevalance of smoking
select count(*), count(distinct uth_member_id), sum(tobacco_flag) as tb_user, ( sum(tobacco_flag) / count(uth_member_id)::float ) as prev, data_source, bus_cd, year , gender_cd 
from dev.wc_tobacco_cohort_mdcr
group by data_source, bus_cd, year, gender_cd 
order by data_source, bus_cd, gender_cd 
;

select count(*), count(distinct uth_member_id), sum(tobacco_flag) as tb_user, ( sum(tobacco_flag) / count(uth_member_id)::float ) as prev, data_source, bus_cd
from dev.wc_tobacco_cohort_mdcr
group by data_source, bus_cd
order by data_source, bus_cd
;

-----smoking cessation

drop table dev.wc_tobacco_cessation_mdcr

---icd smoking cessation
select distinct d.uth_member_id 
into dev.wc_tobacco_cessation_mdcr
from data_warehouse.claim_diag d 
where d.diag_cd = 'Z716'
and d.uth_member_id in ( select uth_member_id from dev.wc_tobacco_users_mdcr)
and d.year = 2017
;

---cpt smoking cessation
insert into dev.wc_tobacco_cessation_mdcr
select distinct uth_member_id 
from data_warehouse.claim_detail a 
where a.procedure_cd in ( '99406','99407','G0436','G0437','G9016','S9453','S4995','G9276','G9458',
				  		  '4004F','4001F')
  and a.uth_member_id in ( select uth_member_id from dev.wc_tobacco_users_mdcr)
  and a.year = 2017			  
;				  


---rx smoking cessation
select * from reference_tables.redbook where prodnme like '%CHANTIX%';

create table dev.wc_tobacco_cessation_ndcs (ndc_cd text);

select count(*) from dev.wc_tobacco_cessation_ndcs;


update dev.wc_tobacco_cessation_ndcs set ndc_cd = lpad(ndc_cd,11,'0');

select * from dev.wc_tobacco_cessation_ndcs;
 
--ss
insert into dev.wc_tobacco_cessation_mdcr
select distinct a.uth_member_id 
from data_warehouse.pharmacy_claims a 
where a.ndc in ( select ndc_cd from dev.wc_tobacco_ndcs)
 and a.uth_member_id in ( select uth_member_id from dev.wc_tobacco_users_mdcr)
  and a.year = 2017 
 ;


alter table dev.wc_tobacco_cohort_mdcr add column tobacco_cessation_flag int2 default 0;


drop table dev.wc_tobacco_cessation_users_mdcr;

select distinct uth_member_id 
into dev.wc_tobacco_cessation_users_mdcr
from dev.wc_tobacco_cessation_mdcr ;

update dev.wc_tobacco_cohort_mdcr a set tobacco_cessation_flag = 1
  from dev.wc_tobacco_cessation_users_mdcr b 
    where b.uth_member_id = a.uth_member_id
 ;


delete from dev.wc_tobacco_cohort_mdcr where gender_cd not in ('M','F')


--prevalance of cessation overall 
select data_source , bus_cd, year, 'all', count(distinct uth_member_id), 
       sum(tobacco_flag) as tb_user, sum(tobacco_cessation_flag) as tb_cess,
      ( sum(tobacco_flag) / count(uth_member_id)::float ) as prev, 
      ( sum(tobacco_cessation_flag) / sum(tobacco_flag)::float )as cess_prev
from dev.wc_tobacco_cohort_mdcr
group by data_source, bus_cd, year
union 
select data_source , bus_cd, year, gender_cd, count(distinct uth_member_id), 
       sum(tobacco_flag) as tb_user, sum(tobacco_cessation_flag) as tb_cess,
      ( sum(tobacco_flag) / count(uth_member_id)::float ) as prev, 
      ( sum(tobacco_cessation_flag) / sum(tobacco_flag)::float )as cess_prev
from dev.wc_tobacco_cohort_mdcr
group by data_source, bus_cd, year, gender_cd



