
drop table dev.wc_tobacco_diags

---icd
select distinct d.uth_member_id 
into dev.wc_tobacco_diags
from data_warehouse.claim_diag d 
where ( d.diag_cd like'F1720%'
		or d.diag_cd like'F1721%'
		or d.diag_cd like'F1722%'
		or d.diag_cd like'F1729%'
		or d.diag_cd like'Z716%'
		or d.diag_cd like'O9933%'
		or d.diag_cd like'T652%'
		or d.diag_cd in ('Z716','Z720','Z87891','P9681','P042') ) 
and d.data_source in ('truv','optz')
and d.year between 2016 and 2018
;

---cpt
insert into dev.wc_tobacco_diags
select distinct uth_member_id 
from data_warehouse.claim_detail a 
where a.procedure_cd in ( '99406','99407','G0436','G0437','G9016','S9453','S4995','G9276','G9458',
				  '1034F','4004F','4001F','G9906','G9907','G9908','G9909')
  and a.data_source in ('truv','optz')
  and a.year between 2016 and 2018			  
;				  


---rx 
create table dev.wc_tobacco_ndcs (ndc_cd text);

select count(*) from dev.wc_tobacco_ndcs
 
--rx
insert into dev.wc_tobacco_diags
select distinct a.uth_member_id 
from data_warehouse.pharmacy_claims a 
where a.ndc in ( select ndc_cd from dev.wc_tobacco_ndcs)
 and a.data_source in ('truv','optz')
  and a.year between 2017 and 2018	 
 
 
---cohorts
drop table  dev.wc_tobacco_cohort_raw
  
select a.uth_member_id , a.data_source , a.bus_cd , a.age_derived , a.year, a.gender_cd, a.zip3 
into dev.wc_tobacco_cohort_raw
from data_warehouse.member_enrollment_yearly a 
where a.data_source in ('optz','truv')
and a.year = 2016 
and a.age_derived >= 15
and a.enrolled_dec = true
and a.state = 'TX' 
  and a.zip3 between '750' and '799'
;



select count(*), count(distinct uth_member_id), data_source, bus_cd, year , gender_cd , zip3
from dev.wc_tobacco_cohort_raw
group by data_source, bus_cd, year, gender_cd , zip3
order by data_source, bus_cd, gender_cd , zip3
;


alter table dev.wc_tobacco_cohort_raw add column tobacco_flag int2 default 0;

select distinct uth_member_id 
into dev.wc_tobacco_users
from dev.wc_tobacco_diags 

update dev.wc_tobacco_cohort_raw a set tobacco_flag = 1
  from dev.wc_tobacco_users b 
    where b.uth_member_id = a.uth_member_id
 ;

select count(*), count(distinct uth_member_id), sum(tobacco_flag) as tb_user, ( sum(tobacco_flag) / count(uth_member_id)::float ) as prev, data_source, bus_cd, year , gender_cd 
from dev.wc_tobacco_cohort_raw
group by data_source, bus_cd, year, gender_cd 
order by data_source, bus_cd, gender_cd 
;

select count(*), count(distinct uth_member_id), sum(tobacco_flag) as tb_user, ( sum(tobacco_flag) / count(uth_member_id)::float ) as prev, data_source, bus_cd
from dev.wc_tobacco_cohort_raw
group by data_source, bus_cd
order by data_source, bus_cd
;

select distinct ahfsclss, gnrc_nm 
from optum_zip.lu_ndc where ndc in ( select ndc_cd from dev.wc_tobacco_ndcs)

where ahfsclss in ( '12920000', '28160492');
  
