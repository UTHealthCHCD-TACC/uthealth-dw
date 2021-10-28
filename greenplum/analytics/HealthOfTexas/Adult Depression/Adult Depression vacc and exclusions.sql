---run this script first for adult depression 

---get vaccinations
drop table if exists dev.wc_depression_2019_vacc_temp;
drop table if exists dev.wc_depression_2019_vacc;

--cpt hcpcs
select uth_member_id 
into dev.wc_depression_2019_vacc_temp
from data_warehouse.claim_detail a
where a.cpt_hcpcs_cd in ('96127','G8431','G8510','G0444','G8433','G8940')
      and a.year = 2019 
;

--diag 
insert into dev.wc_depression_2019_vacc_temp
select uth_member_id 
from data_warehouse.claim_diag a
where a.diag_cd like 'Z133%'
      and a.year = 2019 
;

--consolidate 
select distinct uth_member_id into dev.wc_depression_2019_vacc 
from dev.wc_depression_2019_vacc_temp;


---------**** Exclusions
drop table if exists dev.wc_depression_2019_exclusions;

select distinct uth_member_id 
into dev.wc_depression_2019_exclusions
from data_warehouse.claim_diag 
where ( 
          diag_cd like '296%' 
       or left(diag_cd,3) between 'F31' and 'F34' 
       or diag_cd in ('F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
       )
  and year = 2019
;


----**** / Exclusions 