
----find denominator: obese population using dx codes

create table dev.wc_5a_obese_dx (diag_cd varchar(20));

insert into dev.wc_5a_obese_dx values 
('E660'),('E661'),('E662'),('E668'),('E669'),('27800'),('27801'),('V853'),
('V8530'),('V8531'),('V8532'),('V8533'),('V8534'),('V8535'),('V8536'),('V8537'),('V8538'),
('V8539'),('V854'),('Z6830'),
('Z6831'),('Z6832'),('Z6833'),('Z6834'),('Z6835'),('Z6836'),('Z6837'),
('Z6838'),('Z6839'),('Z6841'),('Z6842'),('Z6843'),('Z6844'),('Z6845');


drop table if exists dev.wc_5a_obese_temp;



---diag from claims
select p.pcn, d.year_fy 
into dev.wc_5a_obese_temp 
from medicaid.clm_dx d 
   join medicaid.clm_proc p 
      on d.icn = p.icn 
   join dev.wc_5a_obese_dx a 
      on a.diag_cd = d.prim_dx_cd 
 	or a.diag_cd = d.adm_dx_cd 
 	or a.diag_cd = d.dx_cd_1 
    or a.diag_cd = d.dx_cd_2 
    or a.diag_cd = d.dx_cd_3 
    or a.diag_cd = d.dx_cd_4 
    or a.diag_cd = d.dx_cd_5 
    or a.diag_cd = d.dx_cd_6  
    or a.diag_cd = d.dx_cd_7 
    or a.diag_cd = d.dx_cd_8
    or a.diag_cd = d.dx_cd_9 
    or a.diag_cd = d.dx_cd_10
    or a.diag_cd = d.dx_cd_11
    or a.diag_cd = d.dx_cd_12 
    or a.diag_cd = d.dx_cd_13 
    or a.diag_cd = d.dx_cd_14 
    or a.diag_cd = d.dx_cd_15 
    or a.diag_cd = d.dx_cd_16 
    or a.diag_cd = d.dx_cd_17 
    or a.diag_cd = d.dx_cd_18 
    or a.diag_cd = d.dx_cd_19 
    or a.diag_cd = d.dx_cd_20
    or a.diag_cd = d.dx_cd_21
    or a.diag_cd = d.dx_cd_22
    or a.diag_cd = d.dx_cd_23
    or a.diag_cd = d.dx_cd_24
    or a.diag_cd = d.dx_cd_25
where d.year_fy between 2016 and 2019 
;


---diag from enc
insert into dev.wc_5a_obese_temp
select p.mem_id, d.year_fy 
from medicaid.enc_dx d 
   join medicaid.enc_proc p 
      on d.derv_enc = p.derv_enc 
   join dev.wc_5a_obese_dx a 
      on a.diag_cd = d.prim_dx_cd 
 	or a.diag_cd = d.adm_dx_cd 
 	or a.diag_cd = d.dx_cd_1 
    or a.diag_cd = d.dx_cd_2 
    or a.diag_cd = d.dx_cd_3 
    or a.diag_cd = d.dx_cd_4 
    or a.diag_cd = d.dx_cd_5 
    or a.diag_cd = d.dx_cd_6  
    or a.diag_cd = d.dx_cd_7 
    or a.diag_cd = d.dx_cd_8
    or a.diag_cd = d.dx_cd_9 
    or a.diag_cd = d.dx_cd_10
    or a.diag_cd = d.dx_cd_11
    or a.diag_cd = d.dx_cd_12 
    or a.diag_cd = d.dx_cd_13 
    or a.diag_cd = d.dx_cd_14 
    or a.diag_cd = d.dx_cd_15 
    or a.diag_cd = d.dx_cd_16 
    or a.diag_cd = d.dx_cd_17 
    or a.diag_cd = d.dx_cd_18 
    or a.diag_cd = d.dx_cd_19 
    or a.diag_cd = d.dx_cd_20
    or a.diag_cd = d.dx_cd_21
    or a.diag_cd = d.dx_cd_22
    or a.diag_cd = d.dx_cd_23
    or a.diag_cd = d.dx_cd_24
where d.year_fy between 2016 and 2019 
;


--consolidation
drop table dev.wc_5a_obese_members;


select distinct pcn, year_fy 
into dev.wc_5a_obese_members
from dev.wc_5a_obese_temp
;


select count(*), year_fy 
from dev.wc_5a_obese_members
group by year_fy 
order by year_fy 
;


----*****************************************************************************************************************
--- weight counselling codes
----*****************************************************************************************************************

create table dev.wc_5a_obese_counsel_dx (diag_cd varchar(20));

insert into dev.wc_5a_obese_counsel_dx values 
('Z713'),('Z7189'),('V653');


drop table if exists dev.wc_5a_obese_counsel_temp;

---diag from claims
select p.pcn, d.year_fy 
into dev.wc_5a_obese_counsel_temp 
from medicaid.clm_dx d 
   join medicaid.clm_proc p 
      on d.icn = p.icn 
   join dev.wc_5a_obese_counsel_dx a 
      on a.diag_cd = d.prim_dx_cd 
 	or a.diag_cd = d.adm_dx_cd 
 	or a.diag_cd = d.dx_cd_1 
    or a.diag_cd = d.dx_cd_2 
    or a.diag_cd = d.dx_cd_3 
    or a.diag_cd = d.dx_cd_4 
    or a.diag_cd = d.dx_cd_5 
    or a.diag_cd = d.dx_cd_6  
    or a.diag_cd = d.dx_cd_7 
    or a.diag_cd = d.dx_cd_8
    or a.diag_cd = d.dx_cd_9 
    or a.diag_cd = d.dx_cd_10
    or a.diag_cd = d.dx_cd_11
    or a.diag_cd = d.dx_cd_12 
    or a.diag_cd = d.dx_cd_13 
    or a.diag_cd = d.dx_cd_14 
    or a.diag_cd = d.dx_cd_15 
    or a.diag_cd = d.dx_cd_16 
    or a.diag_cd = d.dx_cd_17 
    or a.diag_cd = d.dx_cd_18 
    or a.diag_cd = d.dx_cd_19 
    or a.diag_cd = d.dx_cd_20
    or a.diag_cd = d.dx_cd_21
    or a.diag_cd = d.dx_cd_22
    or a.diag_cd = d.dx_cd_23
    or a.diag_cd = d.dx_cd_24
    or a.diag_cd = d.dx_cd_25
where d.year_fy between 2016 and 2019 
;


---diag from enc
insert into dev.wc_5a_obese_counsel_temp
select p.mem_id, d.year_fy 
from medicaid.enc_dx d 
   join medicaid.enc_proc p 
      on d.derv_enc = p.derv_enc 
   join dev.wc_5a_obese_counsel_dx a 
      on a.diag_cd = d.prim_dx_cd 
 	or a.diag_cd = d.adm_dx_cd 
 	or a.diag_cd = d.dx_cd_1 
    or a.diag_cd = d.dx_cd_2 
    or a.diag_cd = d.dx_cd_3 
    or a.diag_cd = d.dx_cd_4 
    or a.diag_cd = d.dx_cd_5 
    or a.diag_cd = d.dx_cd_6  
    or a.diag_cd = d.dx_cd_7 
    or a.diag_cd = d.dx_cd_8
    or a.diag_cd = d.dx_cd_9 
    or a.diag_cd = d.dx_cd_10
    or a.diag_cd = d.dx_cd_11
    or a.diag_cd = d.dx_cd_12 
    or a.diag_cd = d.dx_cd_13 
    or a.diag_cd = d.dx_cd_14 
    or a.diag_cd = d.dx_cd_15 
    or a.diag_cd = d.dx_cd_16 
    or a.diag_cd = d.dx_cd_17 
    or a.diag_cd = d.dx_cd_18 
    or a.diag_cd = d.dx_cd_19 
    or a.diag_cd = d.dx_cd_20
    or a.diag_cd = d.dx_cd_21
    or a.diag_cd = d.dx_cd_22
    or a.diag_cd = d.dx_cd_23
    or a.diag_cd = d.dx_cd_24
where d.year_fy between 2016 and 2019 
;



--cpt/hcpcs clm 
insert into dev.wc_5a_obese_counsel_temp
select p.icn, d.year_fy 
from medicaid.clm_detail d 
  join medicaid.clm_proc p 
    on d.icn = p.icn 
   and d.year_fy = p.year_fy 
where d.year_fy between 2016 and 2019
  and d.proc_cd in ( '43770','43644','43645','43842','43843','43845','43846','43847','43659','S2082','S2085','43645',
'43771','43772','43774','43775','43848','43886','43887','43888');

--cpt/hcpcs enc 
insert into dev.wc_5a_obese_counsel_temp
select p.mem_id, d.year_fy 
from medicaid.enc_det d 
  join medicaid.enc_proc p 
    on d.derv_enc = p.derv_enc 
   and d.year_fy = p.year_fy 
where d.year_fy between 2016 and 2019
  and d.proc_cd in ( '43770','43644','43645','43842','43843','43845','43846','43847','43659','S2082','S2085','43645',
'43771','43772','43774','43775','43848','43886','43887','43888');

--icd proc 
create table dev.wc_5a_obese_counsel_proc (diag_cd varchar(20));

insert into dev.wc_5a_obese_counsel_proc values 
('4389'),('443'),('4431'),('4438'),('4439'),('4468'),('4495'),('4496'),('4497'),('4499'),('445'),('4551'),('4521'),
('0DV60CZ'),('0DV60DZ'),('0DV63CZ'),('0DV63DZ'),('0DV64CZ'),('0DV64DZ'),('0DV67DZ'),('0DV68DZ')
;

--icd proc from clms 
insert into dev.wc_5a_obese_counsel_temp
select d.pcn , d.year_fy 
from medicaid.clm_proc d 
join dev.wc_5a_obese_counsel_proc a 
      on   a.diag_cd = d.proc_icd_cd_1 
    or a.diag_cd = d.proc_icd_cd_2 
    or a.diag_cd = d.proc_icd_cd_3 
    or a.diag_cd = d.proc_icd_cd_4 
    or a.diag_cd = d.proc_icd_cd_5 
    or a.diag_cd = d.proc_icd_cd_6  
    or a.diag_cd = d.proc_icd_cd_7 
    or a.diag_cd = d.proc_icd_cd_8
    or a.diag_cd = d.proc_icd_cd_9 
    or a.diag_cd = d.proc_icd_cd_10
    or a.diag_cd = d.proc_icd_cd_11
    or a.diag_cd = d.proc_icd_cd_12 
    or a.diag_cd = d.proc_icd_cd_13 
    or a.diag_cd = d.proc_icd_cd_14 
    or a.diag_cd = d.proc_icd_cd_15 
    or a.diag_cd = d.proc_icd_cd_16 
    or a.diag_cd = d.proc_icd_cd_17 
    or a.diag_cd = d.proc_icd_cd_18 
    or a.diag_cd = d.proc_icd_cd_19 
    or a.diag_cd = d.proc_icd_cd_20
    or a.diag_cd = d.proc_icd_cd_21
    or a.diag_cd = d.proc_icd_cd_22
    or a.diag_cd = d.proc_icd_cd_23
    or a.diag_cd = d.proc_icd_cd_24
    or a.diag_cd = d.proc_icd_cd_25
where d.year_fy between 2016 and 2019 ;


--icd proc from enc 
insert into dev.wc_5a_obese_counsel_temp
select d.mem_id , d.year_fy 
from medicaid.enc_proc d 
join dev.wc_5a_obese_counsel_proc a 
      on a.diag_cd = d.prim_proc_cd  
    or a.diag_cd = d.proc_icd_cd_1 
    or a.diag_cd = d.proc_icd_cd_2 
    or a.diag_cd = d.proc_icd_cd_3 
    or a.diag_cd = d.proc_icd_cd_4 
    or a.diag_cd = d.proc_icd_cd_5 
    or a.diag_cd = d.proc_icd_cd_6  
    or a.diag_cd = d.proc_icd_cd_7 
    or a.diag_cd = d.proc_icd_cd_8
    or a.diag_cd = d.proc_icd_cd_9 
    or a.diag_cd = d.proc_icd_cd_10
    or a.diag_cd = d.proc_icd_cd_11
    or a.diag_cd = d.proc_icd_cd_12 
    or a.diag_cd = d.proc_icd_cd_13 
    or a.diag_cd = d.proc_icd_cd_14 
    or a.diag_cd = d.proc_icd_cd_15 
    or a.diag_cd = d.proc_icd_cd_16 
    or a.diag_cd = d.proc_icd_cd_17 
    or a.diag_cd = d.proc_icd_cd_18 
    or a.diag_cd = d.proc_icd_cd_19 
    or a.diag_cd = d.proc_icd_cd_20
    or a.diag_cd = d.proc_icd_cd_21
    or a.diag_cd = d.proc_icd_cd_22
    or a.diag_cd = d.proc_icd_cd_23
    or a.diag_cd = d.proc_icd_cd_24
where d.year_fy between 2016 and 2019 ;


---drg from clm - none found 


---drg from enc
insert into dev.wc_5a_obese_counsel_temp
select p.mem_id , p.year_fy 
from  medicaid.enc_proc p 
where p.year_fy between 2016 and 2019
  and p.drg in ('0619','0620','0621');


--consolidate
select distinct pcn, year_fy 
into dev.wc_5a_obese_counsel
from dev.wc_5a_obese_counsel_temp
;
 
---------------------------

----*****************************************************************************************************************
---  Final Values for spreadsheet
----*****************************************************************************************************************

--by program type 
with cte_dec as (
 select distinct client_nbr, year_fy 
  from medicaid.enrl 
  where elig_date in ( '201608','201708','201808','201908')
) 
, cte_enrl as (  
   select client_nbr, enrl_fy , min(mco_program_nm) as mco_program_nm, min(sex) as sex, min(agegrp) as agegrp 
   from medicaid.agg_enrl_mcd_fscyr 
   where smib = '0'
   group by client_nbr, enrl_fy 
) 
select replace( (a.enrl_fy::text ||  mco_program_nm), ' ','' )  as nv,
       count(distinct a.client_nbr) as denom , count(d.pcn) as obese_counsel_numer
from cte_enrl a 
   join cte_dec b 
     on a.client_nbr = b.client_nbr 
    and a.enrl_fy = b.year_fy
   join dev.wc_5a_obese_members c 
      on c.pcn = a.client_nbr 
     and c.year_fy = a.enrl_fy 
   left outer join dev.wc_5a_obese_counsel d 
      on d.pcn = a.client_nbr 
     and d.year_fy = a.enrl_fy 
group by enrl_fy, mco_program_nm
order by enrl_fy, mco_program_nm
;


---overall dual eligible
with cte_dec as (
 select distinct client_nbr, year_fy 
  from medicaid.enrl 
  where elig_date in ( '201608','201708','201808','201908')
) 
, cte_enrl as (  
   select client_nbr, enrl_fy , min(mco_program_nm) as mco_program_nm, min(sex) as sex, min(agegrp) as agegrp 
   from medicaid.agg_enrl_mcd_fscyr 
   where smib = '1' 
   group by client_nbr, enrl_fy 
) 
select replace( (ENRL_FY::text || 'DUAL ELIGIBLE'), ' ','' )  as nv,
      count(a.CLIENT_NBR) as uniq_den, count(d.pcn) as num
from cte_enrl a 
   join cte_dec b 
     on a.client_nbr = b.client_nbr 
    and a.enrl_fy = b.year_fy
   join dev.wc_5a_obese_members c 
      on c.pcn = a.client_nbr 
     and c.year_fy = a.enrl_fy 
   left outer join dev.wc_5a_obese_counsel d 
      on d.pcn = a.client_nbr 
     and d.year_fy = a.enrl_fy      
group by a.ENRL_FY
order by a.ENRL_FY
;


---by age group and medicaid type
with cte_dec as (
 select distinct client_nbr, year_fy 
  from medicaid.enrl 
  where elig_date in ( '201608','201708','201808','201908')
) 
, cte_enrl as (  
   select client_nbr, enrl_fy , min(mco_program_nm) as mco_program_nm, min(sex) as sex, min(agegrp) as agegrp 
   from medicaid.agg_enrl_mcd_fscyr 
   where smib = '0'
   group by client_nbr, enrl_fy 
) 
select replace( (a.ENRL_FY::text || MCO_PROGRAM_NM  || a.AgeGrp::text ), ' ','' )  as nv,
      count(a.CLIENT_NBR) as uniq_den, count(d.pcn) as num
from cte_enrl a 
   join cte_dec b 
     on a.client_nbr = b.client_nbr 
    and a.enrl_fy = b.year_fy
   join dev.wc_5a_obese_members c 
      on c.pcn = a.client_nbr 
     and c.year_fy = a.enrl_fy 
   left outer join dev.wc_5a_obese_counsel d 
      on d.pcn = a.client_nbr 
     and d.year_fy = a.enrl_fy        
group by a.ENRL_FY , a.MCO_PROGRAM_NM, a.AgeGrp 
order by a.ENRL_FY, a.MCO_PROGRAM_NM, a.AgeGrp ;


---by age group, gender, and medicaid type
with cte_dec as (
 select distinct client_nbr, year_fy 
  from medicaid.enrl 
  where elig_date in ( '201608','201708','201808','201908')
) 
, cte_enrl as (  
   select client_nbr, enrl_fy , min(mco_program_nm) as mco_program_nm, min(sex) as sex, min(agegrp) as agegrp 
   from medicaid.agg_enrl_mcd_fscyr 
   where smib = '0'
   group by client_nbr, enrl_fy 
) 
select replace( (a.ENRL_FY::text || MCO_PROGRAM_NM || SEX || a.AgeGrp::text ), ' ','' )  as nv,
      count(a.CLIENT_NBR) as uniq_den, count(d.pcn) as num
from cte_enrl a 
   join cte_dec b 
     on a.client_nbr = b.client_nbr 
    and a.enrl_fy = b.year_fy
   join dev.wc_5a_obese_members c 
      on c.pcn = a.client_nbr 
     and c.year_fy = a.enrl_fy 
   left outer join dev.wc_5a_obese_counsel d 
      on d.pcn = a.client_nbr 
     and d.year_fy = a.enrl_fy         
where sex in ('M','F')
group by a.ENRL_FY , sex, a.MCO_PROGRAM_NM, a.AgeGrp  
order by a.ENRL_FY, sex, a.MCO_PROGRAM_NM, a.AgeGrp 
;
