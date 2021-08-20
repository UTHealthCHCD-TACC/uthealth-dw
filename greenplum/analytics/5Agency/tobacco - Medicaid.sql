
----find denominator: smoking population using dx codes

create table dev.wc_5a_smoking_dx (diag_cd varchar(20));

insert into dev.wc_5a_smoking_dx values 
('F17200'),('F17201'),('F17203'),('F17208'),('F17209'),('F17210'),('F17211'),('F17213'),('F17218'),('F17219'),('F17220'),('F17221'),
('F17223'),('F17228'),('F17229'),('F17290'),('F17291'),('F17293'),('F17298'),('F17299'),('T65211A'),('T65211D'),('T65211S'),('T65212A'),
('T65212D'),('T65212S'),('T65213A'),('T65213D'),('T65213S'),('T65214A'),('T65214D'),('T65214S'),('T65221A'),('T65221D'),('T65221S'),('T65222A'),
('T65222D'),('T65222S'),('T65223A'),('T65223D'),('T65223S'),('T65224A'),('T65224D'),('T65224S'),('T65291A'),('T65291D'),('T65291S'),('T65292A'),
('T65292D'),('T65292S'),('T65293A'),('T65293D'),('T65293S'),('T65294A'),('T65294D'),('T65294S'),('Z716'),('Z720'),('Z87891'),('P9681'),('P042'),
('O99330'),('O99331'),('O99332'),('O99333'),('O99334'),('O99335');


----get smokers
drop table if exists dev.wc_5a_smoking_temp;


---diag from claims - lookback 2 years 
select p.pcn, d.year_fy 
into dev.wc_5a_smoking_temp 
from medicaid.clm_dx d 
   join medicaid.clm_proc p 
      on d.icn = p.icn 
   join dev.wc_5a_smoking_dx a 
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
where d.year_fy between 2014 and 2020
;


---diag from enc
insert into dev.wc_5a_smoking_temp
select p.mem_id, d.year_fy 
from medicaid.enc_dx d 
   join medicaid.enc_proc p 
      on d.derv_enc = p.derv_enc 
   join dev.wc_5a_smoking_dx a 
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
where d.year_fy between 2014 and 2020
;


--cpt/hcpcs clm 
insert into dev.wc_5a_smoking_temp
select p.icn, d.year_fy 
from medicaid.clm_detail d 
  join medicaid.clm_proc p 
    on d.icn = p.icn 
   and d.year_fy = p.year_fy 
where d.year_fy between 2014 and 2020
  and d.proc_cd in ( '99406','99407','G0436','G0437','G9016','S9453','S4995','G9276','G9458',
'1034F','4004F','4001F','G9906','G9907','G9908','G9909');

--cpt/hcpcs enc 
insert into dev.wc_5a_smoking_temp
select p.mem_id, d.year_fy 
from medicaid.enc_det d 
  join medicaid.enc_proc p 
    on d.derv_enc = p.derv_enc 
   and d.year_fy = p.year_fy 
where d.year_fy between 2014 and 2020
  and d.proc_cd in ( '99406','99407','G0436','G0437','G9016','S9453','S4995','G9276','G9458',
'1034F','4004F','4001F','G9906','G9907','G9908','G9909');

--consolidation
drop table dev.wc_5a_smoking_members;


select distinct pcn, year_fy 
into dev.wc_5a_smoking_members
from dev.wc_5a_smoking_temp
;


select count(*), year_fy 
from dev.wc_5a_smoking_members
group by year_fy 
order by year_fy 
;


----*****************************************************************************************************************
--- smoking cessation codes - reporting year only 
----*****************************************************************************************************************

create table dev.wc_5a_smoking_counsel_dx (diag_cd varchar(20));

insert into dev.wc_5a_smoking_counsel_dx values 
('Z716');


drop table if exists dev.wc_5a_smoking_counsel_temp;

---diag from claims
select p.pcn, d.year_fy 
into dev.wc_5a_smoking_counsel_temp 
from medicaid.clm_dx d 
   join medicaid.clm_proc p 
      on d.icn = p.icn 
   join dev.wc_5a_smoking_counsel_dx a 
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
where d.year_fy between 2016 and 2020
;


---diag from enc
insert into dev.wc_5a_smoking_counsel_temp
select p.mem_id, d.year_fy 
from medicaid.enc_dx d 
   join medicaid.enc_proc p 
      on d.derv_enc = p.derv_enc 
   join dev.wc_5a_smoking_counsel_dx a 
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
where d.year_fy between 2016 and 2020
;



--cpt/hcpcs clm 
insert into dev.wc_5a_smoking_counsel_temp
select p.icn, d.year_fy 
from medicaid.clm_detail d 
  join medicaid.clm_proc p 
    on d.icn = p.icn 
   and d.year_fy = p.year_fy 
where d.year_fy between 2016 and 2020
  and d.proc_cd in ( '99406','99407','G0436','G0437','G9016','S9453','S4995','G9276','G9458','4004F','4001F');

--cpt/hcpcs enc 
insert into dev.wc_5a_smoking_counsel_temp
select p.mem_id, d.year_fy 
from medicaid.enc_det d 
  join medicaid.enc_proc p 
    on d.derv_enc = p.derv_enc 
   and d.year_fy = p.year_fy 
where d.year_fy between 2016 and 2020
  and d.proc_cd in ( '99406','99407','G0436','G0437','G9016','S9453','S4995','G9276','G9458','4004F','4001F');


---ndcs 
 
create table dev.wc_5a_smoking_ndc (ncd_cd varchar(20));

insert into dev.wc_5a_smoking_ndc values 
-- where a.ndc like  '50090%'

select * from dev.wc_5a_smoking_ndc;
 
--chip rx 
insert into dev.wc_5a_smoking_counsel_temp
select a.pcn, a.year_fy 
from medicaid.chip_rx a 
  join dev.wc_5a_smoking_ndc b 
    on b.ncd_cd = substring(a.ndc,1,9)
where a.year_fy between 2016 and 2020
 ;
 
--ffs rx
insert into dev.wc_5a_smoking_counsel_temp
select a.pcn, a.year_fy 
from medicaid.ffs_rx a 
  join dev.wc_5a_smoking_ndc b 
    on b.ncd_cd = substring(a.ndc,1,9)
where a.year_fy between 2016 and 2019
 ;

 
--mcro rx
insert into dev.wc_5a_smoking_counsel_temp
select a.pcn, a.year_fy 
from medicaid.mco_rx a 
  join dev.wc_5a_smoking_ndc b 
    on b.ncd_cd = substring(a.ndc,1,9)
where a.year_fy between 2016 and 2020
 ;

--consolidate
drop table if exists dev.wc_5a_smoking_counsel;
select distinct pcn, year_fy 
into dev.wc_5a_smoking_counsel
from dev.wc_5a_smoking_counsel_temp
;
 
---------------------------

----*****************************************************************************************************************
---  Final Values for spreadsheet
----*****************************************************************************************************************

--by program type 
with cte_dec as (
 select distinct client_nbr, year_fy 
  from medicaid.enrl 
  where elig_date in ( '201608','201708','201808','201908','202008')
) 
, cte_enrl as (  
   select client_nbr, enrl_fy , min(mco_program_nm) as mco_program_nm, min(sex) as sex, min(agegrp) as agegrp 
   from medicaid.agg_enrl_mdcd_fscyr 
   where smib = '0'
     and age >= 15
   group by client_nbr, enrl_fy 
) 
select replace( (a.enrl_fy::text ||  mco_program_nm), ' ','' )  as nv,
       count(distinct a.client_nbr) as denom , count(d.pcn) as smoking_counsel_numer
from cte_enrl a 
   join cte_dec b 
     on a.client_nbr = b.client_nbr 
    and a.enrl_fy = b.year_fy
  /* --smoking 
   left outer join dev.wc_5a_smoking_members d 
      on d.pcn = a.client_nbr 
     and d.year_fy = a.enrl_fy
     */
 --smoking cessation
   join dev.wc_5a_smoking_members c 
      on c.pcn = a.client_nbr 
     and c.year_fy = a.enrl_fy 
   left outer join dev.wc_5a_smoking_counsel d 
      on d.pcn = a.client_nbr 
     and d.year_fy = a.enrl_fy  
group by enrl_fy, mco_program_nm
order by enrl_fy, mco_program_nm
;


---overall dual eligible
with cte_dec as (
 select distinct client_nbr, year_fy 
  from medicaid.enrl 
  where elig_date in ( '201608','201708','201808','201908','202008')
) 
, cte_enrl as (  
   select client_nbr, enrl_fy , min(mco_program_nm) as mco_program_nm, min(sex) as sex, min(agegrp) as agegrp 
   from medicaid.agg_enrl_mdcd_fscyr 
   where smib = '1' 
    and age >=15
   group by client_nbr, enrl_fy 
) 
select replace( (ENRL_FY::text || 'DUAL ELIGIBLE'), ' ','' )  as nv,
      count(a.CLIENT_NBR) as uniq_den, count(d.pcn) as num
from cte_enrl a 
   join cte_dec b 
     on a.client_nbr = b.client_nbr 
    and a.enrl_fy = b.year_fy
  /* --smoking 
   left outer join dev.wc_5a_smoking_members d 
      on d.pcn = a.client_nbr 
     and d.year_fy = a.enrl_fy
     */
 --smoking cessation
   join dev.wc_5a_smoking_members c 
      on c.pcn = a.client_nbr 
     and c.year_fy = a.enrl_fy 
   left outer join dev.wc_5a_smoking_counsel d 
      on d.pcn = a.client_nbr 
     and d.year_fy = a.enrl_fy   
group by a.ENRL_FY
order by a.ENRL_FY
;


---by age group and medicaid type
with cte_dec as (
 select distinct client_nbr, year_fy 
  from medicaid.enrl 
  where elig_date in ( '201608','201708','201808','201908','202008')
) 
, cte_enrl as (  
   select client_nbr, enrl_fy , min(mco_program_nm) as mco_program_nm, min(sex) as sex, min(agegrp) as agegrp 
   from medicaid.agg_enrl_mdcd_fscyr 
   where smib = '0'
     and age >= 15
   group by client_nbr, enrl_fy 
) 
select replace( (a.ENRL_FY::text || MCO_PROGRAM_NM  || a.AgeGrp::text ), ' ','' )  as nv,
      count(a.CLIENT_NBR) as uniq_den, count(d.pcn) as num
from cte_enrl a 
   join cte_dec b 
     on a.client_nbr = b.client_nbr 
    and a.enrl_fy = b.year_fy
  /* --smoking 
   left outer join dev.wc_5a_smoking_members d 
      on d.pcn = a.client_nbr 
     and d.year_fy = a.enrl_fy
     */
 --smoking cessation
   join dev.wc_5a_smoking_members c 
      on c.pcn = a.client_nbr 
     and c.year_fy = a.enrl_fy 
   left outer join dev.wc_5a_smoking_counsel d 
      on d.pcn = a.client_nbr 
     and d.year_fy = a.enrl_fy  
group by a.ENRL_FY , a.MCO_PROGRAM_NM, a.AgeGrp 
order by a.ENRL_FY, a.MCO_PROGRAM_NM, a.AgeGrp ;


---by age group, gender, and medicaid type
with cte_dec as (
 select distinct client_nbr, year_fy 
  from medicaid.enrl 
  where elig_date in ( '201608','201708','201808','201908','202008')
) 
, cte_enrl as (  
   select client_nbr, enrl_fy , min(mco_program_nm) as mco_program_nm, min(sex) as sex, min(agegrp) as agegrp 
   from medicaid.agg_enrl_mdcd_fscyr 
   where smib = '0'
     and age >=15
   group by client_nbr, enrl_fy 
) 
select replace( (a.ENRL_FY::text || MCO_PROGRAM_NM || SEX || a.AgeGrp::text ), ' ','' )  as nv,
      count(a.CLIENT_NBR) as uniq_den, count(d.pcn) as num
from cte_enrl a 
   join cte_dec b 
     on a.client_nbr = b.client_nbr 
    and a.enrl_fy = b.year_fy
  /* --smoking 
   left outer join dev.wc_5a_smoking_members d 
      on d.pcn = a.client_nbr 
     and d.year_fy = a.enrl_fy
     */
 --smoking cessation
   join dev.wc_5a_smoking_members c 
      on c.pcn = a.client_nbr 
     and c.year_fy = a.enrl_fy 
   left outer join dev.wc_5a_smoking_counsel d 
      on d.pcn = a.client_nbr 
     and d.year_fy = a.enrl_fy       
where sex in ('M','F')
group by a.ENRL_FY , sex, a.MCO_PROGRAM_NM, a.AgeGrp  
order by a.ENRL_FY, sex, a.MCO_PROGRAM_NM, a.AgeGrp 
;
