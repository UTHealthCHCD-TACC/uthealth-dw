
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
insert into dev.wc_5a_obese_cohort
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
select distinct pcn, year_fy 
into dev.wc_5a_obese_members
from dev.wc_5a_obese_cohort 
;


select count(*), year_fy 
from dev.wc_5a_obese_temp
group by year_fy 
order by year_fy 
;


---------------------------

with cte_dec as (
 select distinct client_nbr, year_fy 
  from medicaid.enrl 
  where elig_date in ( '201608','201708','201808','201908')
) 
, cte_enrl as (  
   select client_nbr, enrl_fy , min(mco_program_nm) as mco_program_nm, min(sex) as sex, min(agegrp) as agegrp 
   from medicaid.agg_enrl_mcd_fscyr 
   group by client_nbr, enrl_fy 
) 
select replace( (a.enrl_fy::text ||  mco_program_nm), ' ','' )  as nv,
       count(distinct a.client_nbr) as denom , count(c.pcn) as obese_numerator
from cte_enrl a 
   join cte_dec b 
     on a.client_nbr = b.client_nbr 
    and a.enrl_fy = b.year_fy
   left outer join dev.wc_5a_obese_members c 
      on c.pcn = a.client_nbr 
     and c.year_fy = a.enrl_fy 
group by enrl_fy, mco_program_nm
order by enrl_fy, mco_program_nm
;


