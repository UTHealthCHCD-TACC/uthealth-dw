select distinct pat_stat 
from dev.dw_clm_tmp1;

select *
from dev.dw_clm_tmp1 dct
where dw_alt_indivl_key = '33003303920'
order by adm_dt, dschrg_dt;

select *
from dev.dw_clm_final
where dw_alt_indivl_key = '33003303920'
order by adm_dt, dschrg_dt;

drop table dev.dw_clm_tmp2;
create table dev.dw_clm_tmp2
WITH (appendonly=true, orientation=column)
as
with recursive admitjoin (dw_alt_indivl_key, pat_stat, adm_dt, dschrg_dt) as
(
       select t1.dw_alt_indivl_key,
       		  t1.pat_stat,
              t1.adm_dt,
              t2.dschrg_dt
       from   dev.dw_clm_tmp1 t1
       join   dev.dw_clm_tmp1 t2
       on     t1.dw_alt_indivl_key = t2.dw_alt_indivl_key
       where  ((t2.adm_dt - t1.dschrg_dt = 1) or (t2.adm_dt between t1.adm_dt and t1.dschrg_dt ))
       and t1.pat_stat IN ('02', '05', '65', '82', '85', '88', '93', '94') 
    union
       select t3.dw_alt_indivl_key,
       	 	  t3.pat_stat,
              t3.adm_dt,
              t4.dschrg_dt
       from   admitjoin t3
       join   dev.dw_clm_tmp1 t4
       on     t3.dw_alt_indivl_key = t4.dw_alt_indivl_key
        where  ((t4.adm_dt - t3.dschrg_dt = 1) or (t4.adm_dt between t3.adm_dt and t3.dschrg_dt ))
       and t3.pat_stat IN ('02', '05', '65', '82', '85', '88', '93', '94' ) 
)
select distinct on (dw_alt_indivl_key, adm_dt) dw_alt_indivl_key,
		 adm_dt,
         max(dschrg_dt) as dschrg_dt 
         --min(adm_dt) as adm_dt 
from     admitjoin 
group by 1, 2
distributed by (dw_alt_indivl_key);

drop table dev.dw_clm_final;
create table dev.dw_clm_final
WITH (appendonly=true, orientation=column)
as
select dw_alt_indivl_key, dschrg_dt, min(adm_dt) as adm_dt
from dev.dw_clm_tmp2
group by 1, 2;


