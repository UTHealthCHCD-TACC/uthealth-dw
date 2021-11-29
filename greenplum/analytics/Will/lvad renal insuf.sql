
---lvad checking renal insufficiency and adding dates 

create table dev.wc_lvad_mcrn (member_id text, less_RI char(1), severe_RI char(1), ex_lvad date, intra_lvad date, perc_lvad date);


create table dev.wc_lvad_optd (member_id text, less_RI char(1), severe_RI char(1), ex_lvad date, intra_lvad date, perc_lvad date);


---run import, then add columns
alter table dev.wc_lvad_mcrn add column less_RI_date date default null; 

alter table dev.wc_lvad_mcrn add column severe_RI_date date default null; 

alter table dev.wc_lvad_optd add column less_RI_date date default null; 

alter table dev.wc_lvad_optd add column severe_RI_date date default null; 


---------------------------------------------------------
--- ******* OPTD **********************
---------------------------------------------------------

--RI less severe
insert into dev.wc_lvad_RI_clms
select d.clmid, d.patid, min(fst_dt) as RI_date, 'less' as ri_sev 
from optum_dod.diagnostic d 
where d.diag in ('40311','40312','40313','40391','40392','40393','5851','5852','5853','5854','403','40301',
	'404','40401','I120','N181','N182','N183','N184','I129','I1310','I130')
  and extract(year from d.fst_dt) between 2014 and 2018 
group by d.clmid , d.patid	
;	
	
--RI severe
select d.clmid, d.patid, min(fst_dt) as RI_date, 'severe' as ri_sev 
into dev.wc_lvad_RI_clms
from optum_dod.diagnostic d 
where d.diag in ( '5855','5856','N185','N186','40402','40403','I1311','I132')
  and extract(year from d.fst_dt) between 2014 and 2018 
group by d.clmid , d.patid
  ;
  
 select count(*), count(distinct member_id) from dev.wc_lvad_optd
 
 ---optd update less severe
  with ri_cte as ( 
select member_id, case when less_ri_date is null then 'N' else 'Y' end as RI_less_flag, less_ri_date 
from (  
	select a.member_id, a.ex_lvad, a.intra_lvad, a.perc_lvad, min(ri_date) as less_RI_date
	from dev.wc_lvad_optd a  
	  left outer join dev.wc_lvad_RI_clms b  
	     on a.member_id = b.patid::text
	    and (    b.RI_date >= case when a.ex_lvad is null then '2050-01-01' else a.ex_lvad end  
	          or b.RI_date >= case when a.intra_lvad is null then '2050-01-01' else a.intra_lvad  end  
	          or b.RI_date >= case when   a.perc_lvad is null then '2050-01-01' else a.perc_lvad end ) 
	    and ri_sev = 'less'
	group by a.member_id, a.ex_lvad, a.intra_lvad, a.perc_lvad   
) inr 
)
update  dev.wc_lvad_optd a set less_RI = c.RI_less_flag, less_ri_date = c.less_ri_date 
   from ri_cte c 
  where c.member_id = a.member_id
;



---optd update severe
 with ri_cte as ( 
select member_id, case when sev_ri_date is null then 'N' else 'Y' end as RI_severe_flag, sev_ri_date 
from (  
	select a.member_id, a.ex_lvad, a.intra_lvad, a.perc_lvad, min(ri_date) as sev_RI_date
	from dev.wc_lvad_optd a  
	  left outer join dev.wc_lvad_RI_clms b  
	     on a.member_id = b.patid::text
	    and (    b.RI_date >= case when a.ex_lvad is null then '2050-01-01' else a.ex_lvad end  
	          or b.RI_date >= case when a.intra_lvad is null then '2050-01-01' else a.intra_lvad  end  
	          or b.RI_date >= case when   a.perc_lvad is null then '2050-01-01' else a.perc_lvad end ) 
	    and ri_sev = 'severe'
	group by a.member_id, a.ex_lvad, a.intra_lvad, a.perc_lvad   
) inr
)
update  dev.wc_lvad_optd a set severe_RI = c.RI_severe_flag, severe_ri_date = c.sev_ri_date 
   from ri_cte c 
  where c.member_id = a.member_id
;


---------------------------------------------------------
--- ******* MCRN **********************
---------------------------------------------------------

--RI less severe
insert into dev.wc_lvad_RI_clms_mcrn
select d.uth_claim_id , x.member_id_src,  min(date) as RI_date, 'less' as ri_sev 
from data_warehouse.claim_diag d 
   join data_warehouse.dim_uth_member_id x  
       on x.uth_member_id = d.uth_member_id 
where d.diag_cd in ('40311','40312','40313','40391','40392','40393','5851','5852','5853','5854','403','40301',
	'404','40401','I120','N181','N182','N183','N184','I129','I1310','I130')
  and extract(year from d.date) between 2014 and 2018 
  and d.data_source = 'mcrn'
group by d.uth_claim_id , x.member_id_src
;	
	
--RI severe
select d.uth_claim_id , x.member_id_src, min(date) as RI_date, 'severe' as ri_sev 
into dev.wc_lvad_RI_clms_mcrn
from  data_warehouse.claim_diag d 
   join data_warehouse.dim_uth_member_id x  
       on x.uth_member_id = d.uth_member_id 
where d.diag_cd in ( '5855','5856','N185','N186','40402','40403','I1311','I132')
  and extract(year from d.date) between 2014 and 2018 
  and d.data_source = 'mcrn'
group by d.uth_claim_id , x.member_id_src
  ;


 select * from dev.wc_lvad_mcrn;

select * from dev.wc_lvad_RI_clms_mcrn;
 
---mcrn less update
 with ri_cte as (  
select member_id, case when less_ri_date is null then 'N' else 'Y' end as RI_less_flag, less_ri_date 
from (  
	select a.member_id, a.ex_lvad, a.intra_lvad, a.perc_lvad, min(ri_date) as less_RI_date
	from dev.wc_lvad_mcrn a  
	  left outer join dev.wc_lvad_RI_clms_mcrn b  
	     on a.member_id = b.member_id_src
	    and (    b.RI_date >= case when a.ex_lvad is null then '2050-01-01' else a.ex_lvad end  
	          or b.RI_date >= case when a.intra_lvad is null then '2050-01-01' else a.intra_lvad  end  
	          or b.RI_date >= case when   a.perc_lvad is null then '2050-01-01' else a.perc_lvad end ) 
	    and ri_sev = 'less'
	group by a.member_id, a.ex_lvad, a.intra_lvad, a.perc_lvad   
) inr 
)
update  dev.wc_lvad_mcrn a set less_RI = c.RI_less_flag, less_ri_date = c.less_ri_date 
   from ri_cte c 
  where c.member_id = a.member_id
;


---mcrn severe update
 with ri_cte as (  
select member_id, case when sev_ri_date is null then 'N' else 'Y' end as RI_severe_flag, sev_ri_date 
from (  
	select a.member_id, a.ex_lvad, a.intra_lvad, a.perc_lvad, min(ri_date) as sev_RI_date
	from dev.wc_lvad_mcrn a  
	  left outer join dev.wc_lvad_RI_clms_mcrn b  
	     on a.member_id = b.member_id_src
	    and (    b.RI_date >= case when a.ex_lvad is null then '2050-01-01' else a.ex_lvad end  
	          or b.RI_date >= case when a.intra_lvad is null then '2050-01-01' else a.intra_lvad  end  
	          or b.RI_date >= case when   a.perc_lvad is null then '2050-01-01' else a.perc_lvad end ) 
	    and ri_sev = 'severe'
	group by a.member_id, a.ex_lvad, a.intra_lvad, a.perc_lvad   
) inr 
)
update  dev.wc_lvad_mcrn a set severe_RI = c.RI_severe_flag, severe_ri_date = c.sev_ri_date 
   from ri_cte c 
  where c.member_id = a.member_id
;



select count(*) from dev.wc_lvad_mcrn where severe_ri = 'Y' --and severe_ri_date is null;


select count(*) from dev.wc_lvad_optd --where severe_ri = 'Y'



