--Annual Preventive Exam Aggregated 65 plus

---bring in medicaid data
drop table dev.wc_temp_mdcd65_ape_2016

create table dev.wc_temp_mdcd65_ape_2016 ( client_num text, fst_elig text, lst_elig text, zip3 char(3), sex char(1), age text, age_group text, vacc_flag text);


update dev.wc_temp_mdcd65_ape_2016 set vacc_flag = '0' where vacc_flag = '';

select count(*), sum(vacc_flag::int ) from dev.wc_temp_mdcd65_ape_2016

delete from dev.wc_temp_mdcd65_ape_2016 where sex = 'U';

select count(*) , sum (vacc_flag::int)
 from dev.wc_temp_mdcd65_ape_2016

---optum and truven cohorts from DW
drop table dev.wc_ape_65plus_2016;

select uth_member_id, 
       a.gender_cd, 
       a.zip3,
       data_source 
 into dev.wc_ape_65plus_2016
from data_warehouse.member_enrollment_yearly a
where a.data_source in ('truv','optz')
  and a.year = 2016
  and a.state = 'TX'
  and a.age_derived >= 65
  and a.zip3 between '750' and '799'
  and a.total_enrolled_months = 12
  and a.gender_cd in ('M','F')
;

insert into dev.wc_ape_65plus_2016
select a.uth_member_id, 
       a.gender_cd, 
       a.zip3,
       a.data_source 
from data_warehouse.member_enrollment_yearly a
  join data_warehouse.medicare_mbsf_abcd_enrollment b 
    on a.uth_member_id = b.uth_member_id 
   and b.bene_hi_cvrage_tot_mons = 12
   and b.bene_smi_cvrage_tot_mons > 0
   and b.year  = a.year  
where a.data_source = 'mdcr'
  and a.year = 2016 
  and a.state = 'TX'
  and a.age_derived >= 65
  and a.zip3 between '750' and '799'
  and a.total_enrolled_months = 12
  and a.gender_cd in ('M','F')
;

delete from dev.wc_ape_65plus_2016 where length(zip3::text) = 2;



---
drop table dev.wc_ape_65plus_2016_vacc;

select distinct uth_member_id 
into dev.wc_ape_65plus_2016_vacc
from data_warehouse.claim_detail a
where a.procedure_cd in ('99381','99382','99383','99384','99385','99386','99387',
						 '99391','99392','99393','99394','99395','99396','99397',
						 'S0610','S0612','S0615')
      and a.year = 2016 
      and a.uth_member_id in ( select uth_member_id from dev.wc_ape_65plus_2016)
;


insert into dev.wc_ape_65plus_2016_vacc
select distinct uth_member_id 
from data_warehouse.claim_diag 
where diag_cd in ('Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419',
				  'V700','V700','V7231','V705','V703','V7284','V7285') 
      and year = 2016 
      and uth_member_id in ( select uth_member_id from dev.wc_ape_65plus_2016)
      and uth_member_id not in ( select uth_member_id from dev.wc_ape_65plus_2016_vacc)
;


alter table dev.wc_ape_65plus_2016 add column vacc_flag int2 default 0;


update dev.wc_ape_65plus_2016 a set vacc_flag = 1
  from dev.wc_ape_65plus_2016_vacc b 
    where b.uth_member_id = a.uth_member_id
 ;

---combine medicaid and other data sets
drop table dev.wc_ape_65plus_all_2016


select * 
into dev.wc_ape_65plus_all_2016
from ( 
select * from dev.wc_ape_65plus_2016
union 
select client_num::bigint, sex, zip3, 'mdcd' as data_source,  vacc_flag::int from dev.wc_temp_mdcd65_ape_2016
) inr 


select count(*), sum(vacc_flag), data_source, count(distinct uth_member_id) as mem
from dev.wc_ape_65plus_all_2016 
group by data_source;

select * from dev.wc_ape_65plus_2016
------ Calculations ---------------------------

----------------------------------------------------------------------------------------
---********************** Weights **************************
----------------------------------------------------------------------------------------
 		
CREATE OR REPLACE FUNCTION public.ape_weights ( )
RETURNS int AS $FUNC$	 
	declare
	r_data_source text; 
	r_den numeric;
	r_num numeric; 
	r_result numeric;
	r_ag int;
begin
	
---all
	r_num := 0;
	r_den := (	select count(*) from dev.wc_ape_65plus_all_2016 );

	for r_num , r_data_source
	  in 
	select count(*), data_source 
	from dev.wc_ape_65plus_all_2016
	group by data_source 
	order by data_source 
	
	loop 
	    r_result = r_num / r_den;
	    r_result = trunc(r_result,4);
	    raise notice 'Overall Weight % is % ', r_data_source, r_result;
	end loop;

---female 
	r_num := 0;
	r_den := (	select count(*) from dev.wc_ape_65plus_all_2016 where gender_cd = 'F' );

	for r_num , r_data_source
	  in 
	select count(*), data_source 
	from dev.wc_ape_65plus_all_2016
	where gender_cd = 'F'
	group by data_source 
	order by data_source 
	
	loop 
	    r_result = r_num / r_den;
	    r_result = trunc(r_result,4);
	    raise notice 'Female Weight % is % ', r_data_source, r_result;
	end loop;

---male 
	r_num := 0;
	r_den := (	select count(*) from dev.wc_ape_65plus_all_2016 where gender_cd = 'M' );

	for r_num , r_data_source
	  in 
	select count(*), data_source 
	from dev.wc_ape_65plus_all_2016
	where gender_cd = 'M'
	group by data_source 
	order by data_source 
	
	loop 
	    r_result = r_num / r_den;
	    r_result = trunc(r_result,4);
	    raise notice 'Male Weight % is % ', r_data_source, r_result;
	end loop;


	return 0;
end $FUNC$ language 'plpgsql';
 

select public.ape_weights ();

   
select distinct gender_cd ,count(*), data_source from dev.wc_ape_65plus_all_2016 group by gender_cd, data_source;

----------------------------------------------------------------------------------------
---********************** Prevalance All **************************
----------------------------------------------------------------------------------------

--prevalance - row 51  optz truv mdcd mdcr
select ( sum(vacc_flag) / count(uth_member_id)::float ) as prev, count(uth_member_id), sum(vacc_flag), data_source
from dev.wc_ape_65plus_all_2016 a 
group by data_source
  order by data_source desc 
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id), sum(vacc_flag), data_source
from dev.wc_ape_65plus_all_2016 a  
where a.gender_cd = 'F'
  group by data_source
  order by data_source desc 
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id), sum(vacc_flag), data_source
from dev.wc_ape_65plus_all_2016 a 
where  a.gender_cd = 'M'
  group by data_source
    order by data_source desc 
;



----------------------------------------------------------------------------------------
---********************** Prevalance by ZIP **************************
----------------------------------------------------------------------------------------

---truv
select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev , a.zip3 
from dev.wc_ape_65plus_all_2016 a 
where a.data_source = 'truv'
group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev , a.zip3 
from dev.wc_ape_65plus_all_2016 a 
where a.data_source =  'truv'
  and a.gender_cd = 'F'
  group by a.zip3 
order by a.zip3
;


select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev , a.zip3 
from dev.wc_ape_65plus_all_2016 a 
where a.data_source = 'truv'
  and a.gender_cd = 'M'
  group by a.zip3 
order by a.zip3
;

---optum
--missing zip 753 772 
insert into dev.wc_ape_65plus_all_2016 values 
(0001,'M',753,'optz',0),
(0006,'F',753,'optz',0),
(0007,'M',772,'optz',0),
(0012,'F',772,'optz',0)
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev , a.zip3 
from dev.wc_ape_65plus_all_2016 a 
where a.data_source = 'optz'
group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev , a.zip3 
from dev.wc_ape_65plus_all_2016 a 
where a.data_source =  'optz'
  and a.gender_cd = 'F'
  group by a.zip3 
order by a.zip3
;


select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev , a.zip3 
from dev.wc_ape_65plus_all_2016 a 
where a.data_source = 'optz'
  and a.gender_cd = 'M'
  group by a.zip3 
order by a.zip3
;
 

---medicare
select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev , a.zip3 
from dev.wc_ape_65plus_all_2016 a 
where a.data_source = 'mdcr'
group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev , a.zip3 
from dev.wc_ape_65plus_all_2016 a 
where a.data_source =  'mdcr'
  and a.gender_cd = 'F'
  group by a.zip3 
order by a.zip3
;


select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev , a.zip3 
from dev.wc_ape_65plus_all_2016 a 
where a.data_source = 'mdcr'
  and a.gender_cd = 'M'
  group by a.zip3 
order by a.zip3
;

---medicaid
select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev , a.zip3 
from dev.wc_ape_65plus_all_2016 a 
where a.data_source = 'mdcd'
group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev , a.zip3 
from dev.wc_ape_65plus_all_2016 a 
where a.data_source =  'mdcd'
  and a.gender_cd = 'F'
  group by a.zip3 
order by a.zip3
;


select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev , a.zip3 
from dev.wc_ape_65plus_all_2016 a 
where a.data_source = 'mdcd'
  and a.gender_cd = 'M'
  group by a.zip3 
order by a.zip3
;
