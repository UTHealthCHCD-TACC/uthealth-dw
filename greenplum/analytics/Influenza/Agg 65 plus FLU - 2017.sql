---Influenza Vacc Prevalence 65+ for 2017---

drop table dev.wc_temp_mdcd65_flu_2017

create table dev.wc_temp_mdcd65_flu_2017 ( client_num text, fst_elig text, lst_elig text, zip3 char(3), sex char(1), age text, age_group text, vacc_flag text);

update dev.wc_temp_mdcd65_flu_2017 set vacc_flag = '0' where vacc_flag = '';

select count(*) , sum (vacc_flag::int) 
from dev.wc_temp_mdcd65_flu_2017



---optum and truven cohorts from DW
drop table dev.wc_flu_65plus_2017;

select uth_member_id, 
       a.gender_cd, 
       a.zip3,
       data_source 
 into dev.wc_flu_65plus_2017
from data_warehouse.member_enrollment_yearly a
where a.data_source in ('truv','optz')
  and a.year = 2017
  and a.state = 'TX'
  and a.age_derived >= 65
  and a.zip3 between '750' and '799'
  and a.total_enrolled_months = 12
  and a.gender_cd in ('M','F')
;

insert into dev.wc_flu_65plus_2017
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
  and a.year = 2017 
  and a.state = 'TX'
  and a.age_derived >= 65
  and a.zip3 between '750' and '799'
  and a.total_enrolled_months = 12
  and a.gender_cd in ('M','F')
;


----get vacc rates
drop table dev.wc_flu_65plus_2017_vacc;

select distinct uth_member_id 
into dev.wc_flu_65plus_2017_vacc
from data_warehouse.claim_detail a
where a.procedure_cd in ('90630','90654','90655','90655','90656','90657','90658','90658','90660','90661','90662',
		  				 '90672','90672','90673','90674','90682','90685','90685','90686','90687','90688','90756','90756',
		  				 '90653','90657','90658','90658','G0008','Q2034','Q2035','Q2036','Q2037','Q2038','Q2039','G8482')
      and a.year = 2017 
      and a.uth_member_id in ( select uth_member_id from dev.wc_flu_65plus_2017)
;




alter table dev.wc_flu_65plus_2017 add column vacc_flag int2 default 0;


update dev.wc_flu_65plus_2017 a set vacc_flag = 1
  from dev.wc_flu_65plus_2017_vacc b 
    where b.uth_member_id = a.uth_member_id
 ;


drop table dev.wc_flu_65plus_all_2017

select * 
into dev.wc_flu_65plus_all_2017
from ( 
select * from dev.wc_flu_65plus_2017
union 
select client_num::bigint, sex, zip3, 'mdcd' as data_source,  vacc_flag::int from dev.wc_temp_mdcd65_flu_2017
) inr 


select count(*) from dev.wc_flu_65plus_all_2017 

select count(*), sum(vacc_flag), data_source, count(distinct uth_member_id) as mem, 
( sum(vacc_flag) / count(uth_member_id)::float ) as prev
from dev.wc_flu_65plus_all_2017 
group by data_source;


------ Calculations ---------------------------

----------------------------------------------------------------------------------------
---********************** Weights **************************
----------------------------------------------------------------------------------------
 		
CREATE OR REPLACE FUNCTION public.flu_weights ( )
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
	r_den := (	select count(*) from dev.wc_flu_65plus_all_2017 );

	for r_num , r_data_source
	  in 
	select count(*), data_source 
	from dev.wc_flu_65plus_all_2017 
	group by data_source 
	order by data_source 
	
	loop 
	    r_result = r_num / r_den;
	    r_result = trunc(r_result,4);
	    raise notice 'Overall Weight % is % ', r_data_source, r_result;
	end loop;

---female 
	r_num := 0;
	r_den := (	select count(*) from dev.wc_flu_65plus_all_2017 where gender_cd = 'F' );

	for r_num , r_data_source
	  in 
	select count(*), data_source 
	from dev.wc_flu_65plus_all_2017 
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
	r_den := (	select count(*) from dev.wc_flu_65plus_all_2017 where gender_cd = 'M' );

	for r_num , r_data_source
	  in 
	select count(*), data_source 
	from dev.wc_flu_65plus_all_2017 
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
 

select public.flu_weights ();

   
select distinct gender_cd , data_source from dev.wc_flu_65plus_all_2017

----------------------------------------------------------------------------------------
---********************** Prevalance All **************************
----------------------------------------------------------------------------------------

--prevalance - row 51  optz truv mdcd mdcr
select ( sum(vacc_flag) / count(uth_member_id)::float ) as prev, count(uth_member_id), sum(vacc_flag), data_source
from dev.wc_flu_65plus_all_2017 a 
group by data_source
  order by data_source desc 
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id), sum(vacc_flag), data_source
from dev.wc_flu_65plus_all_2017 a  
where a.gender_cd = 'F'
  group by data_source
  order by data_source desc 
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id), sum(vacc_flag), data_source
from dev.wc_flu_65plus_all_2017 a 
where  a.gender_cd = 'M'
  group by data_source
    order by data_source desc 
;



----------------------------------------------------------------------------------------
---********************** Prevalance by ZIP **************************
----------------------------------------------------------------------------------------

--truven
select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev , a.zip3 
from dev.wc_flu_65plus_all_2017 a 
where a.data_source = 'truv'
group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev , a.zip3 
from dev.wc_flu_65plus_all_2017 a 
where a.data_source =  'truv'
  and a.gender_cd = 'F'
  group by a.zip3 
order by a.zip3
;


select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev , a.zip3 
from dev.wc_flu_65plus_all_2017 a 
where a.data_source = 'truv'
  and a.gender_cd = 'M'
  group by a.zip3 
order by a.zip3
;


---optum
--missing zip 753 772 
insert into dev.wc_flu_65plus_all_2017 values 
(0001,'M',753,'optz',0),
(0006,'F',753,'optz',0),
(0007,'M',772,'optz',0),
(0012,'F',772,'optz',0)
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev , a.zip3 
from dev.wc_flu_65plus_all_2017 a 
where a.data_source = 'optz'
group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev , a.zip3 
from dev.wc_flu_65plus_all_2017 a 
where a.data_source =  'optz'
  and a.gender_cd = 'F'
  group by a.zip3 
order by a.zip3
;


select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev , a.zip3 
from dev.wc_flu_65plus_all_2017 a 
where a.data_source = 'optz'
  and a.gender_cd = 'M'
  group by a.zip3 
order by a.zip3
;

 
---mdcr
select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev , a.zip3 
from dev.wc_flu_65plus_all_2017 a 
where a.data_source = 'mdcr'
group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev , a.zip3 
from dev.wc_flu_65plus_all_2017 a 
where a.data_source =  'mdcr'
  and a.gender_cd = 'F'
  group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev , a.zip3 
from dev.wc_flu_65plus_all_2017 a 
where a.data_source = 'mdcr'
  and a.gender_cd = 'M'
  group by a.zip3 
order by a.zip3
;


--mdcd
select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev , a.zip3 
from dev.wc_flu_65plus_all_2017 a 
where a.data_source = 'mdcd'
group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev , a.zip3 
from dev.wc_flu_65plus_all_2017 a 
where a.data_source =  'mdcd'
  and a.gender_cd = 'F'
  group by a.zip3 
order by a.zip3
;


select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev , a.zip3 
from dev.wc_flu_65plus_all_2017 a 
where a.data_source = 'mdcd'
  and a.gender_cd = 'M'
  group by a.zip3 
order by a.zip3
;