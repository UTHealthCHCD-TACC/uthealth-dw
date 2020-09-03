
drop table dev.wc_temp_mdcd65_2017

create table dev.wc_temp_mdcd65_2017 ( client_num text, sex char(1), age text, zip3 char(3), vacc_flag text);


select count(*) , sum (vacc_flag::int) from dev.wc_temp_mdcd65_2017

update dev.wc_temp_mdcd65_2017 set vacc_flag = '0' where vacc_flag = '';

---optum and truven cohorts from DW
drop table dev.wc_flu_65plus_2017;

select uth_member_id, 
       a.gender_cd, 
       a.zip3,
       data_source 
 into dev.wc_flu_65plus_2017
from data_warehouse.member_enrollment_yearly a
where a.data_source in ('truv','optz','mdcr')
  and a.year = 2017 
  and a.state = 'TX'
  and a.age_derived >= 65
  and a.zip3 between '750' and '799'
  and a.total_enrolled_months = 12
  and a.gender_cd in ('M','F')
;


select distinct uth_member_id 
into dev.wc_flu_65plus_2017_vacc
from data_warehouse.claim_detail a
where a.procedure_cd in ('90630','90654','90655','90655','90656','90657','90658','90658','90660','90661','90662',
		  '90672','90672','90673','90674','90682','90685','90685','90686','90687','90688','90756','90756',
		  '90653','90657','90658','90658','G0008','Q2034','Q2035','Q2036','Q2037','Q2038','Q2039')
      and a.year = 2017 
      and a.uth_member_id in ( select uth_member_id from dev.wc_flu_65plus_2017)
;


alter table dev.wc_flu_65plus_2017 add column vacc_flag int2 default 0;


update dev.wc_flu_65plus_2017 a set vacc_flag = 1
  from dev.wc_flu_65plus_2017_vacc b 
    where b.uth_member_id = a.uth_member_id
 ;


drop table dev.wc_flu_65plus_all_2017



select * from dev.wc_flu_65plus_2017

select * 
into dev.wc_flu_65plus_all_2017
from ( 
select * from dev.wc_flu_65plus_2017
union 
select client_num::bigint, sex, zip3, 'mdcd' as data_source,  vacc_flag::int from dev.wc_temp_mdcd65_2017
) inr 


select count(*), sum(vacc_flag), data_source, count(distinct uth_member_id) as mem
from dev.wc_flu_65plus_all_2017 
group by data_source;


------ Calculations ---------------------------

----------------------------------------------------------------------------------------
---********************** Weights **************************
----------------------------------------------------------------------------------------
 		
CREATE OR REPLACE FUNCTION public.flu_weights ( )
RETURNS int AS $FUNC$	 
	declare
	r_data_source text; r_den float; r_num int; 	r_result float;
begin
	r_num := 0;
	r_den := (	select count(*)
			    from dev.wc_flu_65plus_all_2017 
			    where gender_cd = 'M'
				);


	for r_num , r_data_source
	  in 
	select count(*), data_source 
	from dev.wc_flu_65plus_all_2017 
	where gender_cd = 'M'
	group by data_source 
	order by data_source desc

	loop 
	    r_result = r_num / r_den;
	    raise notice 'Weight % is % ', r_data_source, r_result;
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

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev --, a.zip3 
from dev.wc_flu_65plus_all_2017 a 
where a.data_source = 'mdcd'
group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev --, a.zip3 
from dev.wc_flu_65plus_all_2017 a 
where a.data_source =  'mdcd'
  and a.gender_cd = 'F'
  group by a.zip3 
order by a.zip3
;


select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev --, a.zip3 
from dev.wc_flu_65plus_all_2017 a 
where a.data_source = 'mdcd'
  and a.gender_cd = 'M'
  group by a.zip3 
order by a.zip3
;

 
