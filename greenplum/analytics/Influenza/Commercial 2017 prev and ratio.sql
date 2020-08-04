---optum and truven cohorts from DW
drop table dev.wc_flu_com_2017_vacc;

select uth_member_id, 
       a.zip3, 
	   case  when a.age_derived between 0  and 17 then 1 
	         when a.age_derived between 18 and 29 then 2
	         when a.age_derived between 30 and 39 then 3
	         when a.age_derived between 40 and 49 then 4
	         when a.age_derived between 50 and 59 then 5 
	         when a.age_derived between 60 and 64 then 6 
	   end as age_group,
       a.gender_cd, 
       data_source 
 into dev.wc_flu_com_2017
from data_warehouse.member_enrollment_yearly a
where a.data_source in ('truv','optz')
  and a.year = 2017 
  and a.state = 'TX'
  and a.zip3 between '750' and '799'
  and a.age_derived < 65
  and a.bus_cd = 'COM'
  and a.total_enrolled_months = 12
  and a.gender_cd in ('M','F')
;


delete from dev.wc_flu_com_2017 where age_group is null;

select distinct uth_member_id 
into dev.wc_flu_com_2017_vacc
from data_warehouse.claim_detail a
where a.procedure_cd in ('90630','90654','90655','90655','90656','90657','90658','90658','90660','90661','90662',
		  '90672','90672','90673','90674','90682','90685','90685','90686','90687','90688','90756','90756',
		  '90653','90657','90658','90658','G0008','Q2034','Q2035','Q2036','Q2037','Q2038','Q2039')
      and a.year = 2017 
      and a.uth_member_id in ( select uth_member_id from dev.wc_flu_com_2017)
;



alter table dev.wc_flu_com_2017 add column vacc_flag int2 default 0;


update dev.wc_flu_com_2017 a set vacc_flag = 1
  from dev.wc_flu_com_2017_vacc b 
    where b.uth_member_id = a.uth_member_id
 ;

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
			    from dev.wc_flu_com_2017 
			    --where gender_cd = 'M'
			    where age_group = '6'
				);


	for r_num , r_data_source
	  in 
	select count(*), data_source 
	from dev.wc_flu_com_2017 
	--where gender_cd = 'M'
	where age_group = '6'
	group by data_source 

	loop 
	    r_result = r_num / r_den;
	    raise notice 'Weight % is % ', r_data_source, r_result;
	end loop;

	return 0;
end $FUNC$ language 'plpgsql';
 

select public.flu_weights ();

   
			    
----------------------------------------------------------------------------------------
---********************** Prevalance All **************************
----------------------------------------------------------------------------------------

--prevalance all TRUVEN - row 51  
select ( sum(vacc_flag) / count(uth_member_id)::float ) as prev, count(uth_member_id), sum(vacc_flag)
from dev.wc_flu_com_2017 a 
where a.data_source = 'truv'
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id), sum(vacc_flag)
from dev.wc_flu_com_2017 a 
where a.data_source = 'truv'
  and a.gender_cd = 'F'
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id), sum(vacc_flag)
from dev.wc_flu_com_2017 a 
where a.data_source = 'truv'
  and a.gender_cd = 'M'
;


select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id), sum(vacc_flag)
from dev.wc_flu_com_2017 a 
where a.data_source = 'truv'
  and a.age_group = '6'
;

--prevalance all OPTZ - row 51  
select ( sum(vacc_flag) / count(uth_member_id)::float )as prev, count(uth_member_id), sum(vacc_flag)
from dev.wc_flu_com_2017 a 
where a.data_source = 'optz'
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id), sum(vacc_flag)
from dev.wc_flu_com_2017 a 
where a.data_source = 'optz'
  and a.gender_cd = 'F'
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id), sum(vacc_flag)
from dev.wc_flu_com_2017 a 
where a.data_source = 'optz'
  and a.gender_cd = 'M'
;


select ( sum(vacc_flag) / count(uth_member_id)::float ) as prev, count(uth_member_id), sum(vacc_flag)
from dev.wc_flu_com_2017 a 
where a.data_source = 'optz'
  and a.age_group = '6'
;

----------------------------------------------------------------------------------------
---********************** Prevalance by ZIP **************************
----------------------------------------------------------------------------------------

-- truven 
select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev --, a.zip3 
from dev.wc_flu_com_2017 a 
where a.data_source = 'truv'
group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev --, a.zip3 
from dev.wc_flu_com_2017 a 
where a.data_source = 'truv'
  and a.gender_cd = 'F'
  group by a.zip3 
order by a.zip3
;


select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev --, a.zip3 
from dev.wc_flu_com_2017 a 
where a.data_source = 'truv'
  and a.gender_cd = 'M'
  group by a.zip3 
order by a.zip3
;


select ( sum(vacc_flag) / count(uth_member_id)::float )as prev --, a.zip3 
from dev.wc_flu_com_2017 a 
where a.data_source = 'truv'
  and a.age_group = '6'
  group by a.zip3 
order by a.zip3
  ;
  
 
 -- optum by zip 
select ( sum(vacc_flag) / count(uth_member_id)::float ) as prev, a.zip3 
from dev.wc_flu_com_2017 a 
where a.data_source = 'optz'
group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float ) as prev, a.zip3 
from dev.wc_flu_com_2017 a 
where a.data_source = 'optz'
  and a.gender_cd = 'F'
  group by a.zip3 
order by a.zip3
;


select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, a.zip3 
from dev.wc_flu_com_2017 a 
where a.data_source = 'optz'
  and a.gender_cd = 'M'
  group by a.zip3 
order by a.zip3
;


select ( sum(vacc_flag) / count(uth_member_id)::float ) as prev , a.zip3 
from dev.wc_flu_com_2017 a 
where a.data_source = 'optz'
  and a.age_group = '6'
  group by a.zip3 
order by a.zip3
  ;