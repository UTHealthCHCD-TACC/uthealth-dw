---Commercial Annual Preventive Exam (APE) - 2017 

---optum and truven cohorts from DW
drop table dev.wc_ape_com_2017;

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
 into dev.wc_ape_com_2017
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

delete from dev.wc_ape_com_2017 where age_group is null;

delete from dev.wc_ape_com_2017 where length(zip3::text) = 2

drop table dev.wc_ape_com_2017_vacc;

select distinct uth_member_id 
into dev.wc_ape_com_2017_vacc
from data_warehouse.claim_detail a
where a.procedure_cd in ('99381','99382','99383','99384','99385','99386','99387',
						 '99391','99392','99393','99394','99395','99396','99397',
						 'S0610','S0612','S0615')
      and a.year = 2017 
      and a.uth_member_id in ( select uth_member_id from dev.wc_ape_com_2017)
;


insert into dev.wc_ape_com_2017_vacc
select distinct uth_member_id 
from data_warehouse.claim_diag 
where diag_cd in ('Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419',
				  'V700','V700','V7231','V705','V703','V7284','V7285') 
      and year = 2017 
      and uth_member_id in ( select uth_member_id from dev.wc_ape_com_2017)
      and uth_member_id not in ( select uth_member_id from dev.wc_ape_com_2017_vacc)
;


alter table dev.wc_ape_com_2017 add column vacc_flag int2 default 0;


update dev.wc_ape_com_2017 a set vacc_flag = 1
  from dev.wc_ape_com_2017_vacc b 
    where b.uth_member_id = a.uth_member_id
 ;

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
	r_den := (	select count(*) from dev.wc_ape_com_2017 );

	for r_num , r_data_source
	  in 
	select count(*), data_source 
	from dev.wc_ape_com_2017 
	group by data_source 
	
	loop 
	    r_result = r_num / r_den;
	    r_result = trunc(r_result,4);
	    raise notice 'Overall Weight % is % ', r_data_source, r_result;
	end loop;

---female 
	r_num := 0;
	r_den := (	select count(*) from dev.wc_ape_com_2017 where gender_cd = 'F' );

	for r_num , r_data_source
	  in 
	select count(*), data_source 
	from dev.wc_ape_com_2017 
	where gender_cd = 'F'
	group by data_source 
	
	loop 
	    r_result = r_num / r_den;
	    r_result = trunc(r_result,4);
	    raise notice 'Female Weight % is % ', r_data_source, r_result;
	end loop;

---male 
	r_num := 0;
	r_den := (	select count(*) from dev.wc_ape_com_2017 where gender_cd = 'M' );

	for r_num , r_data_source
	  in 
	select count(*), data_source 
	from dev.wc_ape_com_2017 
	where gender_cd = 'M'
	group by data_source 
	
	loop 
	    r_result = r_num / r_den;
	    r_result = trunc(r_result,4);
	    raise notice 'Male Weight % is % ', r_data_source, r_result;
	end loop;

---age_group
	r_ag :=0;
	loop
		r_ag := r_ag+1;
		r_num := 0;
		r_den := (	select count(*) from dev.wc_ape_com_2017 where age_group = r_ag );
	
		for r_num , r_data_source
		  in 
		select count(*), data_source 
		from dev.wc_ape_com_2017 
		where age_group = r_ag
		group by data_source 
		
		loop 
		    r_result = r_num / r_den;
		    r_result = trunc(r_result,4);
		    raise notice 'Age % Weight % is % ', r_ag, r_data_source, r_result;
		end loop;
	if r_ag > 6 then exit; end if;
	end loop;


	return 0;
end $FUNC$
language 'plpgsql';
 

select public.flu_weights ();

   
			    
----------------------------------------------------------------------------------------
---********************** Prevalance All **************************
----------------------------------------------------------------------------------------

--prevalance all - row 51  -- optum 
select * 
from (
select ( sum(vacc_flag) / count(uth_member_id)::float )as prev, count(uth_member_id) as mems, 'all' as grp
from dev.wc_ape_com_2017 a 
where a.data_source = 'optz'
union all
select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id) as mems, gender_cd
from dev.wc_ape_com_2017 a 
where a.data_source = 'optz'
group by a.gender_cd
) x 
order by grp
;


select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id) as mems, age_group 
from dev.wc_ape_com_2017 a 
where a.data_source = 'optz'
group by age_group 
order by age_group;


--prevalance all - row 51  -- truven
select * 
from (
select ( sum(vacc_flag) / count(uth_member_id)::float ) as prev, count(uth_member_id) as mems, 'all' as grp
from dev.wc_ape_com_2017 a 
where a.data_source = 'truv'
union all 
select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id) as mems, gender_cd
from dev.wc_ape_com_2017 a 
where a.data_source = 'truv'
group by a.gender_cd 
) x 
order by grp
;


select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id) as mems, age_group 
from dev.wc_ape_com_2017 a 
where a.data_source = 'truv'
group by age_group 
order by age_group;


----------------------------------------------------------------------------------------
---********************** Prevalance by ZIP **************************
----------------------------------------------------------------------------------------

-- truven
select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id) as mems --, a.zip3 
from dev.wc_ape_com_2017 a 
where a.data_source = 'truv'
group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev , count(uth_member_id) as mems--, a.zip3 
from dev.wc_ape_com_2017 a 
where a.data_source = 'truv'
  and a.gender_cd = 'F'
  group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev , count(uth_member_id) as mems--, a.zip3 
from dev.wc_ape_com_2017 a 
where a.data_source = 'truv'
  and a.gender_cd = 'M'
  group by a.zip3 
order by a.zip3
;
  
select ( sum(vacc_flag) / count(uth_member_id)::float )as prev, count(uth_member_id) as mems --, a.zip3 
from dev.wc_ape_com_2017 a 
where a.data_source = 'truv'
  and a.age_group = '6'
  group by a.zip3 
order by a.zip3
 ;


delete from dev.wc_ape_com_2017 where zip3 = '772';

 -- optum weight  missing zip 753 772 
insert into dev.wc_ape_com_2017 values 
(0001, 753,1,'M','optz',0),
(0002, 753,2,'M','optz',0),
(0003, 753,3,'M','optz',0),
(0004, 753,4,'F','optz',0),
(0005, 753,5,'F','optz',0),
(0006, 753,6,'F','optz',0),
(0007, 772,1,'M','optz',0),
(0008, 772,2,'M','optz',0),
(0009, 772,3,'M','optz',0),
(0010, 772,4,'F','optz',0),
(0011, 772,5,'F','optz',0),
(0012, 772,6,'F','optz',0)

select ( sum(vacc_flag) / count(uth_member_id)::float ) as prev, count(uth_member_id) as mems 
from dev.wc_ape_com_2017 a 
where a.data_source = 'optz'
group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float ) as prev, count(uth_member_id) as mems 
from dev.wc_ape_com_2017 a 
where a.data_source = 'optz'
  and a.gender_cd = 'F'
  group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id) as mems 
from dev.wc_ape_com_2017 a 
where a.data_source = 'optz'
  and a.gender_cd = 'M'
  group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float )as prev, count(uth_member_id) as mems 
from dev.wc_ape_com_2017 a 
where a.data_source = 'optz'
  and a.age_group = '6'
  group by a.zip3 
order by a.zip3
 ;
