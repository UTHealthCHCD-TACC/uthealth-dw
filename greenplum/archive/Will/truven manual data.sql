

--overall by year 
select count(*), year--, gender_cd 
from data_warehouse.member_enrollment_yearly 
where data_source = 'truv'
  and bus_cd = 'COM'
  and state = 'TX'
group by year --, gender_cd
order by year --, gender_cd desc



---age group and gender

select gender_cd,
       case when age_derived between 0 and 19 then 1 
            when age_derived between 20 and 34 then 2 
            when age_derived between 35 and 44 then 3 
            when age_derived between 45 and 54 then 4 
            when age_derived between 55 and 64 then 5 
            when age_derived between 65 and 74 then 6 
            when age_derived >= 75 then 7 
       end as age_group,
       year, 
       count(*)
from data_warehouse.member_enrollment_yearly
where data_source = 'truv'
  and bus_cd = 'COM'
  and state = 'TX'
group by year, gender_cd, 
            case when age_derived between 0 and 19 then 1 
            when age_derived between 20 and 34 then 2 
            when age_derived between 35 and 44 then 3 
            when age_derived between 45 and 54 then 4 
            when age_derived between 55 and 64 then 5 
            when age_derived between 65 and 74 then 6 
            when age_derived >= 75 then 7 
       end
order by year, case when age_derived between 0 and 19 then 1 
            when age_derived between 20 and 34 then 2 
            when age_derived between 35 and 44 then 3 
            when age_derived between 45 and 54 then 4 
            when age_derived between 55 and 64 then 5 
            when age_derived between 65 and 74 then 6 
            when age_derived >= 75 then 7 
       end  , gender_cd desc 
       
       
update data_warehouse.member_enrollment_yearly set state = 'ZZ' where state = 'UNK'      
       
---state
select count(*), state, year  
from data_warehouse.member_enrollment_yearly 
where data_source = 'truv'
  and bus_cd = 'COM'
  --and year = 2011
group by state, year 
order by year, state


select sum(total_enrolled_months ), year 
from data_warehouse.member_enrollment_yearly 
where data_source = 'truv'
and bus_cd = 'COM'
and age_derived >= 18
group by year;


select count(*), year  
from data_warehouse.member_enrollment_monthly 
where data_source = 'truv'
 and bus_cd = 'COM'
 and age_derived >= 18
 group by year 
