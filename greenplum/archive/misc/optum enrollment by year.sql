
 select count(distinct uth_member_id), year
  from data_warehouse.member_enrollment_yearly
  where data_source = 'optz'
  and state = 'TX'
  group by year
  order by year
  ;
 
 
 
 select count(distinct uth_member_id) , year, gender_Cd 
  from data_warehouse.member_enrollment_yearly
  where data_source = 'optz'
  and gender_cd in ('M','F')
  and state = 'TX'
  group by year, gender_cd  
  order by year, gender_cd desc 
  ;
 
 
   select count(distinct uth_member_id) , year,
         case when age_derived <= 19 then 1 
              when age_derived between 20 and 34 then 2
              when age_derived between 35 and 44 then 3 
              when age_derived between 45 and 54 then 4
              when age_derived between 55 and 64 then 5
              when age_derived between 65 and 74 then 6
              when age_derived > 74 then 7 
          end as age_group
  from data_warehouse.member_enrollment_yearly
  where data_source = 'optz'
  and gender_cd in ('M','F')
  and state = 'TX'
  group by year,
           case when age_derived <= 19 then 1 
              when age_derived between 20 and 34 then 2
              when age_derived between 35 and 44 then 3 
              when age_derived between 45 and 54 then 4
              when age_derived between 55 and 64 then 5
              when age_derived between 65 and 74 then 6
              when age_derived > 74 then 7 
          end 
  order by year, case when age_derived <= 19 then 1 
              when age_derived between 20 and 34 then 2
              when age_derived between 35 and 44 then 3 
              when age_derived between 45 and 54 then 4
              when age_derived between 55 and 64 then 5
              when age_derived between 65 and 74 then 6
              when age_derived > 74 then 7 
          end
;
 
  
  
  
  
  select count(distinct uth_member_id) , gender_cd, 
         case when age_derived <= 19 then 1 
              when age_derived between 20 and 34 then 2
              when age_derived between 35 and 44 then 3 
              when age_derived between 45 and 54 then 4
              when age_derived between 55 and 64 then 5
              when age_derived between 65 and 74 then 6
              when age_derived > 74 then 7 
          end as age_group
  from data_warehouse.member_enrollment_yearly
  where data_source = 'optz'
  and gender_cd in ('M','F')
  and year = 2019
  and state = 'TX'
  group by gender_cd, 
           case when age_derived <= 19 then 1 
              when age_derived between 20 and 34 then 2
              when age_derived between 35 and 44 then 3 
              when age_derived between 45 and 54 then 4
              when age_derived between 55 and 64 then 5
              when age_derived between 65 and 74 then 6
              when age_derived > 74 then 7 
          end 
  order by case when age_derived <= 19 then 1 
              when age_derived between 20 and 34 then 2
              when age_derived between 35 and 44 then 3 
              when age_derived between 45 and 54 then 4
              when age_derived between 55 and 64 then 5
              when age_derived between 65 and 74 then 6
              when age_derived > 74 then 7 
          end, gender_cd desc 
;




 
  select count(distinct uth_member_id) --, state
  from data_warehouse.member_enrollment_yearly
  where data_source = 'optd'
  and gender_cd in ('M','F')
  and year = 2019
  group by state
  order by state 
