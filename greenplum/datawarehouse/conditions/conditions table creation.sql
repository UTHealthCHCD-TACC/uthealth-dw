create schema conditions;

---list of conditions
create table conditions.condition_desc ( condition_cd text, condition_desc text, type_cd text, type_desc text, carry_forward char(1));


---codes used to identify conditions, dx, proc, rev code etc
create table conditions.codeset ( log_seq int2, condition_cd text, cd_type text,  cd_value_raw text, cd_value text );

update conditions.codeset set cd_value = replace(replace(cd_value_raw,'.',''),'x','%')

select * from conditions.codeset c where cd_value like '%\%\%%'

update conditions.codeset set cd_value = '340%' where cd_value_raw = '340.xx'

select * from conditions.codeset cd 

select * from conditions.condition_desc cd 


---table to store which members had which conditions in each year
drop table conditions.member_conditions ;

create table conditions.member_conditions 
(data_source char(4), year int2, uth_member_id bigint, condition_cd text, first_date date )
with (appendonly=true, orientation = column)
distributed by (uth_member_id);


analyze conditions.member_conditions 


CREATE TABLE conditions.condition_map (condition_cd text, condition_map_position int2)






