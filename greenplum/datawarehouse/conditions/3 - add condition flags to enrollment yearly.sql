/* ******************************************************************************************************
 *  This script modifies member enrollment yearly to add new columns for condition flags and 
 *  then populates them from conditions.person_profile
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wc001        || 11/18/2021 || modified to all lower case
 * ******************************************************************************************************
 */

---create one column in yearly enrollment for each condition 
do $$ 
declare
	r_cond_cd text;
	r_carry char(1);
	unfinished_conditions text[]:= array['asth', 'db', 'del', 'htn', 'lb', 
                                         'lbpreg', 'opi', 'preg', 'tob']; 
	condition_column text;
begin
	
	drop table if exists conditions.conditions_member_enrollment_yearly;

	--copy enrollment table to condition schema
	create table conditions.conditions_member_enrollment_yearly
	with (appendoptimized=true, orientation=column, compresstype=zlib)
	as
	select *
	from data_warehouse.member_enrollment_yearly
	distributed by (uth_member_id)
	;

    analyze conditions.conditions_member_enrollment_yearly;
	alter table conditions.conditions_member_enrollment_yearly owner to uthealth_dev;
	
---loop to add new columns based on condition types in condition_desc 
for r_cond_cd 
	 in 
		select condition_cd 
		from conditions.condition_desc cd 		
		order by condition_cd
	loop 	
		if exists ( select 1 
	                from information_schema.columns 
	                where table_schema = 'conditions'
	               	  and table_name = 'conditions_member_enrollment_yearly'
	                  and column_name = r_cond_cd)
	        then
	        raise notice 'column % already exists', r_cond_cd;	
		else 
			
		   execute format ('alter table conditions.conditions_member_enrollment_yearly add column %s int2 default 0', r_cond_cd);
		   raise notice 'added column: % ', r_cond_cd;
		end if;



	---loop through columns and populate,  check for carry forward and apply logic accordingly
	
		select carry_forward 
		into r_carry
		from conditions.condition_desc 
		where condition_cd = r_cond_cd
		;
		
		raise notice 'Loading column % , carry %', r_cond_cd, r_carry;
	
		if r_carry = '0' then 
			
			execute   'update conditions.conditions_member_enrollment_yearly a set ' || r_cond_cd || ' =1 
					   from conditions.person_profile_stage b 
	   				   where a.uth_member_id = b.uth_member_id 
	     				 and a.year = b.year 
	    				 and a.data_source = b.data_source
	                     and b.condition_cd = ''' || r_cond_cd || ''';'
	                    ;
	                   
		else 
		
			execute 'with carry_cte as (select min(year) as yr, uth_member_id, data_source 
			                from conditions.person_profile_stage 
			                where condition_cd = ''' || r_cond_cd || '''
			                group by uth_member_id , data_source 
			                )
					 update conditions.conditions_member_enrollment_yearly a set ' || r_cond_cd || ' =1 
					 from carry_cte b 
					    where a.uth_member_id = b.uth_member_id 
					      and a.year >= b.yr 
					      and a.data_source = b.data_source
					  ;'
					  ;

		end if;
	
		
	end loop;
	
	foreach condition_column in array unfinished_conditions
	loop 

	execute 'alter table conditions.conditions_member_enrollment_yearly drop column ' || condition_column || ';';
	raise notice 'column % dropped', condition_column;
	
	end loop;
	
	
	analyze conditions.conditions_member_enrollment_yearly;


end $$
;




---build copy in data warehouse 
drop table if exists data_warehouse.conditions_member_enrollment_yearly;

create table data_warehouse.conditions_member_enrollment_yearly
with (appendoptimized=true, orientation=column, compresstype=zlib)
as
select *
from conditions.conditions_member_enrollment_yearly
distributed by (uth_member_id);

alter table data_warehouse.conditions_member_enrollment_yearly owner to uthealth_dev;

analyze data_warehouse.conditions_member_enrollment_yearly;


----validate
select sum(aimm) aimm, 
       sum(ami) ami, 
       sum(ca) ca, 
       sum(cfib) cfib, 
       sum(chf) chf, 
       sum(cliv) cliv,
       sum(copd) copd,
       sum(cysf) cysf,
       sum(epi) epi, 
       sum(fbm) fbm,
       sum(hemo) hemo,
       sum(hep) hep,
       sum(lbp) lbp,
       sum(lymp) lymp, 
       sum(ms) ms, 
       sum(nicu) nicu, 
       sum(pain) pain, 
       sum(park) park,
       sum(pneu) pneu,
       sum(ra) ra,
       sum(scd) scd,
       sum(smi) smi,
       sum(str) str, 
       sum(tbi) tbi, 
       sum(trans) trans,
       sum(trau) trau
from conditions.conditions_member_enrollment_yearly 
;




