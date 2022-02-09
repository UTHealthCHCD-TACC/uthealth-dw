/* ******************************************************************************************************
 *  This script modifies member enrollment yearly to add new columns for condition flags and 
 *  then populates them from conditions.person_profile
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wc001        || 11/18/2021 || modified to all lower case
 * ******************************************************************************************************
 */

select * 
from conditions.condition_desc cd 
order by condition_cd 

---create one column in yearly enrollment for each condition 
do $$ 
declare
	r_cond_cd text;
	baseline_conditions text[]:= array['aimm','ami','asth','ca','cfib','chf','ckd','cliv','copd','cysf',
                                       'db','del','dem','dep','epi','fbm','hemo','hep','hiv','htn','ihd',
                                       'lb','lbp','lbpreg','lymp','ms','nicu','opi','pain','park','pneu',
                                       'preg','ra','scd','smi','str','tbi','tob','trans','trau'
                                      ]; 
	condition_column text;
	r_carry char(1);
begin
	
	drop table if exists conditions.member_enrollment_yearly;

	--copy enrollment table to condition schema
	create table conditions.member_enrollment_yearly
	with (appendoptimized=true, orientation=column, compresstype=zlib)
	as
	select * 
	from data_warehouse.member_enrollment_yearly
	distributed by (uth_member_id)
	;
	
	alter table conditions.member_enrollment_yearly owner to uthealth_dev;
	
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
	               	  and table_name = 'member_enrollment_yearly'
	                  and column_name = r_cond_cd)
	        then
	        raise notice 'column % already exists', r_cond_cd;	
		else 
			
		   execute format ('alter table conditions.member_enrollment_yearly add column %s int2 default 0', r_cond_cd);
		   raise notice 'added column: % ', r_cond_cd;
		end if;
	end loop;

	analyze conditions.member_enrollment_yearly;


	---loop through columns and populate,  check for carry forward and apply logic accordingly
	foreach condition_column in array baseline_conditions
	loop 
	
		select carry_forward 
		into r_carry
		from conditions.condition_desc 
		where condition_cd = condition_column
		;
		
		raise notice 'Loading column % , carry %', condition_column, r_carry;
	
		if r_carry = '0' then 
			
			execute   'update conditions.member_enrollment_yearly a set ' || condition_column || ' =1 
					   from conditions.person_profile_stage b 
	   				   where a.uth_member_id = b.uth_member_id 
	     				 and a.year = b.year 
	    				 and a.data_source = b.data_source
	                     and b.condition_cd = ''' || condition_column || ''';'
	                    ;
	                   
		else 
		
			execute 'with carry_cte as (select min(year) as yr, uth_member_id, data_source 
			                from conditions.person_profile_stage 
			                where condition_cd = ''' || condition_column || '''
			                group by uth_member_id , data_source 
			                )
					 update conditions.member_enrollment_yearly a set ' || condition_column || ' =1 
					 from carry_cte b 
					    where a.uth_member_id = b.uth_member_id 
					      and a.year >= b.yr 
					      and a.data_source = b.data_source
					  ;'
					  ;

		end if;
	
		
	end loop;
	
	analyze conditions.member_enrollment_yearly;

end $$
;


----validate
select data_source, 
       year, 
       count(distinct uth_member_id) unique_person_id, 
       sum(aimm) aimm, 
       sum(ami) ami, 
       sum(ca) ca, 
       sum(cab) cab,
       sum(caco) caco,
       sum(cacv) cacv,
       sum(cal) cal, 
       sum(cap) cap, 
       sum(cfib) cfib, 
       sum(chf) chf, 
       sum(ckd) ckd,
       sum(cliv) cliv,
       sum(copd) copd,
       sum(cres) cres, 
       sum(dep) dep,
       sum(epi) epi, 
       sum(fbm) fbm,
       sum(hemo) hemo,
       sum(hep) hep,
       sum(hiv) hiv,
       sum(hml) hml, 
       sum(ihd) ihd, 
       sum(lbp) lbp, 
       sum(lymp) lymp, 
       sum(ms) ms, 
       sum(nicu) nicu, 
       sum(pain) pain, 
       sum(park) park,
       sum(pneu) pneu,
       sum(ra) ra,
       sum(scd) scd,
       sum(scz) scz, 
       sum(smi) smi,
       sum(str) str, 
       sum(tbi) tbi, 
       sum(trans) trans,
       sum(trau) trau
from conditions.member_enrollment_yearly 
where state = 'TX'
 and data_source = 'optz'
group by data_source , year 
order by data_source , year 
;


---truven TX break down by com vs ms 

---seperate report for count of unique persons 

select * 
from (
select data_source, year, 'all' as bus_cd, count(*) as unique_persons ,sum(aimm) aimm, 
       sum(ami) ami, 
       sum(ca) ca, 
       sum(cab) cab,
       sum(caco) caco,
       sum(cacv) cacv,
       sum(cal) cal, 
       sum(cap) cap, 
       sum(cfib) cfib, 
       sum(chf) chf, 
       sum(ckd) ckd,
       sum(cliv) cliv,
       sum(copd) copd,
       sum(cres) cres, 
       sum(dep) dep,
       sum(epi) epi, 
       sum(fbm) fbm,
       sum(hemo) hemo,
       sum(hep) hep,
       sum(hiv) hiv,
       sum(hml) hml, 
       sum(ihd) ihd, 
       sum(lbp) lbp, 
       sum(lymp) lymp, 
       sum(ms) ms, 
       sum(nicu) nicu, 
       sum(pain) pain, 
       sum(park) park,
       sum(pneu) pneu,
       sum(ra) ra,
       sum(scd) scd,
       sum(scz) scz, 
       sum(smi) smi,
       sum(str) str, 
       sum(tbi) tbi, 
       sum(trans) trans,
       sum(trau) trau
from conditions.member_enrollment_yearly  
where data_source = 'optz' and state = 'TX' 
group by data_source, year 
union 
select data_source, year, bus_cd , count(*) as unique_persons ,sum(aimm) aimm, 
       sum(ami) ami, 
       sum(ca) ca, 
       sum(cab) cab,
       sum(caco) caco,
       sum(cacv) cacv,
       sum(cal) cal, 
       sum(cap) cap, 
       sum(cfib) cfib, 
       sum(chf) chf, 
       sum(ckd) ckd,
       sum(cliv) cliv,
       sum(copd) copd,
       sum(cres) cres, 
       sum(dep) dep,
       sum(epi) epi, 
       sum(fbm) fbm,
       sum(hemo) hemo,
       sum(hep) hep,
       sum(hiv) hiv,
       sum(hml) hml, 
       sum(ihd) ihd, 
       sum(lbp) lbp, 
       sum(lymp) lymp, 
       sum(ms) ms, 
       sum(nicu) nicu, 
       sum(pain) pain, 
       sum(park) park,
       sum(pneu) pneu,
       sum(ra) ra,
       sum(scd) scd,
       sum(scz) scz, 
       sum(smi) smi,
       sum(str) str, 
       sum(tbi) tbi, 
       sum(trans) trans,
       sum(trau) trau
from conditions.member_enrollment_yearly  
where data_source = 'truv' and state = 'TX' 
group by data_source, year , bus_cd 
union 
select data_source, year, 'all', count(*) as unique_persons ,sum(aimm) aimm, 
       sum(ami) ami, 
       sum(ca) ca, 
       sum(cab) cab,
       sum(caco) caco,
       sum(cacv) cacv,
       sum(cal) cal, 
       sum(cap) cap, 
       sum(cfib) cfib, 
       sum(chf) chf, 
       sum(ckd) ckd,
       sum(cliv) cliv,
       sum(copd) copd,
       sum(cres) cres, 
       sum(dep) dep,
       sum(epi) epi, 
       sum(fbm) fbm,
       sum(hemo) hemo,
       sum(hep) hep,
       sum(hiv) hiv,
       sum(hml) hml, 
       sum(ihd) ihd, 
       sum(lbp) lbp, 
       sum(lymp) lymp, 
       sum(ms) ms, 
       sum(nicu) nicu, 
       sum(pain) pain, 
       sum(park) park,
       sum(pneu) pneu,
       sum(ra) ra,
       sum(scd) scd,
       sum(scz) scz, 
       sum(smi) smi,
       sum(str) str, 
       sum(tbi) tbi, 
       sum(trans) trans,
       sum(trau) trau
from conditions.member_enrollment_yearly  
where data_source = 'mcrt' and state = 'TX' 
group by data_source, year 
) inr 
order by data_source , year, bus_cd 



select * 
from conditions.condition_desc



