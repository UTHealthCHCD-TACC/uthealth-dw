

drop table data_warehouse.dim_uth_claim_id;

create table data_warehouse.dim_uth_claim_id (    
	generated_value bigserial,
	data_source char(4),
	claim_id_src text not null,
	member_id_src text not null,
	data_year char(4) not null, 
	uth_claim_id bigint,
	unique (data_source, claim_id_src, member_id_src, data_year)
) distributed by (data_source, claim_id_src, member_id_src, data_year);


alter sequence data_warehouse.dim_uth_claim_id_generated_value_seq restart with 100000000;


select dbo.set_all_perms();



insert into data_warehouse.dim_uth_claim_id (data_source, member_id_src, claim_id_src, data_year )
select distinct 'trvc', enrolid::text, coalesce(msclmid, caseid)::text , trunc(year,0)::text 
from truven.ccaes
where --('trvc' || enrolid::text || coalesce(msclmid, caseid)::text || year::text) not in ( select (data_source || claim_id_src || uth_claim_id || data_year) from data_warehouse.dim_uth_claim_id)
   enrolid is not null;

 
insert into data_warehouse.dim_uth_claim_id (data_source, member_id_src, claim_id_src, data_year )
select distinct 'trvc', enrolid::text, msclmid::text , trunc(year,0)::text
from truven.ccaeo
where ('trvc' || enrolid::text || msclmid::text || year::text) not in ( select (data_source || claim_id_src || uth_claim_id || data_year) from data_warehouse.dim_uth_claim_id)
  and enrolid is not null
  and year = 2015; 
 
 
 drop function public.validate_uth_member_id ( );
 
 
CREATE OR REPLACE FUNCTION public.validate_uth_member_id ( )
RETURNS int AS $FUNC$	 
	declare
	r_data_source text; 
	r_claim_id_src text;
	r_member_id_src text;
	r_data_year char(4);
	r_insert_count int;
begin
	r_insert_count := 0;

	for r_data_source, r_claim_id_src, r_member_id_src, r_data_year
		in
	select distinct 'trvc', enrolid::text, coalesce(msclmid, caseid)::text , trunc(year,0)::text
	from truven.ccaes
	where enrolid is not null
	
	loop 
		perform 1 from data_warehouse.dim_uth_claim_id 
				  where data_source = r_data_source
				    and member_id_src = r_member_id_src
				    and claim_id_src = r_claim_id_src
				    and data_year = r_data_year;
		if not found then
			insert into data_warehouse.dim_uth_claim_id (data_source, member_id_src, claim_id_src, data_year )
			values (r_data_source, r_member_id_src, r_claim_id_src, r_data_year);
		
			update data_warehouse.dim_uth_claim_id set uth_claim_id = ( substring(data_year::text,3,2) || generated_value::text )::bigint;
			
			r_insert_count := r_insert_count + 1;
	
		end if;
	end loop;

	return r_insert_count;

end $FUNC$
language 'plpgsql';
 
 
select validate_uth_member_id ();
 

delete
from data_warehouse.dim_uth_claim_id;


update data_warehouse.dim_uth_claim_id set uth_claim_id = ( substring(data_year::text,3,2) || generated_value::text )::bigint;

select * 
from data_warehouse.dim_uth_claim_id;


select enrolid, msclmid::text, year::text
from truven.ccaeo_wc a 
limit 1;
