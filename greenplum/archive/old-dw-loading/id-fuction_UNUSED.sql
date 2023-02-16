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