/* This function was created as a second version of the validate_uth_member_id function
 * It accepts arguments for data_source, source_claim_id, source_member_id, and data_year
 * It returns a boolean indicating whether the record exists or not
 */

DROP FUNCTION public.validate_uth_member_id_2 ( i_data_source char(4), i_claim_id_src text, i_member_id_src text, i_data_year char(4) );
CREATE OR REPLACE FUNCTION public.validate_uth_member_id_2 ( i_data_source char(4), i_claim_id_src text, i_member_id_src text, i_data_year char(4) )
RETURNS bool AS $FUNC$	
declare iResult integer;
begin
--check for claim id entry
	select into iResult count(*) from data_warehouse.dim_uth_claim_id
			  where data_source = i_data_source
			    and claim_id_src = i_claim_id_src
			    and member_id_src = i_member_id_src
			    and data_year = i_data_year;
	if iResult>0 then
		return true;
	else
		return false;
	end if;
end $FUNC$
language 'plpgsql';

/* This is a test of the function - please use values that make sense */ 
select validate_uth_member_id_2 ('trvc','30861897402.0','600745166.0','2012');
