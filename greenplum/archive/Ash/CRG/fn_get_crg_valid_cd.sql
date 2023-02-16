CREATE OR REPLACE FUNCTION dev.fn_get_crg_valid_cd(data_source text,field_name text,value text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
	
	
	
	
	
declare mapped_code text;
begin
	--if match found; replace current value with the mapped value else send the same value back  
	mapped_code:= case 
					when data_source = 'optz' or data_source = 'optd' then 
					
							case when field_name = 'sex' then 
									case when value in('M','m','F','f','U','u') then value
	 								else 'U' end
 								
 							    when field_name = 'dischargestatus' then 
 							    	
	 								case when value is null then '99'	 									
	 									 when value in ('NA','00','1','2','3','4','5','6','7','8','9',
	 													'0A','0C','0M','0P','0Y','+6',
	 													'96','97','98', 'C1','C5',
	 													'D1','DC','G0','OP','PC') then '99' 
 									else value end
 									
 								when field_name = 'placeofservice' then 
	 								case when value in ('02','NONE','UNK') then '99' 	-- for all unmapped values	 
	 								     when value is null then '99' 					-- for all unmapped values	 
 									else value end
 								 
 								when field_name = 'icdversionqualifier' then 
	 								case when value = '10' then '0' 
	 									 when value = '9' then '9' 
	 								     when value is null then '0' --default 
 									else '0' end
 								
 								when field_name = 'itemsiteofservice' or field_name = 'siteofservice'  then 
	 								case when value in ('02','NONE','UNK') then '9' 					-- other / unknown 
	 								     when value is null then '9'									-- other / unknown  
	 								     when value in ('17','49','50', '53','60','71') then '1' 		-- Clinic
	 								     when value in ('12','14') then '2' 							-- Home
	 								     when value in ('34') then '3' 									-- Hospice
	 								     when value in ('21','23','51') then '4' 						-- Inpatient or ER
	 								     when value in ('32') then '5' 									-- Nursing Facility
	 								     when value in ('11') then '6' 									-- Office
	 								     when value in ('22') then '7' 									-- Hospital Outpatient
	 								     when value in ('31') then '8' 									-- Skilled nursing facility
 									else '9' end
 									 								
							else value end
					
					--------------------------------------------------------------------------------------------
					when data_source = 'truv' then	
					
						case when field_name = 'sex' then 
								case when value in('M','m','F','f','U','u') then value
	 							else 'U' end
					
	 					when field_name = 'placeofservice' then 
 								case when value is null then '99' 				-- for all unmapped values	  									
								else cast(cast(value as numeric(10,0))as text) end
 									
	 					 when field_name = 'dischargestatus' then  							    	
 								case when value is null then '99'	 									
 							 	else coalesce(lpad(value,2,'0'), '99') end
	 					
						when field_name = 'icdversionqualifier' then 
							case when value = '10' then '0' 
								 when value = '9' then '9' 
							     when value is null then '0' --default 
							else '0' end
						
						when field_name = 'itemsiteofservice' or field_name = 'siteofservice'  then 
							case when value in ('02','NONE','UNK') then '9' 					-- other / unknown 
							     when value is null then '9'									-- other / unknown  
							     when value in ('17','49','50', '53','60','71') then '1' 		-- Clinic
							     when value in ('12','14') then '2' 							-- Home
							     when value in ('34') then '3' 									-- Hospice
							     when value in ('21','23','51') then '4' 						-- Inpatient or ER
							     when value in ('32') then '5' 									-- Nursing Facility
							     when value in ('11') then '6' 									-- Office
							     when value in ('22') then '7' 									-- Hospital Outpatient
							     when value in ('31') then '8' 									-- Skilled nursing facility
							else '9' end
 									
	 					else value end
					--------------------------------------------------------------------------------------------
					when data_source = 'mcrn' then 
					
						case when field_name = 'sex' then 
								case when value in('M','m','F','f','U','u') then value
 								else 'U' end
							
						    when field_name = 'dischargestatus' then 						    	
 								case when rtrim(ltrim(value))='' 
 	 							THEN '00' when value = '0' then '00' 
 	 							ELSE value end
								
							when field_name = 'placeofservice' then 
 								case when value in ('02','NONE','UNK') then '99' 	-- for all unmapped values	 
 								     when value is null then '99' 					-- for all unmapped values	 
								else value end
							 
							when field_name = 'icdversionqualifier' then 
 								case when value = '10' then '0' 
 									 when value = '9' then '9' 
 								     when value is null then '0' --default 
								else '0' end
							
							when field_name = 'itemsiteofservice' or field_name = 'siteofservice'  then 
 								case when value in ('02','NONE','UNK') then '9' 					-- other / unknown 
 								     when value is null then '9'									-- other / unknown  
 								     when value in ('17','49','50', '53','60','71') then '1' 		-- Clinic
 								     when value in ('12','14') then '2' 							-- Home
 								     when value in ('34') then '3' 									-- Hospice
 								     when value in ('21','23','51') then '4' 						-- Inpatient or ER
 								     when value in ('32') then '5' 									-- Nursing Facility
 								     when value in ('11') then '6' 									-- Office
 								     when value in ('22') then '7' 									-- Hospital Outpatient
 								     when value in ('31') then '8' 									-- Skilled nursing facility
								else '9' end
								 								
						else value end			
					
					
					--------------------------------------------------------------------------------------------

				  else value end;

	return mapped_code;

--select dev.fn_get_crg_valid_cd('optz', 'sex', 'f')

end







$$
EXECUTE ON ANY;
