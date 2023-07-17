/**************
 * This function will calculate the fiscal year, given a date
 * 
 * The dividing line is august 31st/september 1st
 * 
 * For example,
 * 		08/31/2020 -> FY 2020
 * 		09/01/2020 -> FY 2021
 * 
 * --Xiaorui 07/13/2023
 */

create or replace function get_fy_from_date(date_input date)

returns integer as

$$

declare
	
	fy int;

begin
	
	fy := extract(year from date_input);

	if extract(month from date_input) >= 9 then
		fy := fy + 1;
	end if;
	
	return fy;
	
end;

$$

language plpgsql;