/***********************************
 * This script makes some changes to data_warehouse tables to accomodate Medicare data
 * 
 * Claim Header:
 * 	deductible, copay, coinsurance, and out of pocket (oop) columns 
 *
 * Pharmacy Claims:
 *  oop
 *  Changed quantity to numeric(13,3) to accomodate non-integer values for quantity
 * 
 * Xiaorui 9/29/2023 (claim_header) and 10/4/23 (pharmacy_claims)
 */

--claim_header
alter table data_warehouse.claim_header
add column deductible numeric(13,2),
add column copay numeric(13,2),
add column coins numeric(13,2),
add column oop numeric(13,2);

vacuum analyze data_warehouse.claim_header;

--pharmacy_claims (add oop)
alter table data_warehouse.pharmacy_claims
add column oop numeric(13,2);

vacuum analyze data_warehouse.pharmacy_claims;

--pharmacy_claims - covert quantity to numeric(13,3)
--do this in psql because this will take a while
alter table data_warehouse.pharmacy_claims
add column quantity_numeric numeric(13,3);

vacuum analyze data_warehouse.pharmacy_claims;

update data_warehouse.pharmacy_claims
set quantity_numeric = quantity::numeric;

alter table data_warehouse.pharmacy_claims
rename column quantity to quantity_int;

alter table data_warehouse.pharmacy_claims
rename column quantity_numeric to quantity;

alter table data_warehouse.pharmacy_claims
drop column quantity_int;
