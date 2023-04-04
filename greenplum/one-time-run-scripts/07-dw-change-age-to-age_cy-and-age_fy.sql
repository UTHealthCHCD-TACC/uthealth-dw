/************************************************
 * Script purpose:
 * After some changes to DW, it became obvious that age_derived in
 * year-month table needed to be split into age_fy and age_cy to accomodate Medicaid
 *
************************************************/

--change current column to age_cy
alter table data_warehouse.member_enrollment_monthly rename column age_derived to age_cy;

--add age_fy
alter table data_warehouse.member_enrollment_monthly add column age_fy integer;

vacuum analyze data_warehouse.member_enrollment_monthly;
