/************************************************
 * Script purpose:
 * Make modifications to member_enrollment_yearly
 *
************************************************/
--add columns enrl_months_dual/nondual
alter table data_warehouse.member_enrollment_yearly
	add column enrl_months_nondual integer,
	add column enrl_months_dual integer;

--delete column fiscal_year b/c it doesn't make sense
alter table data_warehouse.member_enrollment_yearly
drop column fiscal_year;

vacuum analyze data_warehouse.member_enrollment_monthly;
