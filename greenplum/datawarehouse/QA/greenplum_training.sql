CREATE TABLE sql_class.Employee_table_DW5
(
Employee_No integer,
Dept_No smallint,
Last_name char(20),
First_name varchar(12),
Salary decimal(8,2)
)
distributed replicated;

insert into sql_class.Employee_table_DW5
select * from sql_class.employee_table;

explain
select * from sql_class.employee_table_dw where last_name = 'Larkins';

explain analyze select * from sql_class.employee_table_dw where employee_no in (select employee_no from sql_class.employee_table_dw where employee_no=2000000);

explain analyze select * from sql_class.employee_table_dw5 where employee_no in (2000000, 1000234);

vacuum analyze employee_table_dw3;

explain analyze
select *
from employee_table_dw5 a
join employee_table_dw b on a.dept_no=b.dept_no and b.dept_no=400;

explain analyze
select *
from employee_table_dw a
join employee_table_dw2 b on a.dept_no=b.dept_no;

select * from gp_distribution_policy ;


select a.*
from employee_table a
where salary > (select avg(salary) from employee_table b where b.dept_no=a.dept_no);

create table sql_class.optum_zip_diag_row_zlib (like dev2016.optum_zip_diagnostic)
with (appendonly =true, orientation=row, compresstype=zlib, compresslevel=5);
insert into sql_class.optum_zip_diag_row_zlib
select * from dev2016.optum_zip_diagnostic;

create table sql_class.optum_zip_diag_col_zlibmin (like dev2016.optum_zip_diagnostic)
with (appendonly =true, orientation=column, compresstype=zlib, compresslevel=1);
insert into sql_class.optum_zip_diag_col_zlibmin
select * from dev2016.optum_zip_diagnostic;

select pg_size_pretty(pg_relation_size('sql_class.optum_zip_diag_row_nocomp')) as row_nocomp,
pg_size_pretty(pg_relation_size('sql_class.optum_zip_diag_col_nocomp')) as col_nocop,
pg_size_pretty(pg_relation_size('sql_class.optum_zip_diag_row_zlib')) as row_zlib5,
 pg_size_pretty(pg_relation_size('sql_class.optum_zip_diag_col_zlib')) as col_zlib5,
   pg_size_pretty(pg_relation_size('sql_class.optum_zip_diag_col_zlibmin')) as col_zlib1,
  pg_size_pretty(pg_relation_size('sql_class.optum_zip_diag_col_zlibmax')) as col_zlib9
 
  select get_ao_compression_ratio('sql_class.optum_zip_diag_col_zlibmin'); 
  select get_ao_compression_ratio('sql_class.optum_zip_diag_col_zlibmax'); 
 
 select icd_flag, count(*)
 from sql_class.optum_zip_diag_row_nocomp
 group by 1;

 select icd_flag, count(*)
 from sql_class.optum_zip_diag_col_nocomp
 group by 1;

 select icd_flag, count(*)
 from sql_class.optum_zip_diag_row_zlib
 group by 1;

 select icd_flag, count(*)
 from sql_class.optum_zip_diag_col_zlib
 group by 1;

 select icd_flag, count(*)
 from sql_class.optum_zip_diag_col_zlibmin
 group by 1;

 select icd_flag, count(*)
 from sql_class.optum_zip_diag_col_zlibmax
 group by 1;



