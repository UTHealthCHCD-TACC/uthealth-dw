
-- Backup table before updating
--truncate table qa_reporting.truven_counts_old;

--insert into qa_reporting.truven_counts_old select * from qa_reporting.truven_counts;



-- Insert into truven_counts from hpm_abs table

insert into qa_reporting.truven_counts
(year, table_name, row_count, pat_count, clm_count, last_updated, db_version)
select year, 'hpm_abs', count(*), count(distinct enrolid), 0, current_date, version
  from truven.hpm_abs
group by year, version;



-- Insert into truven_counts from hpm_elig table

insert into qa_reporting.truven_counts
(year, table_name, row_count, pat_count, clm_count, last_updated, db_version)
select year, 'hpm_elig', count(*), count(distinct enrolid), 0, current_date, version
  from truven.hpm_elig
group by year, version;



-- Insert into truven_counts from hpm_ltd table

insert into qa_reporting.truven_counts
(year, table_name, row_count, pat_count, clm_count, last_updated, db_version)
select year, 'hpm_ltd', count(*), count(distinct enrolid), count(distinct caseid), current_date, version
  from truven.hpm_ltd
group by year, version;



-- Insert into truven_counts from hpm_std table

insert into qa_reporting.truven_counts
(year, table_name, row_count, pat_count, clm_count, last_updated, db_version)
select year, 'hpm_std', count(*), count(distinct enrolid), count(distinct caseid), current_date, version
  from truven.hpm_std
group by year, version;



-- Insert into truven_counts from hpm_wc table

insert into qa_reporting.truven_counts
(year, table_name, row_count, pat_count, clm_count, last_updated, db_version)
select year, 'hpm_wc', count(*), count(distinct enrolid), count(distinct caseid), current_date, version
  from truven.hpm_wc
group by year, version;



