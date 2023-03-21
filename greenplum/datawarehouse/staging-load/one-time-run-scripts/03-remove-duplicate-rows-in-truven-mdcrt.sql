/*********************************************************
 * truven.mdcrt has some duplicated rows, so this code will make a copy of the old table
 * then copy ONLY distinct rows back into mdcrt
***********************************************************/

drop table if exists dw_staging.truven_mdcrt_backup;

--make a copy of the mdcrt into dw_staging
create table dw_staging.truven_mdcrt_backup
as select *
from truven.mdcrt
distributed by (seqnum);

--vacuum analyze it so things will run faster
vacuum analyze dw_staging.truven_mdcrt_backup;

--make sure rows match
select count(*) from dw_staging.truven_mdcrt_backup;
select count(*) from truven.mdcrt;
--518329512 both tables

--drop table! Breathe, we already copied it
drop table if exists truven.mdcrt;

--build table again
create table truven.mdcrt
as select distinct *
from dw_staging.truven_mdcrt_backup
distributed by (seqnum);

--check to see if the table is correct now
select * from truven.mdcrt where enrolid = 28396623502 order by dtstart;

enrolid		dtend		dtstart
28396623502	2011-01-31	2011-01-01
28396623502	2011-02-28	2011-02-01
28396623502	2011-03-31	2011-03-01
28396623502	2011-04-30	2011-04-01
28396623502	2011-05-31	2011-05-01
28396623502	2011-06-30	2011-06-01
28396623502	2011-07-31	2011-07-01
28396623502	2011-08-31	2011-08-01
28396623502	2011-09-30	2011-09-01
28396623502	2011-10-31	2011-10-01
28396623502	2011-11-30	2011-11-01
28396623502	2011-12-31	2011-12-01

--looks good.

--counts were cut in half for 2011-2014

--vacuum analyze
vacuum analyze truven.mdcrt;

