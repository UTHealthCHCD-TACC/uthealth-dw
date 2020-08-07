/*
 * Refresh dates for each table 
 */


/*
 * Confinement
select year, extract(quarter from admit_date), min(admit_date), max(admit_date), count(*)  from optum_dod_backup.confinement group by 1, 2 order by 1, 2;
select year, extract(quarter from admit_date), min(admit_date), max(admit_date), count(*)  from optum_dod.confinement group by 1, 2 order by 1, 2;
 */

create table optum_dod.confinement_backup (like optum_dod.confinement)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into optum_dod.confinement_backup
select *
from optum_dod.confinement;

delete from optum_dod.confinement where admit_date >= (select min(admit_date) from optum_dod_refresh.confinement);

insert into optum_dod.confinement
select *
from optum_dod_refresh.confinement;

/*
 * Diagnostic
select year, extract(quarter from fst_dt), min(fst_dt), max(fst_dt), count(*)  from optum_dod_backup.diagnostic group by 1, 2 order by 1, 2;
select year, extract(quarter from fst_dt), min(fst_dt), max(fst_dt), count(*)  from optum_dod.diagnostic group by 1, 2 order by 1, 2;
 */

create table optum_dod.diagnostic_backup (like optum_dod.diagnostic)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into optum_dod.diagnostic_backup
select *
from optum_dod.diagnostic;

delete from optum_dod.diagnostic where fst_dt >= (select min(fst_dt) from optum_dod_refresh.diagnostic);

insert into optum_dod.diagnostic
select *
from optum_dod_refresh.diagnostic;

/*
Lab Result

select year, extract(quarter from fst_dt), min(fst_dt), max(fst_dt), count(*)  from optum_dod_refresh.lab_result group by 1, 2 order by 1, 2;
select year, extract(quarter from fst_dt), min(fst_dt), max(fst_dt), count(*)  from optum_dod.lab_result group by 1, 2 order by 1, 2;

**/

create table optum_dod.lab_result_backup (like optum_dod.lab_result)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into optum_dod.lab_result_backup
select *
from optum_dod.lab_result;

delete from optum_dod.lab_result where fst_dt >= (select min(fst_dt) from optum_dod_refresh.lab_result);

insert into optum_dod.lab_result
select *
from optum_dod_refresh.lab_result;

/*
Medical

select year, extract(quarter from fst_dt), min(fst_dt), max(fst_dt), count(*)  from optum_dod_refresh.medical group by 1, 2 order by 1, 2;
select year, extract(quarter from fst_dt), min(fst_dt), max(fst_dt), count(*)  from optum_dod.medical group by 1, 2 order by 1, 2;

**/

create table optum_dod.medical_backup (like optum_dod.medical)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into optum_dod.medical_backup
select *
from optum_dod.medical;

delete from optum_dod.medical where fst_dt >= (select min(fst_dt) from optum_dod_refresh.medical);

insert into optum_dod.medical
select * from optum_dod_refresh.medical;

/*
mbr_co_enroll

select count(*) from optum_dod_refresh.mbr_co_enroll;
select count(*) from optum_dod.mbr_co_enroll;
*/
create table optum_dod.mbr_co_enroll_backup (like optum_dod.mbr_co_enroll)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into optum_dod.mbr_co_enroll_backup
select * from optum_dod.mbr_co_enroll;

truncate optum_dod.mbr_co_enroll;

insert into optum_dod.mbr_co_enroll
select * from optum_dod_refresh.mbr_co_enroll;

/*
mbr_enroll

select count(*) from optum_dod_refresh.mbr_enroll;
select count(*) from optum_dod.mbr_enroll;
*/
create table optum_dod.mbr_enroll_backup (like optum_dod.mbr_enroll)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into optum_dod.mbr_enroll_backup
select * from optum_dod.mbr_enroll;

truncate optum_dod.mbr_enroll;

insert into optum_dod.mbr_enroll
select * from optum_dod_refresh.mbr_enroll;

/*
Procedure

select year, extract(quarter from fst_dt), min(fst_dt), max(fst_dt), count(*)  from optum_dod_refresh.procedure group by 1, 2 order by 1, 2;
select year, extract(quarter from fst_dt), min(fst_dt), max(fst_dt), count(*)  from optum_dod.procedure group by 1, 2 order by 1, 2;

**/

create table optum_dod.procedure_backup (like optum_dod.procedure)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into optum_dod.procedure_backup
select *
from optum_dod.procedure;

delete from optum_dod.procedure where fst_dt >= (select min(fst_dt) from optum_dod_refresh.procedure);

insert into optum_dod.procedure
select *
from optum_dod_refresh.procedure;

/*
provider

select count(*) from optum_dod_refresh.provider;
select count(*) from optum_dod.provider;
*/
create table optum_dod.provider_backup (like optum_dod.provider)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into optum_dod.provider_backup
select * from optum_dod.provider;

truncate optum_dod.provider;

insert into optum_dod.provider
select * from optum_dod_refresh.provider;

/*
provider_bridge

select count(*) from optum_dod_refresh.provider_bridge;
select count(*) from optum_dod.provider_bridge;
*/
create table optum_dod.provider_bridge_backup (like optum_dod.provider_bridge)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into optum_dod.provider_bridge_backup
select * from optum_dod.provider_bridge;

truncate optum_dod.provider_bridge;

insert into optum_dod.provider_bridge
select * from optum_dod_refresh.provider_bridge;

/*
RX
select year, extract(quarter from fill_dt), min(fill_dt), max(fill_dt), count(*)  from optum_dod_backup.rx group by 1, 2 order by 1, 2;
select year, extract(quarter from fill_dt), min(fill_dt), max(fill_dt), count(*)  from optum_dod.rx group by 1, 2 order by 1, 2;
 */
create table optum_dod.rx_backup (like optum_dod.rx)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into optum_dod.rx_backup
select * from optum_dod.rx;

delete from optum_dod.rx where fill_dt >= (select min(fill_dt) from optum_dod_refresh.rx);

insert into optum_dod.rx
select * from optum_dod_refresh.rx;

/*
Clean up everything
Leave backups till manually vetted
*/

vacuum full optum_dod.confinement;
vacuum full optum_dod.diagnostic;
vacuum full optum_dod.lab_result;
vacuum full optum_dod.mbr_co_enroll;
vacuum full optum_dod.mbr_enroll;
vacuum full optum_dod.medical;
vacuum full optum_dod.procedure;
vacuum full optum_dod.provider;
vacuum full optum_dod.provider_bridge;
vacuum full optum_dod.rx;
