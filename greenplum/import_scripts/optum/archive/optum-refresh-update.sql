/*
 * Refresh dates for each table 
 */


/*
 * Confinement
select year, extract(quarter from admit_date), min(admit_date), max(admit_date), count(*)  from optum_zip_backup.confinement group by 1, 2 order by 1, 2;
select year, extract(quarter from admit_date), min(admit_date), max(admit_date), count(*)  from optum_zip.confinement group by 1, 2 order by 1, 2;
 */

create table optum_zip.confinement_backup (like optum_zip.confinement)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into optum_zip.confinement_backup
select *
from optum_zip.confinement;

delete from optum_zip.confinement where admit_date >= (select min(admit_date) from optum_zip.confinement);

insert into optum_zip.confinement
select *
from optum_zip.confinement;

/*
 * Diagnostic
select year, extract(quarter from fst_dt), min(fst_dt), max(fst_dt), count(*)  from optum_zip_backup.diagnostic group by 1, 2 order by 1, 2;
select year, extract(quarter from fst_dt), min(fst_dt), max(fst_dt), count(*)  from optum_zip.diagnostic group by 1, 2 order by 1, 2;
 */

create table optum_zip.diagnostic_backup (like optum_zip.diagnostic)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into optum_zip.diagnostic_backup
select *
from optum_zip.diagnostic;

delete from optum_zip.diagnostic where fst_dt >= (select min(fst_dt) from optum_zip.diagnostic);

insert into optum_zip.diagnostic
select *
from optum_zip.diagnostic;

/*
Lab Result

select year, extract(quarter from fst_dt), min(fst_dt), max(fst_dt), count(*)  from optum_zip.lab_result group by 1, 2 order by 1, 2;
select year, extract(quarter from fst_dt), min(fst_dt), max(fst_dt), count(*)  from optum_zip.lab_result group by 1, 2 order by 1, 2;

**/

create table optum_zip.lab_result_backup (like optum_zip.lab_result)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into optum_zip.lab_result_backup
select *
from optum_zip.lab_result;

delete from optum_zip.lab_result where fst_dt >= (select min(fst_dt) from optum_zip.lab_result);

insert into optum_zip.lab_result
select *
from optum_zip.lab_result;

/*
Medical

select year, extract(quarter from fst_dt), min(fst_dt), max(fst_dt), count(*)  from optum_zip.medical group by 1, 2 order by 1, 2;
select year, extract(quarter from fst_dt), min(fst_dt), max(fst_dt), count(*)  from optum_zip.medical group by 1, 2 order by 1, 2;

**/

create table optum_zip.medical_backup (like optum_zip.medical)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into optum_zip.medical_backup
select *
from optum_zip.medical;

delete from optum_zip.medical where fst_dt >= (select min(fst_dt) from optum_zip.medical);

insert into optum_zip.medical
select * from optum_zip.medical;

/*
mbr_co_enroll

select count(*) from optum_zip.mbr_co_enroll;
select count(*) from optum_zip.mbr_co_enroll;
*/
create table optum_zip.mbr_co_enroll_backup (like optum_zip.mbr_co_enroll)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into optum_zip.mbr_co_enroll_backup
select * from optum_zip.mbr_co_enroll;

truncate optum_zip.mbr_co_enroll;

insert into optum_zip.mbr_co_enroll
select * from optum_zip.mbr_co_enroll;

/*
mbr_enroll

select count(*) from optum_zip.mbr_enroll;
select count(*) from optum_zip.mbr_enroll;
*/
create table optum_zip.mbr_enroll_backup (like optum_zip.mbr_enroll)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into optum_zip.mbr_enroll_backup
select * from optum_zip.mbr_enroll;

truncate optum_zip.mbr_enroll;

insert into optum_zip.mbr_enroll
select * from optum_zip.mbr_enroll;

/*
Procedure

select year, extract(quarter from fst_dt), min(fst_dt), max(fst_dt), count(*)  from optum_zip.procedure group by 1, 2 order by 1, 2;
select year, extract(quarter from fst_dt), min(fst_dt), max(fst_dt), count(*)  from optum_zip.procedure group by 1, 2 order by 1, 2;

**/

create table optum_zip.procedure_backup (like optum_zip.procedure)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into optum_zip.procedure_backup
select *
from optum_zip.procedure;

delete from optum_zip.procedure where fst_dt >= (select min(fst_dt) from optum_zip.procedure);

insert into optum_zip.procedure
select *
from optum_zip.procedure;

/*
provider

select count(*) from optum_zip.provider;
select count(*) from optum_zip.provider;
*/
create table optum_zip.provider_backup (like optum_zip.provider)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into optum_zip.provider_backup
select * from optum_zip.provider;

truncate optum_zip.provider;

insert into optum_zip.provider
select * from optum_zip.provider;

/*
provider_bridge

select count(*) from optum_zip.provider_bridge;
select count(*) from optum_zip.provider_bridge;
*/
create table optum_zip.provider_bridge_backup (like optum_zip.provider_bridge)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into optum_zip.provider_bridge_backup
select * from optum_zip.provider_bridge;

truncate optum_zip.provider_bridge;

insert into optum_zip.provider_bridge
select * from optum_zip.provider_bridge;

/*
RX
select year, extract(quarter from fill_dt), min(fill_dt), max(fill_dt), count(*)  from optum_zip_backup.rx group by 1, 2 order by 1, 2;
select year, extract(quarter from fill_dt), min(fill_dt), max(fill_dt), count(*)  from optum_zip.rx group by 1, 2 order by 1, 2;
 */
create table optum_zip.rx_backup (like optum_zip.rx)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into optum_zip.rx_backup
select * from optum_zip.rx;

delete from optum_zip.rx where fill_dt >= (select min(fill_dt) from optum_zip.rx);

insert into optum_zip.rx
select * from optum_zip.rx;

/*
Clean up everything
Leave backups till manually vetted
*/

vacuum full optum_zip.confinement;
vacuum full optum_zip.diagnostic;
vacuum full optum_zip.lab_result;
vacuum full optum_zip.mbr_co_enroll;
vacuum full optum_zip.mbr_enroll;
vacuum full optum_zip.medical;
vacuum full optum_zip.procedure;
vacuum full optum_zip.provider;
vacuum full optum_zip.provider_bridge;
vacuum full optum_zip.rx;
