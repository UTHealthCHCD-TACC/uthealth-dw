update optum_zip.rx set year=date_part('year', fill_dt);
update optum_zip.confinement set year=date_part('year', fill_dt);
update optum_zip.facility_detail set year=date_part('year', fill_dt);
update optum_zip.lab_result set year=date_part('year', fill_dt);
update optum_zip.medical set year=date_part('year', fill_dt);
update optum_zip.procedure set year=date_part('year', fill_dt);


select distinct year from optum_zip.procedure;


-- add quarter column and year
