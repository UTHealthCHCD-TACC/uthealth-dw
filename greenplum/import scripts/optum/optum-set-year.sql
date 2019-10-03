update optum_dod.rx set year=date_part('year', fill_dt);
update optum_dod.confinement set year=date_part('year', fill_dt);
update optum_dod.facility_detail set year=date_part('year', fill_dt);
update optum_dod.lab_result set year=date_part('year', fill_dt);
update optum_dod.medical set year=date_part('year', fill_dt);
update optum_dod.procedure set year=date_part('year', fill_dt);


select distinct year from optum_dod.procedure;


-- add quarter column and year
