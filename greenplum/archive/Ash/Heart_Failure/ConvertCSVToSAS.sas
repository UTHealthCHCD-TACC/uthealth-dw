
libname pc1 "O:\heart_failure\DOD";
libname pc2 "O:\heart_failure\ZIP";

PROC IMPORT DATAFILE= 'O:\heart_failure\am_hf_optd_confinement.csv' 
out = pc1.optd_confinement
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;

PROC IMPORT DATAFILE= 'O:\heart_failure\am_hf_optd_diagnostic.csv' 
out = pc1.optd_diagnostic
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;

PROC IMPORT DATAFILE= 'O:\heart_failure\am_hf_optd_lab_result.csv' 
out = pc1.optd_lab_result
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;

PROC IMPORT DATAFILE= 'O:\heart_failure\am_hf_optd_lu_diagnosis.csv' 
out = pc1.optd_lu_diagnosis
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;

PROC IMPORT DATAFILE= 'O:\heart_failure\am_hf_optd_lu_ndc.csv' 
out = pc1.optd_lu_ndc
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;


PROC IMPORT DATAFILE= 'O:\heart_failure\am_hf_optd_medical.csv' 
out = pc1.optd_medical
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;

PROC IMPORT DATAFILE= 'O:\heart_failure\am_hf_optd_member_enrollment.csv' 
out = pc1.optd_member_enrollment
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;

PROC IMPORT DATAFILE= 'O:\heart_failure\am_hf_optd_procedure.csv' 
out = pc1.optd_procedure
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;

PROC IMPORT DATAFILE= 'O:\heart_failure\am_hf_optd_rx.csv' 
out = pc1.optd_rx
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;





PROC IMPORT DATAFILE= 'O:\heart_failure\am_hf_optz_confinement.csv' 
out = pc2.optz_confinement
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;

PROC IMPORT DATAFILE= 'O:\heart_failure\am_hf_optz_diagnostic.csv' 
out = pc2.optz_diagnostic
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;

PROC IMPORT DATAFILE= 'O:\heart_failure\am_hf_optz_lab_result.csv' 
out = pc2.optz_lab_result
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;

PROC IMPORT DATAFILE= 'O:\heart_failure\am_hf_optz_lu_diagnosis.csv' 
out = pc2.optz_lu_diagnosis
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;

PROC IMPORT DATAFILE= 'O:\heart_failure\am_hf_optz_lu_ndc.csv' 
out = pc2.optz_lu_ndc
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;


PROC IMPORT DATAFILE= 'O:\heart_failure\am_hf_optz_medical.csv' 
out = pc2.optz_medical
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;

PROC IMPORT DATAFILE= 'O:\heart_failure\am_hf_optz_member_enrollment.csv' 
out = pc2.optz_member_enrollment
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;

PROC IMPORT DATAFILE= 'O:\heart_failure\am_hf_optz_procedure.csv' 
out = pc2.optz_procedure
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;

PROC IMPORT DATAFILE= 'O:\heart_failure\am_hf_optz_rx.csv' 
out = pc2.optz_rx
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;
