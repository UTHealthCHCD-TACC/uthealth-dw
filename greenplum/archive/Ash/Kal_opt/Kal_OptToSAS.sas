
libname pc "O:\Kal_opt";

PROC IMPORT DATAFILE= 'O:\Kal_opt\csv\am_mbp_enrollment.csv' 
out = pc.member
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;

PROC IMPORT DATAFILE= 'O:\Kal_opt\csv\am_mbp_medical.csv' 
out = pc.medical
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;

PROC IMPORT DATAFILE= 'O:\Kal_opt\csv\am_mbp_provider.csv' 
out = pc.provider
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;
