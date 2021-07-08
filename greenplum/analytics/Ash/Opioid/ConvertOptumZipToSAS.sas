
libname pc "O:\Opioid2021\Optz_V3";
*libname pc1 "H:\SPH\Opioid\New\CSVfromDW";

PROC IMPORT DATAFILE= 'H:\Projects\Opiod\TestData\am_optz_opioid_confinement_com.csv' 
out = pc.am_optz_opioid_confinement_com
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;

PROC IMPORT DATAFILE= 'H:\Projects\Opiod\TestData\am_optz_opioid_confinement_mcr.csv' 
out = pc.am_optz_opioid_confinement_mcr
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;

PROC IMPORT DATAFILE= 'H:\Projects\Opiod\TestData\am_optz_opioid_diagnostic_com.csv' 
out = pc.am_optz_opioid_diagnostic_com
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;
PROC IMPORT DATAFILE= 'H:\Projects\Opiod\TestData\am_optz_opioid_diagnostic_mcr.csv' 
out = pc.am_optz_opioid_diagnostic_mcr
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;
PROC IMPORT DATAFILE= 'H:\Projects\Opiod\TestData\am_optz_opioid_medical_com.csv' 
out = pc.am_optz_opioid_medical_com
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;
PROC IMPORT DATAFILE= 'H:\Projects\Opiod\TestData\am_optz_opioid_medical_mcr.csv' 
out = pc.am_optz_opioid_medical_mcr
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;
PROC IMPORT DATAFILE= 'H:\Projects\Opiod\TestData\am_optz_opioid_members.csv' 
out = pc.am_optz_opioid_members
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;
PROC IMPORT DATAFILE= 'H:\Projects\Opiod\TestData\am_optz_opioid_procedure_com.csv' 
out = pc.am_optz_opioid_procedure_com
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;
PROC IMPORT DATAFILE= 'H:\Projects\Opiod\TestData\am_optz_opioid_procedure_mcr.csv' 
out = pc.am_optz_opioid_procedure_mcr
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;
PROC IMPORT DATAFILE= 'H:\Projects\Opiod\TestData\am_optz_opioid_rx_com.csv' 
out = pc.am_optz_opioid_rx_com
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;
PROC IMPORT DATAFILE= 'H:\Projects\Opiod\TestData\am_optz_opioid_rx_mcr.csv' 
out = pc.am_optz_opioid_rx_mcr
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;
