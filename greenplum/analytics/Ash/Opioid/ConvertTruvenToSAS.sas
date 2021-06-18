
libname pc "O:\Opioid2021\Truv_V3";
*libname pc1 "H:\SPH\Opioid\New\CSVfromDW";

PROC IMPORT DATAFILE= 'H:\Projects\Opiod\TestData\am_truv_opioid_ccaed.csv' 
out = pc.am_truv_opioid_ccaed
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;

PROC IMPORT DATAFILE= 'H:\Projects\Opiod\TestData\am_truv_opioid_ccaef.csv' 
out = pc.am_truv_opioid_ccaef
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;

PROC IMPORT DATAFILE= 'H:\Projects\Opiod\TestData\am_truv_opioid_ccaei.csv' 
out = pc.am_truv_opioid_ccaei
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;
PROC IMPORT DATAFILE= 'H:\Projects\Opiod\TestData\am_truv_opioid_ccaeo.csv' 
out = pc.am_truv_opioid_ccaeo
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;
PROC IMPORT DATAFILE= 'H:\Projects\Opiod\TestData\am_truv_opioid_ccaes.csv' 
out = pc.am_truv_opioid_ccaes
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;
PROC IMPORT DATAFILE= 'H:\Projects\Opiod\TestData\am_truv_opioid_mdcrd.csv' 
out = pc.am_truv_opioid_mdcrd
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;
PROC IMPORT DATAFILE= 'H:\Projects\Opiod\TestData\am_truv_opioid_mdcrf.csv' 
out = pc.am_truv_opioid_mdcrf
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;
PROC IMPORT DATAFILE= 'H:\Projects\Opiod\TestData\am_truv_opioid_mdcri.csv' 
out = pc.am_truv_opioid_mdcri
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;
PROC IMPORT DATAFILE= 'H:\Projects\Opiod\TestData\am_truv_opioid_mdcro.csv' 
out = pc.am_truv_opioid_mdcro
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;
PROC IMPORT DATAFILE= 'H:\Projects\Opiod\TestData\am_truv_opioid_mdcrs.csv' 
out = pc.am_truv_opioid_mdcrs
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;
PROC IMPORT DATAFILE= 'H:\Projects\Opiod\TestData\am_truv_opioid_members.csv' 
out = pc.am_truv_opioid_members
dbms=csv REPLACE;
GUESSINGROWS=10000;
RUN;
