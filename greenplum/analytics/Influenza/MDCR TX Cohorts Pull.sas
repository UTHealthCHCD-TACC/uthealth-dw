** W. Coughlin  3/30/2020;


libname dbx "T:\COPD Siddharth\TX";

libname mdcr14 "X:\2014\TX";

libname mdcr15 "X:\2015\TX";

libname mdcr16 "X:\2016\TX";

libname mdcr17 "X:\2017\TX";

libname dxtx "X:\DX\TX-Diag";





proc sql;

select * 
from mdcr17.mbsf_abcd_summary
where BENE_BIRTH_DT <= '01JAN1948'
;


quit; 
