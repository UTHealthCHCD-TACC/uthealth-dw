

select * --distinct COND_CD 
from CONDPROC.dbo.wellnesscode w 




insert into CONDPROC.dbo.wellnesscode values
                                             ('1','HPV','CPT/HCPCS','90649'),
                                             ('1','HPV','CPT/HCPCS','90650'),
                                             ('1','HPV','CPT/HCPCS','90651')
                                             ;
                                             
                                            
select * 
from trsers.dbo.ers_uhcmedclm a 
where a.HCPCSCPTCode in ('90649','90650','90651')
  and FSCYR = 2016
;


select *, dateadd(month,-12,(datefromparts(FSCYR,month(dob),1)) )
from TRSERS.dbo.ERS_AGG_YR
where FSCYR = 2019
  and age = 13
  and enrlmnth = 12
;

select distinct fscyr from TRSERS.dbo.TRS_AGG_YR_FIN tayf 



