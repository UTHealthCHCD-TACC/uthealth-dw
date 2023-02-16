


---cervical cancer
select distinct patid 
into STAGE.dbo.wc_dr_cervical_clms
from OPT_ZIP.dbo.zip5_m2020q1q2q3 cd 
where cd.PROC_CD in ( '88141','88142','88143','88147','88148','88150','88152','88153','88154','88164','88165','88166','88167','88174','88175',
					 'G0123','G0124','G0141','G0143','G0144','G0145','G0147','G0148','P3000','P3001','Q0091',
                        '87620' ,'87621','87622','87624','87625','G0476')
;


select a.ZIPCODE_5, right(ZIPCODE_5,5) , a.yrdob, (2020 - a.YRDOB) between 21 and 64
from OPT_ZIP.dbo.zip5_mbr_enroll a

  ---cerv cancer counts
 select count(distinct a.patid ),  bus , avg(2020.0-yrdob) as mean_age
 from OPT_ZIP.dbo.zip5_mbr_enroll a
    join [REF].dbo.ZipCode z  
       on z.ZIP = right(ZIPCODE_5,5) 
      and z.State = 'TX'
    join STAGE.dbo.wc_dr_cervical_clms b       on b.patid = a.patid 
 where a.BUS = 'COM'
   and a.ELIGend >= '2020-01-01'
   and a.GDR_CD = 'F' and (2020 - a.YRDOB) between 21 and 64
group by a.bus
order by a.bus 
;



--breast cancer-------------------------------
select distinct patid 
into STAGE.dbo.wc_dr_breast_clms
from OPT_ZIP.dbo.zip5_m2020q1q2q3 cd 
where cd.PROC_CD in ('77055','77056','77057', '77061','77062','77063', '77065','77066','77067','G0202', 'G0204', 'G0206')
  ; 

  
  ---breast cancer counts
 select count(distinct a.patid ),  bus , avg(2020.0-yrdob) as mean_age
 from OPT_ZIP.dbo.zip5_mbr_enroll a
    join [REF].dbo.ZipCode z  
       on z.ZIP = right(ZIPCODE_5,5) 
      and z.State = 'TX'
    join STAGE.dbo.wc_dr_breast_clms b       on b.patid = a.patid 
 where a.BUS = 'COM'
   and a.ELIGend >= '2020-01-01'
   and a.GDR_CD = 'F' and (2020 - a.YRDOB) between 40 and 75
group by a.bus
order by a.bus 
;
  
  
--colorectal ------------------------
select distinct patid 
into STAGE.dbo.wc_dr_colorectal_clms
from OPT_ZIP.dbo.zip5_m2020q1q2q3 cd 
where cd.PROC_CD in  ('82270', '82274', '81528', '45330','45331','45332','45333','45334','45335', '45337','45338','45339','45340','45341','45342',
  '45345','45346','45347','45349', '45350', '44388','44389','44390','44391','44392','44393','44394', '44397', '44401','44402','44403','44404','44405','44406','44407','44408',
  '45355','45378','45379','45380','45381','45382','45383','45384','45385','45386','45387','45388','45389','45390','45391','45392','45393',
  '45398', '74261', '74262', '74263' ,'G0328', 'G0464', 'G0104', 'G0105', 'G0121')
  ;
 
 
 --colortecal counts
 select count(distinct a.patid ),  bus , avg(2020.0-yrdob) as mean_age, a.GDR_CD 
 from OPT_ZIP.dbo.zip5_mbr_enroll a
    join [REF].dbo.ZipCode z  
       on z.ZIP = right(ZIPCODE_5,5) 
      and z.State = 'TX'
    join STAGE.dbo.wc_dr_colorectal_clms b       on b.patid = a.patid 
 where a.BUS = 'COM'
   and a.ELIGend >= '2020-01-01'
   and (2020 - a.YRDOB) between 40 and 75
group by a.bus, a.GDR_CD 
order by a.bus , a.GDR_CD 
;
  
