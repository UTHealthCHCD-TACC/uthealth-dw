

--all claims
select count(distinct a.msclmid) as clm_cnt,  sum(a.netpay) as netpay 
from truven.ccaeo a
where a.stdplac in ('2','11','17','20','49','50','53','71','72')
  and a.year = 2019
  and a.egeoloc = 49
;


--telehealth claims
select count(distinct a.msclmid) as clm_cnt,sum(a.netpay) as netpay 
from truven.ccaeo a
where ( a.proc1 in ('99444','98969','G0508','G0509','Q3014','G0459','G0406','G0425',
	              'G0407','G0408','G0426','G0427') 
	   or a.procmod in ('95','GQ','GT')
	  )
  and a.year = 2019
  and a.egeoloc = 49
;



select count(distinct a.clmid) as clms, sum(a.charge)
from optum_zip.medical a
   join data_warehouse.dim_uth_member_id b
      on b.member_id_src = a.patid::text 
  join data_warehouse.member_enrollment_yearly c 
      on b.uth_member_id = c.uth_member_id
     and c.year = 2019
     and c.state = 'TX'
where ( a.proc_cd in ('99444','98969','G0508','G0509','Q3014','G0459','G0406','G0425',
	              'G0407','G0408','G0426','G0427')	                      
	   or a.procmod in  ('95','GQ','GT') 
	   or a.procmod2 in ('95','GQ','GT') 
	   or a.procmod3 in ('95','GQ','GT') 
	   or a.procmod4 in ('95','GQ','GT') 
	   )	  
  and  extract(year from a.fst_dt) = 2019
;



---physician prof
select count(distinct a.msclmid) as clm_cnt, sum(a.netpay) as netpay 
from truven.ccaeo a
where a.stdplac in ('2','11','17','20','49','50','53','71','72')
  and a.year = 2019
  and a.egeoloc = 49
  and a.stdprov in ('430','433','435','438','440','443','448','450','453','455','460','500','505','510','520','530','535',
                    '540','545','140','145','150','160','170','175','180','185','200','202','204','206','208','210','215',
                    '220','225','227','230','240','245','250','260','265','550','555','560','565','570','575','580','585',
                    '270','275','280','285','290','295','300','320','325','330','340','350','355','360','380','400','410',
                    '413','415','418','420','423','425','428')
;

---physician tele 
select count(distinct a.msclmid) as clm_cnt, sum(a.netpay) as netpay 
from truven.ccaeo a
where ( a.proc1 in ('99444','98969','G0508','G0509','Q3014','G0459','G0406','G0425',
	              'G0407','G0408','G0426','G0427') 
	   or a.procmod in ('95','GQ','GT')
	  )
  and a.year = 2019
  and a.egeoloc = 49
  and a.stdprov in ('430','433','435','438','440','443','448','450','453','455','460','500','505','510','520','530','535',
                    '540','545','140','145','150','160','170','175','180','185','200','202','204','206','208','210','215',
                    '220','225','227','230','240','245','250','260','265','550','555','560','565','570','575','580','585',
                    '270','275','280','285','290','295','300','320','325','330','340','350','355','360','380','400','410',
                    '413','415','418','420','423','425','428')
;


---physician assisstant prof
select count(distinct a.msclmid) as clm_cnt, sum(a.netpay) as netpay 
from truven.ccaeo a
where a.stdplac in ('2','11','17','20','49','50','53','71','72')
  and a.year = 2019
  and a.egeoloc = 49
  and a.stdprov in ('825','845')
;

---physician assisstant tele
select count(distinct a.msclmid) as clm_cnt, sum(a.netpay) as netpay 
from truven.ccaeo a
where ( a.proc1 in ('99444','98969','G0508','G0509','Q3014','G0459','G0406','G0425',
	              'G0407','G0408','G0426','G0427') 
	   or a.procmod in ('95','GQ','GT')
	  )
  and a.year = 2019
  and a.egeoloc = 49
  and a.stdprov in (825,845)
;

---psychiatrist prof
select count(distinct a.msclmid) as clm_cnt, sum(a.netpay) as netpay 
from truven.ccaeo a
where a.stdplac in ('2','11','17','20','49','50','53','71','72')
  and a.year = 2019
  and a.egeoloc = 49
  and a.stdprov in (458,365)
;

---psychiatrist tele
select count(distinct a.msclmid) as clm_cnt, sum(a.netpay) as netpay 
from truven.ccaeo a
where ( a.proc1 in ('99444','98969','G0508','G0509','Q3014','G0459','G0406','G0425',
	              'G0407','G0408','G0426','G0427') 
	   or a.procmod in ('95','GQ','GT')
	  )
  and a.year = 2019
  and a.egeoloc = 49
  and a.stdprov in (458,365)
;


---psychologist prof
select count(distinct a.msclmid) as clm_cnt, sum(a.netpay) as netpay 
from truven.ccaeo a
where a.stdplac in ('2','11','17','20','49','50','53','71','72')
  and a.year = 2019
  and a.egeoloc = 49
  and a.stdprov = 860
;  


---psychologist tele
select count(distinct a.msclmid) as clm_cnt, sum(a.netpay) as netpay 
from truven.ccaeo a
where  ( a.proc1 in ('99444','98969','G0508','G0509','Q3014','G0459','G0406','G0425',
	              'G0407','G0408','G0426','G0427') 
	   or a.procmod in ('95','GQ','GT')
	  )
  and a.year = 2019
  and a.egeoloc = 49
  and a.stdprov = 860
;  
  
  
---occupational therapist

---physical therapist prof
select count(distinct a.msclmid) as clm_cnt, sum(a.netpay) as netpay 
from truven.ccaeo a
where a.stdplac in ('2','11','17','20','49','50','53','71','72')
  and a.year = 2019
  and a.egeoloc = 49
  and a.stdprov = 850
;

---physical therapist tele
select count(distinct a.msclmid) as clm_cnt, sum(a.netpay) as netpay 
from truven.ccaeo a
where  ( a.proc1 in ('99444','98969','G0508','G0509','Q3014','G0459','G0406','G0425',
	              'G0407','G0408','G0426','G0427') 
	   or a.procmod in ('95','GQ','GT')
	  )
  and a.year = 2019
  and a.egeoloc = 49
  and a.stdprov = 850
;


---nutritionist prof
select count(distinct a.msclmid) as clm_cnt, sum(a.netpay) as netpay 
from truven.ccaeo a
where a.stdplac in ('2','11','17','20','49','50','53','71','72')
  and a.year = 2019
  and a.egeoloc = 49
  and a.stdprov = 810
;

---nutritionist tele
select count(distinct a.msclmid) as clm_cnt, sum(a.netpay) as netpay 
from truven.ccaeo a
where  ( a.proc1 in ('99444','98969','G0508','G0509','Q3014','G0459','G0406','G0425',
	              'G0407','G0408','G0426','G0427') 
	   or a.procmod in ('95','GQ','GT')
	  )
  and a.year = 2019
  and a.egeoloc = 49
  and a.stdprov = 810
;


---audiologist 
not available



---all other providers
select count(distinct a.msclmid) as clm_cnt, sum(a.netpay) as netpay 
from truven.ccaeo a
where a.stdplac in ('2','11','17','20','49','50','53','71','72')
  and a.year = 2019
  and a.egeoloc = 49
  and a.stdprov not in ('430','433','435','438','440','443','448','450','453','455','460','500','505','510','520','530','535',
                    '540','545','140','145','150','160','170','175','180','185','200','202','204','206','208','210','215',
                    '220','225','227','230','240','245','250','260','265','550','555','560','565','570','575','580','585',
                    '270','275','280','285','290','295','300','320','325','330','340','350','355','360','380','400','410',
                    '413','415','418','420','423','425','428','810','850','860','458','365','825','845')
;

---physician tele 
select count(distinct a.msclmid) as clm_cnt, sum(a.netpay) as netpay 
from truven.ccaeo a
where ( a.proc1 in ('99444','98969','G0508','G0509','Q3014','G0459','G0406','G0425',
	              'G0407','G0408','G0426','G0427') 
	   or a.procmod in ('95','GQ','GT')
	  )
  and a.year = 2019
  and a.egeoloc = 49
  and a.stdprov not in ('430','433','435','438','440','443','448','450','453','455','460','500','505','510','520','530','535',
                    '540','545','140','145','150','160','170','175','180','185','200','202','204','206','208','210','215',
                    '220','225','227','230','240','245','250','260','265','550','555','560','565','570','575','580','585',
                    '270','275','280','285','290','295','300','320','325','330','340','350','355','360','380','400','410',
                    '413','415','418','420','423','425','428','810','850','860','458','365','825','845')
;
