


---cervical cancer
select distinct uth_member_id, year, data_source 
into dev.wc_dr_cervical_clms
from data_warehouse.claim_detail cd 
where cd.year  between 2018 and 2019 
  and cd.data_source in ('mcrt','optz') 
  and cpt_hcpcs in ( '88141','88142','88143','88147','88148','88150','88152','88153','88154','88164','88165','88166','88167','88174','88175',
					 'G0123','G0124','G0141','G0143','G0144','G0145','G0147','G0148','P3000','P3001','Q0091',
                        '87620' ,'87621','87622','87624','87625','G0476')
;


  ---cerv cancer counts
 select count(*), a.data_source , a.year, bus_cd , avg(a.age_derived ) as mean_age 
 from data_warehouse.member_enrollment_yearly a
    join dev.wc_dr_cervical_clms b       on b.uth_member_id = a.uth_member_id  and b.year = a.year 
 where a.gender_cd in ('F')
   and a.age_derived between 21 and 64 
   and a.state = 'TX'
   and a.year between 2018 and 2019
   and a.data_source = 'optz' and a.bus_cd = 'COM'
group by a.data_source , a.year , bus_cd
order by a.data_source , a.year , bus_cd
;



--breast cancer-------------------------------
select distinct uth_member_id, year, data_source 
into dev.wc_dr_breast_clms
from data_warehouse.claim_detail cd 
where cd.year  between 2018 and 2019 
  and cd.data_source in ('mcrt','optz') 
  and cpt_hcpcs in ('77055','77056','77057', '77061','77062','77063', '77065','77066','77067','G0202', 'G0204', 'G0206')
  ; 

  
  ---breast cancer counts
 select count(*), a.data_source , a.year, bus_cd , avg(a.age_derived ) as mean_age 
 from data_warehouse.member_enrollment_yearly a
     join dev.wc_dr_breast_clms b       on b.uth_member_id = a.uth_member_id  and b.year = a.year 
 where a.gender_cd in ('F')
   and a.age_derived between 40 and 75 
   and a.state = 'TX'
   and a.year between 2018 and 2019
   and a.data_source in ('mcrt','optz')
group by a.data_source , a.year , bus_cd
order by a.data_source , a.year , bus_cd
;
  
  
  
--colorectal ------------------------
select distinct uth_member_id, year, data_source 
into dev.wc_dr_colorectal_clms
from data_warehouse.claim_detail cd 
where cd.year  between 2018 and 2019 
  and cd.data_source in ('mcrt','optz') 
  and cpt_hcpcs in ('82270', '82274', '81528', '45330','45331','45332','45333','45334','45335', '45337','45338','45339','45340','45341','45342',
  '45345','45346','45347','45349', '45350', '44388','44389','44390','44391','44392','44393','44394', '44397', '44401','44402','44403','44404','44405','44406','44407','44408',
  '45355','45378','45379','45380','45381','45382','45383','45384','45385','45386','45387','45388','45389','45390','45391','45392','45393',
  '45398', '74261', '74262', '74263' ,'G0328', 'G0464', 'G0104', 'G0105', 'G0121')
  ;
 
 
 --colortecal counts
 select count(*), a.data_source , a.year, bus_cd , avg(a.age_derived ) as mean_age --, gender_cd 
 from data_warehouse.member_enrollment_yearly a
     join dev.wc_dr_colorectal_clms b       on b.uth_member_id = a.uth_member_id  and b.year = a.year 
 where a.gender_cd in ('M','F')
   and a.age_derived between 50 and 75 
   and a.state = 'TX'
   and a.year between 2018 and 2019
   and a.data_source in ('mcrt','optz')
group by a.data_source , a.year , bus_cd--, gender_cd 
order by a.data_source , a.year , bus_cd--, gender_cd 
;

