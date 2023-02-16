--Annual Preventive Exam Aggregated 65 plus 2019


---optum and truven cohorts from DW
drop table dev.wc_flu_65plus_2019;

--commercial
select uth_member_id, 
       a.gender_cd, 
       a.zip3,
       data_source 
 into dev.wc_flu_65plus_2019
from data_warehouse.member_enrollment_yearly a
where a.data_source in ('truv','optz')
  and a.year = 2019
  and a.bus_cd = 'COM'
  and a.state = 'TX'
  and a.age_derived >= 65
  and a.zip3 between '750' and '799'
  and a.total_enrolled_months = 12
  and a.gender_cd in ('M','F')
;

---medicare advantage
insert  into dev.wc_flu_65plus_2019
select uth_member_id, 
       a.gender_cd, 
       a.zip3,
       case when data_source = 'truv' then 'trma' 
            when data_source = 'optz' then 'opma' 
       end as data_source 
from data_warehouse.member_enrollment_yearly a
where a.data_source in ('truv','optz')
  and a.year = 2019
  and a.bus_cd = 'MCR'
  and a.state = 'TX'
  and a.age_derived >= 65
  and a.zip3 between '750' and '799'
  and a.total_enrolled_months = 12
  and a.gender_cd in ('M','F')
;


---medicare texas 
insert into dev.wc_flu_65plus_2019
select a.uth_member_id, 
       a.gender_cd, 
       a.zip3,
       a.data_source 
from data_warehouse.member_enrollment_yearly a
  join data_warehouse.medicare_mbsf_abcd_enrollment b 
    on a.uth_member_id = b.uth_member_id 
   and b.bene_hi_cvrage_tot_mons = 12
   and b.bene_smi_cvrage_tot_mons > 0
   and b.year  = a.year  
where a.data_source = 'mcrt'
  and a.year = 2019 
  and a.state = 'TX'
  and a.age_derived >= 65
  and a.zip3 between '750' and '799'
  and a.total_enrolled_months = 12
  and a.gender_cd in ('M','F')
;


---medicaid 
insert into dev.wc_flu_65plus_2019 
select a.uth_member_id, 
       m.sex, 
       substring(m.zip3,1,3) as zip3, 
       a.data_source 
 from medicaid.agg_enrl_medicaid_cy1220 m  
    join data_warehouse.dim_uth_member_id a 
       on a.data_source = 'mdcd' 
      and a.member_id_src = m.client_nbr
 where m.age >= 65
   and m.enrl_cy = 2019
   and m.enrl_months = 12 
   and m.sex in ('M','F') 
   and m.zip3::int between 750 and 799 
  ;
 
 
--cleanup   
delete from dev.wc_flu_65plus_2019 where zip3 = '771';

delete from dev.wc_flu_65plus_2019 where length(zip3::text) = 2;

--vacc table from all other scripts
alter table dev.wc_flu_65plus_2019 add column vacc_flag int2 default 0;


update dev.wc_flu_65plus_2019 a set vacc_flag = 1
  from dev.wc_flu_2019_vacc b 
    where b.uth_member_id = a.uth_member_id
 ;

------ Calculations ---------------------------

----------------------------------------------------------------------------------------
---********************** Prevalance All **************************
----------------------------------------------------------------------------------------

--prevalance - row 51  optz truv mdcd mdcr
select ( sum(vacc_flag) / count(uth_member_id)::float ) as prev, count(uth_member_id), sum(vacc_flag), data_source
from dev.wc_flu_65plus_2019 a 
group by data_source
  order by data_source desc 
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id), sum(vacc_flag), data_source
from dev.wc_flu_65plus_2019 a  
where a.gender_cd = 'F'
  group by data_source
  order by data_source desc 
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id), sum(vacc_flag), data_source
from dev.wc_flu_65plus_2019 a 
where  a.gender_cd = 'M'
  group by data_source
    order by data_source desc 
;


insert into dev.wc_flu_65plus_2019 values 

----------------------------------------------------------------------------------------
---********************** Prevalance by ZIP **************************
---
--!  go to bottom and insert filler values  !
----------------------------------------------------------------------------------------

---truven COM
select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id) as mem
from dev.wc_flu_65plus_2019 a 
where a.data_source = 'truv'
group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id) as mem
from dev.wc_flu_65plus_2019 a 
where a.data_source =  'truv'
  and a.gender_cd = 'F'
  group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id) as mem
from dev.wc_flu_65plus_2019 a 
where a.data_source = 'truv'
  and a.gender_cd = 'M'
  group by a.zip3 
order by a.zip3
;

---truven MA
select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id) as mem
from dev.wc_flu_65plus_2019 a 
where a.data_source = 'trma'
group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id) as mem
from dev.wc_flu_65plus_2019 a 
where a.data_source =  'trma'
  and a.gender_cd = 'F'
  group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id) as mem
from dev.wc_flu_65plus_2019 a 
where a.data_source = 'trma'
  and a.gender_cd = 'M'
  group by a.zip3 
order by a.zip3
;

---optum COM
select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id) as mem
from dev.wc_flu_65plus_2019 a 
where a.data_source = 'optz'
group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id) as mem
from dev.wc_flu_65plus_2019 a 
where a.data_source =  'optz'
  and a.gender_cd = 'F'
  group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id) as mem
from dev.wc_flu_65plus_2019 a 
where a.data_source = 'optz'
  and a.gender_cd = 'M'
  group by a.zip3 
order by a.zip3
;
 
---optum MA
select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id) as mem
from dev.wc_flu_65plus_2019 a 
where a.data_source = 'opma'
group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id) as mem
from dev.wc_flu_65plus_2019 a 
where a.data_source =  'opma'
  and a.gender_cd = 'F'
  group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id) as mem
from dev.wc_flu_65plus_2019 a 
where a.data_source = 'opma'
  and a.gender_cd = 'M'
  group by a.zip3 
order by a.zip3
;


---medicaid
select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id) as mem
from dev.wc_flu_65plus_2019 a 
where a.data_source = 'mdcd'
group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id) as mem
from dev.wc_flu_65plus_2019 a 
where a.data_source =  'mdcd'
  and a.gender_cd = 'F'
  group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id) as mem
from dev.wc_flu_65plus_2019 a 
where a.data_source = 'mdcd'
  and a.gender_cd = 'M'
  group by a.zip3 
order by a.zip3
;

---medicare
select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id) as mem
from dev.wc_flu_65plus_2019 a 
where a.data_source = 'mcrt'
group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id) as mem
from dev.wc_flu_65plus_2019 a 
where a.data_source =  'mcrt'
  and a.gender_cd = 'F'
  group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id) as mem
from dev.wc_flu_65plus_2019 a 
where a.data_source = 'mcrt'
  and a.gender_cd = 'M'
  group by a.zip3 
order by a.zip3
;



--- FILLER VALUES

insert into dev.wc_flu_65plus_2019 values ('1','M','750','optz','0'),
('2','M','751','optz','0'),
('3','M','752','optz','0'),
('4','M','753','optz','0'),
('5','M','754','optz','0'),
('6','M','755','optz','0'),
('7','M','756','optz','0'),
('8','M','757','optz','0'),
('9','M','758','optz','0'),
('10','M','759','optz','0'),
('11','M','760','optz','0'),
('12','M','761','optz','0'),
('13','M','762','optz','0'),
('14','M','763','optz','0'),
('15','M','764','optz','0'),
('16','M','765','optz','0'),
('17','M','766','optz','0'),
('18','M','767','optz','0'),
('19','M','768','optz','0'),
('20','M','769','optz','0'),
('21','M','770','optz','0'),
('22','M','772','optz','0'),
('23','M','773','optz','0'),
('24','M','774','optz','0'),
('25','M','775','optz','0'),
('26','M','776','optz','0'),
('27','M','777','optz','0'),
('28','M','778','optz','0'),
('29','M','779','optz','0'),
('30','M','780','optz','0'),
('31','M','781','optz','0'),
('32','M','782','optz','0'),
('33','M','783','optz','0'),
('34','M','784','optz','0'),
('35','M','785','optz','0'),
('36','M','786','optz','0'),
('37','M','787','optz','0'),
('38','M','788','optz','0'),
('39','M','789','optz','0'),
('40','M','790','optz','0'),
('41','M','791','optz','0'),
('42','M','792','optz','0'),
('43','M','793','optz','0'),
('44','M','794','optz','0'),
('45','M','795','optz','0'),
('46','M','796','optz','0'),
('47','M','797','optz','0'),
('48','M','798','optz','0'),
('49','M','799','optz','0'),
('50','F','750','optz','0'),
('51','F','751','optz','0'),
('52','F','752','optz','0'),
('53','F','753','optz','0'),
('54','F','754','optz','0'),
('55','F','755','optz','0'),
('56','F','756','optz','0'),
('57','F','757','optz','0'),
('58','F','758','optz','0'),
('59','F','759','optz','0'),
('60','F','760','optz','0'),
('61','F','761','optz','0'),
('62','F','762','optz','0'),
('63','F','763','optz','0'),
('64','F','764','optz','0'),
('65','F','765','optz','0'),
('66','F','766','optz','0'),
('67','F','767','optz','0'),
('68','F','768','optz','0'),
('69','F','769','optz','0'),
('70','F','770','optz','0'),
('71','F','772','optz','0'),
('72','F','773','optz','0'),
('73','F','774','optz','0'),
('74','F','775','optz','0'),
('75','F','776','optz','0'),
('76','F','777','optz','0'),
('77','F','778','optz','0'),
('78','F','779','optz','0'),
('79','F','780','optz','0'),
('80','F','781','optz','0'),
('81','F','782','optz','0'),
('82','F','783','optz','0'),
('83','F','784','optz','0'),
('84','F','785','optz','0'),
('85','F','786','optz','0'),
('86','F','787','optz','0'),
('87','F','788','optz','0'),
('88','F','789','optz','0'),
('89','F','790','optz','0'),
('90','F','791','optz','0'),
('91','F','792','optz','0'),
('92','F','793','optz','0'),
('93','F','794','optz','0'),
('94','F','795','optz','0'),
('95','F','796','optz','0'),
('96','F','797','optz','0'),
('97','F','798','optz','0'),
('98','F','799','optz','0'),
('99','M','750','opma','0'),
('100','M','751','opma','0'),
('101','M','752','opma','0'),
('102','M','753','opma','0'),
('103','M','754','opma','0'),
('104','M','755','opma','0'),
('105','M','756','opma','0'),
('106','M','757','opma','0'),
('107','M','758','opma','0'),
('108','M','759','opma','0'),
('109','M','760','opma','0'),
('110','M','761','opma','0'),
('111','M','762','opma','0'),
('112','M','763','opma','0'),
('113','M','764','opma','0'),
('114','M','765','opma','0'),
('115','M','766','opma','0'),
('116','M','767','opma','0'),
('117','M','768','opma','0'),
('118','M','769','opma','0'),
('119','M','770','opma','0'),
('120','M','772','opma','0'),
('121','M','773','opma','0'),
('122','M','774','opma','0'),
('123','M','775','opma','0'),
('124','M','776','opma','0'),
('125','M','777','opma','0'),
('126','M','778','opma','0'),
('127','M','779','opma','0'),
('128','M','780','opma','0'),
('129','M','781','opma','0'),
('130','M','782','opma','0'),
('131','M','783','opma','0'),
('132','M','784','opma','0'),
('133','M','785','opma','0'),
('134','M','786','opma','0'),
('135','M','787','opma','0'),
('136','M','788','opma','0'),
('137','M','789','opma','0'),
('138','M','790','opma','0'),
('139','M','791','opma','0'),
('140','M','792','opma','0'),
('141','M','793','opma','0'),
('142','M','794','opma','0'),
('143','M','795','opma','0'),
('144','M','796','opma','0'),
('145','M','797','opma','0'),
('146','M','798','opma','0'),
('147','M','799','opma','0'),
('148','F','750','opma','0'),
('149','F','751','opma','0'),
('150','F','752','opma','0'),
('151','F','753','opma','0'),
('152','F','754','opma','0'),
('153','F','755','opma','0'),
('154','F','756','opma','0'),
('155','F','757','opma','0'),
('156','F','758','opma','0'),
('157','F','759','opma','0'),
('158','F','760','opma','0'),
('159','F','761','opma','0'),
('160','F','762','opma','0'),
('161','F','763','opma','0'),
('162','F','764','opma','0'),
('163','F','765','opma','0'),
('164','F','766','opma','0'),
('165','F','767','opma','0'),
('166','F','768','opma','0'),
('167','F','769','opma','0'),
('168','F','770','opma','0'),
('169','F','772','opma','0'),
('170','F','773','opma','0'),
('171','F','774','opma','0'),
('172','F','775','opma','0'),
('173','F','776','opma','0'),
('174','F','777','opma','0'),
('175','F','778','opma','0'),
('176','F','779','opma','0'),
('177','F','780','opma','0'),
('178','F','781','opma','0'),
('179','F','782','opma','0'),
('180','F','783','opma','0'),
('181','F','784','opma','0'),
('182','F','785','opma','0'),
('183','F','786','opma','0'),
('184','F','787','opma','0'),
('185','F','788','opma','0'),
('186','F','789','opma','0'),
('187','F','790','opma','0'),
('188','F','791','opma','0'),
('189','F','792','opma','0'),
('190','F','793','opma','0'),
('191','F','794','opma','0'),
('192','F','795','opma','0'),
('193','F','796','opma','0'),
('194','F','797','opma','0'),
('195','F','798','opma','0'),
('196','F','799','opma','0'),
('197','M','750','truv','0'),
('198','M','751','truv','0'),
('199','M','752','truv','0'),
('200','M','753','truv','0'),
('201','M','754','truv','0'),
('202','M','755','truv','0'),
('203','M','756','truv','0'),
('204','M','757','truv','0'),
('205','M','758','truv','0'),
('206','M','759','truv','0'),
('207','M','760','truv','0'),
('208','M','761','truv','0'),
('209','M','762','truv','0'),
('210','M','763','truv','0'),
('211','M','764','truv','0'),
('212','M','765','truv','0'),
('213','M','766','truv','0'),
('214','M','767','truv','0'),
('215','M','768','truv','0'),
('216','M','769','truv','0'),
('217','M','770','truv','0'),
('218','M','772','truv','0'),
('219','M','773','truv','0'),
('220','M','774','truv','0'),
('221','M','775','truv','0'),
('222','M','776','truv','0'),
('223','M','777','truv','0'),
('224','M','778','truv','0'),
('225','M','779','truv','0'),
('226','M','780','truv','0'),
('227','M','781','truv','0'),
('228','M','782','truv','0'),
('229','M','783','truv','0'),
('230','M','784','truv','0'),
('231','M','785','truv','0'),
('232','M','786','truv','0'),
('233','M','787','truv','0'),
('234','M','788','truv','0'),
('235','M','789','truv','0'),
('236','M','790','truv','0'),
('237','M','791','truv','0'),
('238','M','792','truv','0'),
('239','M','793','truv','0'),
('240','M','794','truv','0'),
('241','M','795','truv','0'),
('242','M','796','truv','0'),
('243','M','797','truv','0'),
('244','M','798','truv','0'),
('245','M','799','truv','0'),
('246','F','750','truv','0'),
('247','F','751','truv','0'),
('248','F','752','truv','0'),
('249','F','753','truv','0'),
('250','F','754','truv','0'),
('251','F','755','truv','0'),
('252','F','756','truv','0'),
('253','F','757','truv','0'),
('254','F','758','truv','0'),
('255','F','759','truv','0'),
('256','F','760','truv','0'),
('257','F','761','truv','0'),
('258','F','762','truv','0'),
('259','F','763','truv','0'),
('260','F','764','truv','0'),
('261','F','765','truv','0'),
('262','F','766','truv','0'),
('263','F','767','truv','0'),
('264','F','768','truv','0'),
('265','F','769','truv','0'),
('266','F','770','truv','0'),
('267','F','772','truv','0'),
('268','F','773','truv','0'),
('269','F','774','truv','0'),
('270','F','775','truv','0'),
('271','F','776','truv','0'),
('272','F','777','truv','0'),
('273','F','778','truv','0'),
('274','F','779','truv','0'),
('275','F','780','truv','0'),
('276','F','781','truv','0'),
('277','F','782','truv','0'),
('278','F','783','truv','0'),
('279','F','784','truv','0'),
('280','F','785','truv','0'),
('281','F','786','truv','0'),
('282','F','787','truv','0'),
('283','F','788','truv','0'),
('284','F','789','truv','0'),
('285','F','790','truv','0'),
('286','F','791','truv','0'),
('287','F','792','truv','0'),
('288','F','793','truv','0'),
('289','F','794','truv','0'),
('290','F','795','truv','0'),
('291','F','796','truv','0'),
('292','F','797','truv','0'),
('293','F','798','truv','0'),
('294','F','799','truv','0'),
('295','M','750','trma','0'),
('296','M','751','trma','0'),
('297','M','752','trma','0'),
('298','M','753','trma','0'),
('299','M','754','trma','0'),
('300','M','755','trma','0'),
('301','M','756','trma','0'),
('302','M','757','trma','0'),
('303','M','758','trma','0'),
('304','M','759','trma','0'),
('305','M','760','trma','0'),
('306','M','761','trma','0'),
('307','M','762','trma','0'),
('308','M','763','trma','0'),
('309','M','764','trma','0'),
('310','M','765','trma','0'),
('311','M','766','trma','0'),
('312','M','767','trma','0'),
('313','M','768','trma','0'),
('314','M','769','trma','0'),
('315','M','770','trma','0'),
('316','M','772','trma','0'),
('317','M','773','trma','0'),
('318','M','774','trma','0'),
('319','M','775','trma','0'),
('320','M','776','trma','0'),
('321','M','777','trma','0'),
('322','M','778','trma','0'),
('323','M','779','trma','0'),
('324','M','780','trma','0'),
('325','M','781','trma','0'),
('326','M','782','trma','0'),
('327','M','783','trma','0'),
('328','M','784','trma','0'),
('329','M','785','trma','0'),
('330','M','786','trma','0'),
('331','M','787','trma','0'),
('332','M','788','trma','0'),
('333','M','789','trma','0'),
('334','M','790','trma','0'),
('335','M','791','trma','0'),
('336','M','792','trma','0'),
('337','M','793','trma','0'),
('338','M','794','trma','0'),
('339','M','795','trma','0'),
('340','M','796','trma','0'),
('341','M','797','trma','0'),
('342','M','798','trma','0'),
('343','M','799','trma','0'),
('344','F','750','trma','0'),
('345','F','751','trma','0'),
('346','F','752','trma','0'),
('347','F','753','trma','0'),
('348','F','754','trma','0'),
('349','F','755','trma','0'),
('350','F','756','trma','0'),
('351','F','757','trma','0'),
('352','F','758','trma','0'),
('353','F','759','trma','0'),
('354','F','760','trma','0'),
('355','F','761','trma','0'),
('356','F','762','trma','0'),
('357','F','763','trma','0'),
('358','F','764','trma','0'),
('359','F','765','trma','0'),
('360','F','766','trma','0'),
('361','F','767','trma','0'),
('362','F','768','trma','0'),
('363','F','769','trma','0'),
('364','F','770','trma','0'),
('365','F','772','trma','0'),
('366','F','773','trma','0'),
('367','F','774','trma','0'),
('368','F','775','trma','0'),
('369','F','776','trma','0'),
('370','F','777','trma','0'),
('371','F','778','trma','0'),
('372','F','779','trma','0'),
('373','F','780','trma','0'),
('374','F','781','trma','0'),
('375','F','782','trma','0'),
('376','F','783','trma','0'),
('377','F','784','trma','0'),
('378','F','785','trma','0'),
('379','F','786','trma','0'),
('380','F','787','trma','0'),
('381','F','788','trma','0'),
('382','F','789','trma','0'),
('383','F','790','trma','0'),
('384','F','791','trma','0'),
('385','F','792','trma','0'),
('386','F','793','trma','0'),
('387','F','794','trma','0'),
('388','F','795','trma','0'),
('389','F','796','trma','0'),
('390','F','797','trma','0'),
('391','F','798','trma','0'),
('392','F','799','trma','0'),
('393','M','750','mdcd','0'),
('394','M','751','mdcd','0'),
('395','M','752','mdcd','0'),
('396','M','753','mdcd','0'),
('397','M','754','mdcd','0'),
('398','M','755','mdcd','0'),
('399','M','756','mdcd','0'),
('400','M','757','mdcd','0'),
('401','M','758','mdcd','0'),
('402','M','759','mdcd','0'),
('403','M','760','mdcd','0'),
('404','M','761','mdcd','0'),
('405','M','762','mdcd','0'),
('406','M','763','mdcd','0'),
('407','M','764','mdcd','0'),
('408','M','765','mdcd','0'),
('409','M','766','mdcd','0'),
('410','M','767','mdcd','0'),
('411','M','768','mdcd','0'),
('412','M','769','mdcd','0'),
('413','M','770','mdcd','0'),
('414','M','772','mdcd','0'),
('415','M','773','mdcd','0'),
('416','M','774','mdcd','0'),
('417','M','775','mdcd','0'),
('418','M','776','mdcd','0'),
('419','M','777','mdcd','0'),
('420','M','778','mdcd','0'),
('421','M','779','mdcd','0'),
('422','M','780','mdcd','0'),
('423','M','781','mdcd','0'),
('424','M','782','mdcd','0'),
('425','M','783','mdcd','0'),
('426','M','784','mdcd','0'),
('427','M','785','mdcd','0'),
('428','M','786','mdcd','0'),
('429','M','787','mdcd','0'),
('430','M','788','mdcd','0'),
('431','M','789','mdcd','0'),
('432','M','790','mdcd','0'),
('433','M','791','mdcd','0'),
('434','M','792','mdcd','0'),
('435','M','793','mdcd','0'),
('436','M','794','mdcd','0'),
('437','M','795','mdcd','0'),
('438','M','796','mdcd','0'),
('439','M','797','mdcd','0'),
('440','M','798','mdcd','0'),
('441','M','799','mdcd','0'),
('442','F','750','mdcd','0'),
('443','F','751','mdcd','0'),
('444','F','752','mdcd','0'),
('445','F','753','mdcd','0'),
('446','F','754','mdcd','0'),
('447','F','755','mdcd','0'),
('448','F','756','mdcd','0'),
('449','F','757','mdcd','0'),
('450','F','758','mdcd','0'),
('451','F','759','mdcd','0'),
('452','F','760','mdcd','0'),
('453','F','761','mdcd','0'),
('454','F','762','mdcd','0'),
('455','F','763','mdcd','0'),
('456','F','764','mdcd','0'),
('457','F','765','mdcd','0'),
('458','F','766','mdcd','0'),
('459','F','767','mdcd','0'),
('460','F','768','mdcd','0'),
('461','F','769','mdcd','0'),
('462','F','770','mdcd','0'),
('463','F','772','mdcd','0'),
('464','F','773','mdcd','0'),
('465','F','774','mdcd','0'),
('466','F','775','mdcd','0'),
('467','F','776','mdcd','0'),
('468','F','777','mdcd','0'),
('469','F','778','mdcd','0'),
('470','F','779','mdcd','0'),
('471','F','780','mdcd','0'),
('472','F','781','mdcd','0'),
('473','F','782','mdcd','0'),
('474','F','783','mdcd','0'),
('475','F','784','mdcd','0'),
('476','F','785','mdcd','0'),
('477','F','786','mdcd','0'),
('478','F','787','mdcd','0'),
('479','F','788','mdcd','0'),
('480','F','789','mdcd','0'),
('481','F','790','mdcd','0'),
('482','F','791','mdcd','0'),
('483','F','792','mdcd','0'),
('484','F','793','mdcd','0'),
('485','F','794','mdcd','0'),
('486','F','795','mdcd','0'),
('487','F','796','mdcd','0'),
('488','F','797','mdcd','0'),
('489','F','798','mdcd','0'),
('490','F','799','mdcd','0');


