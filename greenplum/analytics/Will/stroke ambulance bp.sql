
----get CE 
drop table if exists dev.wc_stroke_mems;

---get CE
select a.uth_member_id, member_id_src, year 
into dev.wc_stroke_mems
from data_warehouse.member_enrollment_yearly a
   join data_warehouse.dim_uth_member_id b 
     on a.uth_member_id = b.uth_member_id 
where a.data_source = 'mcrn'
  and year between 2014 and 2019
  and total_enrolled_months = 12
  and plan_type = 'AB'
 ;
  
 
 ---remove dual eligible
select a.*
into dev.wc_stroke_enrolled_mems
from dev.wc_stroke_mems a 
left outer join medicare_national.mbsf_abcd_summary b 
   on a.member_id_src = b.bene_id 
  and a.year = b.year::int2 
  and (   dual_stus_cd_01 in ('02','03','04','08')
	   or dual_stus_cd_02 in ('02','03','04','08')
	   or dual_stus_cd_03 in ('02','03','04','08')
	   or dual_stus_cd_04 in ('02','03','04','08')
	   or dual_stus_cd_05 in ('02','03','04','08')
	   or dual_stus_cd_06 in ('02','03','04','08')
	   or dual_stus_cd_07 in ('02','03','04','08')
	   or dual_stus_cd_08 in ('02','03','04','08')
	   or dual_stus_cd_09 in ('02','03','04','08')
	   or dual_stus_cd_10 in ('02','03','04','08')
	   or dual_stus_cd_11 in ('02','03','04','08')
	   or dual_stus_cd_12 in ('02','03','04','08') 
      )
where b.bene_id is null 
;



---load dx codes
drop table if exists dev.wc_stroke_diags;

create table dev.wc_stroke_diags (dx text);

insert into dev.wc_stroke_diags values ('430'),('431'),('432'),('4321'),('4329'),('433'),('43301'),('4331'),('43311'),('4332'),('43321'),('4333'),('43331'),('4338'),
('43381'),('4339'),('43391'),('434'),('43401'),('4341'),('43411'),('4349'),('43491'),('435'),('4351'),('4352'),('4353'),('4358'),('4359'),('436'),('I609'),('I619'),('I621'),
('I6200'),('I629'),('I651'),('I6322'),('I6529'),('I63139'),('I63239'),('I6509'),('I63019'),('I63119'),('I63219'),('I658'),('I6359'),('I658'),('I6359'),('I659'),('I6320'),
('I6609'),('I6619'),('I6629'),('I6330'),('I6609'),('I6619'),('I6629'),('I669'),('I6340'),('I669'),('I6350'),('G450'),('G450'),('G458'),('G450'),('G451'),
('G458'),('G459'),('I67848'),('I6789');





----find inpatient strokes
drop table if exists dev.wc_stroke_events;

select distinct a.bene_id, a.clm_from_dt::date 
into dev.wc_stroke_events
from medicare_national.inpatient_base_claims_k a
  join dev.wc_stroke_enrolled_mems b  
     on a.bene_id = b.member_id_src 
where a.prncpal_dgns_cd in ( select dx from dev.wc_stroke_diags)
  and a."year"::int2 >= 2015
   ;
   
insert into  dev.wc_stroke_events 
  select distinct a.bene_id, a.clm_from_dt::date 
  from medicare_national.outpatient_base_claims_k a
  join dev.wc_stroke_enrolled_mems b  
     on a.bene_id = b.member_id_src 
where a.prncpal_dgns_cd in ( select dx from dev.wc_stroke_diags)
  and a."year"::int2 >= 2015
   ;
  
  
  select distinct bene_id, clm_from_dt 
  into dev.wc_stroke_events_extract--count(*), count(distinct bene_id) 
  from dev.wc_stroke_events
  order by bene_id, clm_from_dt;
  
 ----create extract
 --mbsf
 with strk_cte as ( select distinct bene_id from dev.wc_stroke_events) 
 select a.*
 into dev.wc_stroke_mbsf_abcd_summary
 from medicare_national.mbsf_abcd_summary a 
    join strk_cte b 
     on a.bene_id = b.bene_id 
order by a.bene_id, a.year 
;

--bcarrier
 with strk_cte as ( select distinct bene_id from dev.wc_stroke_events) 
 select a.*
 into dev.wc_stroke_bcarrier_claims_k
 from medicare_national.bcarrier_claims_k a 
    join strk_cte b 
     on a.bene_id = b.bene_id 
order by a.bene_id, a.year 
;

--bcarrier line
 with strk_cte as ( select distinct bene_id from dev.wc_stroke_events) 
 select a.*
 into dev.wc_stroke_bcarrier_line_k
 from medicare_national.bcarrier_line_k a 
    join strk_cte b 
     on a.bene_id = b.bene_id 
order by a.bene_id, a.year 
;


---inpatient base 
 with strk_cte as ( select distinct bene_id from dev.wc_stroke_events) 
 select a.*
 into dev.wc_stroke_inpatient_base_claims
 from medicare_national.inpatient_base_claims_k a 
    join strk_cte b 
     on a.bene_id = b.bene_id 
order by a.bene_id, a.year 
;
 


---inpatient rev
 with strk_cte as ( select distinct bene_id from dev.wc_stroke_events) 
 select a.*
 into dev.wc_stroke_inpatient_revenue_center
 from medicare_national.inpatient_revenue_center_k a
    join strk_cte b 
     on a.bene_id = b.bene_id 
order by a.bene_id, a.year 
;

---outpatient base 
 with strk_cte as ( select distinct bene_id from dev.wc_stroke_events) 
 select a.*
 into dev.wc_stroke_outpatient_base_claims
 from medicare_national.outpatient_base_claims_k a 
    join strk_cte b 
     on a.bene_id = b.bene_id 
order by a.bene_id, a.year 
;
 


---outpatient rev
 with strk_cte as ( select distinct bene_id from dev.wc_stroke_events) 
 select a.*
 into dev.wc_stroke_outpatient_revenue_center
 from medicare_national.outpatient_revenue_center_k a
    join strk_cte b 
     on a.bene_id = b.bene_id 
order by a.bene_id, a.year 
;
