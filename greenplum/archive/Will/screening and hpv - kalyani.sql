


select count(*), year 
from data_warehouse.member_enrollment_yearly 
where data_source = 'optz' 
 and state = 'TX' 
 and age_derived between 21 and 65 
 and gender_cd = 'F' 
 and year between 2015 and 2020 
group by year order by year 
;

drop table if exists dev.wc_cc_screen_optz;

select a.uth_claim_id::text, a.uth_member_id, year , month_year_id 
into dev.wc_cc_screen_optz 
from data_warehouse.claim_detail a 
where cpt_hcpcs in ( '88141','88142','88143', '88147', '88148', '88150','88152','88153','88154', '88164','88165','88166','88167', '88174', '88175')
  and data_source = 'optz' 
  and year between 2015 and 2020 
  ;
  
 insert into  dev.wc_cc_screen_optz 
 select a.labclmid, b.uth_member_id,extract(year from a.fst_dt) as yr, get_my_from_date(a.fst_dt) as month_year_id
 from optum_zip.lab_result a
    join data_warehouse.dim_uth_member_id b 
       on a.patid::text = b.member_id_src 
 where loinc_cd in ('10524-7', '18500-9', '19762-4', '19764-0', '19765-7', '19766-5', '19774-9', '33717-0', '47527-7', '47528-5')
 and extract(year from a.fst_dt) between 2015 and 2020 
 ;


 select count(distinct uth_claim_id), month_year_id 
 from dev.wc_cc_screen_optz a  
   join data_warehouse.member_enrollment_yearly b
	on b.data_source = 'optz' 
	 and b.year = a.year 
	 and a.uth_member_id = b.uth_member_id 
	  and state = 'TX' 
	 and age_derived between 21 and 65 
	 and gender_cd = 'F' 
 group by month_year_id
 order by month_year_id
 ;
 

----------------hpv vacc

select count(*), year 
from data_warehouse.member_enrollment_yearly 
where data_source = 'optz' 
 and state = 'TX' 
 and age_derived between 9 and 26 
 and year between 2015 and 2020 
group by year order by year 
;

drop table if exists dev.wc_cc_screen_optz;

select a.uth_claim_id::text, a.uth_member_id, year , month_year_id 
into dev.wc_cc_hpv_optz 
from data_warehouse.claim_detail a 
where cpt_hcpcs in ( '90649','90651','90650')
  and data_source = 'optz' 
  and year between 2015 and 2020 
  ;


 select count(distinct uth_claim_id), month_year_id 
 from dev.wc_cc_screen_optz a  
   join data_warehouse.member_enrollment_yearly b
	on b.data_source = 'optz' 
	 and b.year = a.year 
	 and a.uth_member_id = b.uth_member_id 
	  and state = 'TX' 
	 and age_derived between 9 and 26 
 group by month_year_id
 order by month_year_id
 ;