---get first HF date
drop table if exists dev.wc_hf_diags;


select bene_id, min(clm_from_dt::date) as hf_date 
into dev.wc_hf_diags
from medicare_texas.inpatient_base_claims_k 
where year = '2018'
and prncpal_dgns_cd in ('428','4281','4282','42821','42822','42823','4283','42831','42832','42833','4284','4284',
	'42841','42842','42843','4289','39891','40291','40211','40201','40401','40403','40411','40413',
	'40491','40493','I50','I5020','I5021','I5022','I5023','I503','I5031','I5032','I5033','I504',
	'I5041','I5042','I5043','I509','I500','I501','I5081','I50811','I50812','I50813','I50814','I5082',
	'I5083','I5084','I5089','I0981','I110','I130','I132')
group by bene_id
;

select *
into dev.wc_hf_first_hf_event
from dev.wc_hf_diags a where bene_id in ( select bene_id from dev.wc_hf_cohorts);


select count(*), count(distinct bene_id) 
from dev.wc_hf_first_hf_event;

---get 65+ cohort
select a.* 
into dev.wc_hf_cohorts
from medicare_texas.mbsf_abcd_summary a 
   join dev.wc_hf_diags b 
     on a.bene_id = b.bene_id
where a.year = '2018' 
and a.age_at_end_ref_yr::int2 >= 65
 ; 

select count(*), count(distinct bene_id) 
from dev.wc_hf_cohorts;


select count(*), year from medicare_texas.pde_file pf 
group by year
order by year;

----build extract tables 

---enrollment
select a.* 
into dev.wc_hf_mbsf_abcd_summary
from medicare_texas.mbsf_abcd_summary a 
   join dev.wc_hf_cohorts b  
     on a.bene_id = b.bene_id 
where a.year between '2015' and '2019'
;


---bcarrier claims
select a.* 
into dev.wc_hf_bcarrier_claims_k
from medicare_texas.bcarrier_claims_k a 
   join dev.wc_hf_cohorts b  
     on a.bene_id = b.bene_id 
where a.year between '2015' and '2019'
;

---bcarrier line
select a.* 
into dev.wc_hf_bcarrier_line_k
from medicare_texas.bcarrier_line_k a 
   join dev.wc_hf_cohorts b  
     on a.bene_id = b.bene_id 
where a.year between '2015' and '2019'
;

---dme claims
select a.* 
into dev.wc_hf_dme_claims_k
from medicare_texas.dme_claims_k a 
   join dev.wc_hf_cohorts b  
     on a.bene_id = b.bene_id 
where a.year between '2015' and '2019'
;

---dme line
select a.* 
into dev.wc_hf_dme_line_k
from medicare_texas.dme_line_k a
   join dev.wc_hf_cohorts b  
     on a.bene_id = b.bene_id 
where a.year between '2015' and '2019'
;

--hha base
select a.* 
into dev.wc_hf_hha_base_claims_k
from medicare_texas.hha_base_claims_k a
   join dev.wc_hf_cohorts b  
     on a.bene_id = b.bene_id 
where a.year between '2015' and '2019'
;


--hha rev center
select a.* 
into dev.wc_hf_hha_revenue_center_k
from medicare_texas.hha_revenue_center_k a
   join dev.wc_hf_cohorts b  
     on a.bene_id = b.bene_id 
where a.year between '2015' and '2019'
;

-- hopsice base
select a.* 
into dev.wc_hf_hospice_base_claims_k
from medicare_texas.hospice_base_claims_k a 
   join dev.wc_hf_cohorts b  
     on a.bene_id = b.bene_id 
where a.year between '2015' and '2019'
;

--hospice rev center
select a.* 
into dev.wc_hf_hospice_revenue_center_k
from medicare_texas.hospice_revenue_center_k a
   join dev.wc_hf_cohorts b  
     on a.bene_id = b.bene_id 
where a.year between '2015' and '2019'
;

--inpatient base
select a.* 
into dev.wc_hf_inpatient_base_claims_k
from medicare_texas.inpatient_base_claims_k a 
   join dev.wc_hf_cohorts b  
     on a.bene_id = b.bene_id 
where a.year between '2015' and '2019'
;

--inpatient rev center
select a.* 
into dev.wc_hf_inpatient_revenue_center_k
from medicare_texas.inpatient_revenue_center_k a
   join dev.wc_hf_cohorts b  
     on a.bene_id = b.bene_id 
where a.year between '2015' and '2019'
;


---outpatient base
select a.* 
into dev.wc_hf_outpatient_base_claims_k
from medicare_texas.outpatient_base_claims_k a 
   join dev.wc_hf_cohorts b  
     on a.bene_id = b.bene_id 
where a.year between '2015' and '2019'
;

---outpatient rev center
select a.* 
into dev.wc_hf_outpatient_revenue_center_k
from medicare_texas.outpatient_revenue_center_k a
   join dev.wc_hf_cohorts b  
     on a.bene_id = b.bene_id 
where a.year between '2015' and '2019'
;

--pde file
select a.* 
into dev.wc_hf_pde_file
from medicare_texas.pde_file a
   join dev.wc_hf_cohorts b  
     on a.bene_id = b.bene_id 
where a.year between '2015' and '2019'
;

---snf base
select a.* 
into dev.wc_hf_snf_base_claims_k
from medicare_texas.snf_base_claims_k a 
   join dev.wc_hf_cohorts b  
     on a.bene_id = b.bene_id 
where a.year between '2015' and '2019'
;

--snf rev center
select a.* 
into dev.wc_hf_snf_revenue_center_k
from medicare_texas.snf_revenue_center_k a
   join dev.wc_hf_cohorts b  
     on a.bene_id = b.bene_id 
where a.year between '2015' and '2019'
;