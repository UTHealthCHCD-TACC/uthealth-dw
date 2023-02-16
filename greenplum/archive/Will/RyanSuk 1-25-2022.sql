J40%, J41%, J42%, J43%, J44%
F00%, F01%, F02%, F03%, F04%, F051%, F065%, F066%, 

F068%, F069%, F09%, F107%, F117%, F127%, F137%, F147%, F157%, F167%, F177%, F187%, F197%, G30%, G310%, G311%, G319%, G328%, G91%, G937%, G94%, R54%
J84%

C00% ~ C96%
N17% ~ N19%, N25%
I50%


insert into dev.wc_suk_main_diags
select *
from reference_tables.ref_cms_icd_cm_codes 
where cd_value like 'I50%'
;


select uth_member_id , from_date_of_service , diag_cd 
into dev.wc_suk_main_diag_claims
from data_warehouse.claim_diag cd 
  join dev.wc_suk_main_diags wc 
    on wc.cd_value = cd.diag_cd 
where extract(year from from_date_of_service) between 2016 and 2020
  and data_source = 'optd'
;

select m.uth_member_id, u.member_id_src, inr.index_date, m.age_derived, m.death_date 
into dev.wc_suk_main
from (
select a.uth_member_id, min(from_date_of_service) as index_date 
from dev.wc_suk_main_diag_claims a 
group by a.uth_member_id
) inr 
   join data_warehouse.member_enrollment_yearly m 
     on m.uth_member_id = inr.uth_member_id 
    and m.year = extract(year from inr.index_date)
    and m.age_derived >= 18
   join data_warehouse.dim_uth_member_id u 
     on u.uth_member_id = m.uth_member_id 
     and u.data_source = 'optd'
;


select count(*), count(distinct uth_member_id) 
from dev.wc_suk_main
;


----metastatic cancer
where cd_value in 
('C7880', 'C7889', 'C7900', 'C7901', 'C7902', 'C7910', 'C7911', 'C7919', 'C792%', 'C7931', 'C7932', 'C7940', 'C7949', 'C7951', 'C7952', 
'C7960', 'C7961', 'C7962', 'C7970', 'C7971', 'C7972', 'C7981', 'C7982', 'C7989', 'C799%', 'C7B00', 'C7B01', 'C7B02', 'C7B03', 'C7B04', 'C7B09')

C770%, C771%, C772%, C773%, C774%, C775%, C778%,  C779%, C7800%, C7801%, C7802%, C781%,    
C782%, C7830, C7839, C784%, C785%, C786%, C787%, C7B1%, C7B8%, C800%, C801%, R180%

insert into dev.wc_suk_meta_diags
select * 
from reference_tables.ref_cms_icd_cm_codes 
where cd_value like 'R180%';

select uth_member_id , from_date_of_service , diag_cd 
into dev.wc_suk_meta_diag_claims
from data_warehouse.claim_diag cd 
  join dev.wc_suk_meta_diags wc 
    on wc.cd_value = cd.diag_cd 
where extract(year from from_date_of_service) between 2016 and 2020
  and data_source = 'optd'
;

select a.uth_member_id, min(from_date_of_service) as meta_date 
into dev.wc_suk_meta_pats
from dev.wc_suk_meta_diag_claims a
group by uth_member_id
;


select member_id_src as patid, index_date, age_derived as age_at_index_date, 
       to_char(death_date,'YYYY-MM') as death_my, 
       case when ( death_date - index_date ) / 365 <= 2 then true else false end as death_boolean,
       b.meta_date as metastatic_date,
       case when b.uth_member_id is null then false else true end as metastatic_boolean
   into dev.wc_suk_cohort_extract
from dev.wc_suk_main a 
  left outer join dev.wc_suk_meta_pats b 
     on a.uth_member_id = b.uth_member_id 

alter table dev.wc_suk_main add column death_boolean boolean;

alter table dev.wc_suk_main add column metastatic_boolean boolean;

alter table dev.wc_suk_main add column meta_first_date date;



select count(*)
from dev.wc_suk_cohort_extract wsce 


---file received back from caroline, load in csv 
create table dev.wc_suk_cohort_hf ( patid text, index_date text, age_at_index_date text, death_my text, death_boolean text, 
                                    metastatic_date text, metastatic_boolean text, hf_score text, hf_boolean text )
                                    ;

                                   
select * 
into dev.wc_suk_cohort_final 
from dev.wc_suk_cohort_hf          
where death_boolean = 'True' 
  or metastatic_boolean = 'True' 
  or hf_boolean = 'True'
;

---begin actual extract here
select * 
from dev.wc_suk_cohort_final 
;


---enrollment
select patid, pat_planid, cdhp, eligeff, eligend, gdr_cd, health_exch, lis_dual, race, state, yrdob
into dev.wc_suk_extract_enrollment
from optum_dod.mbr_enroll_r 
where patid::text in ( select patid from dev.wc_suk_cohort_final) 
  and eligend >= '2016-01-01' 
;

select count(*) from dev.wc_suk_extract_enrollment

--medical
select patid, pat_planid, admit_chan, admit_type, charge, clmid, clmseq, 
	   cob, coins, conf_id, copay, deduct, drg, dstatus, enctr, fst_dt, hccc, 
	   icd_flag, loc_cd, lst_dt, ndc, pos, proc_cd, procmod, prov, provcat, 
	   std_cost, std_cost_yr, tos_cd, units, ndc_uom, ndc_qty, tos_ext
into dev.wc_suk_extract_medical
from optum_dod.medical m 
where patid::text in ( select patid from dev.wc_suk_cohort_final) 
  and extract(year from m.fst_dt) between 2016 and 2020 
;  

--diagnosis
select patid, pat_planid, clmid, diag, diag_position, icd_flag, loc_cd, poa, fst_dt
into dev.wc_suk_extract_diagnosis
from optum_dod.diagnostic m 
where patid::text in ( select patid from dev.wc_suk_cohort_final) 
  and extract(year from m.fst_dt) between 2016 and 2020 
;  

--procedure
select patid, pat_planid, clmid, icd_flag, proc, proc_position, fst_dt
into dev.wc_suk_extract_procedure
from optum_dod."procedure" m
where patid::text in ( select patid from dev.wc_suk_cohort_final) 
  and extract(year from m.fst_dt) between 2016 and 2020 
;  

--confinement
select patid, pat_planid, admit_date, charge, coins, conf_id, copay, deduct, diag1,
       diag2, diag3, diag4, diag5, 
       disch_date, drg, dstatus, icd_flag, los, pos, proc1, proc2, proc3, proc4, proc5, 
       prov, std_cost, std_cost_yr, tos_cd, icu_ind, icu_surg_ind, maj_surg_ind, tos
into dev.wc_suk_extract_confinement
from optum_dod.confinement m 
where patid::text in ( select patid from dev.wc_suk_cohort_final) 
  and extract(year from m.admit_date) between 2016 and 2020 
;  

--rx 
select patid, pat_planid, ahfsclss, avgwhlsl, brnd_nm, charge, clmid, 
       copay, daw, days_sup, deduct, dispfee, fill_dt, form_typ, fst_fill, 
       gnrc_ind, gnrc_nm, ndc, prc_typ, quantity, rfl_nbr, spclt_ind, std_cost, std_cost_yr
into dev.wc_suk_extract_rx
from optum_dod.rx 
where patid::text in ( select patid from dev.wc_suk_cohort_final) 
  and extract(year from fill_dt) between 2016 and 2020 
;  

--provider xxx
select p.prov_unique, p.bed_sz_range, p.prov_state, p.prov_type, p.provcat, p.taxonomy1, p.taxonomy2
into dev.wc_suk_extract_provider
from optum_dod.provider p 
   join optum_dod.provider_bridge b 
     on b.prov_unique  = p.prov_unique
   join dev.wc_suk_extract_medical m 
     on m.prov = b.prov 
;  

--date of death 
select patid, death_ym 
into dev.wc_suk_extract_dateofdeath
from optum_dod.mbrwdeath 
where patid::text in ( select patid from dev.wc_suk_cohort_final) 
;  