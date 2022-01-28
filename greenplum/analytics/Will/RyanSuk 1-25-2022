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

