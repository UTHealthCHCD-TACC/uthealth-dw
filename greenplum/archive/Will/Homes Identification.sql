--*****************************************************
--*   Homes TLFB Final
--*****************************************************


select distinct record_id , participantid 
from homes.raw.HOMES_TLFB_Final_20220809 htf 
where redcap_repeat_instrument = ''
;

select * 
from homes.stage.homes_phi 
where id_verify = '1-1-173-TD'
;

--*****************************************************
--*   GPRA New Final
--*****************************************************

--462 records 
--blank, baseline, discharge, m_follow_up
--242 unique participants
select distinct dem4 as birth_date, record_id, pid
from homes.raw.GPRANewFinal_20220809 gf 
where redcap_repeat_instrument = 'baseline'
;



--*****************************************************
--*   Homes New Final
--*****************************************************

--277 unique participants
select distinct cif_001 , cif_002, cif_003, ssn, cif_007 as birth_date
     , b1 as sex_at_birth -- 1: Female, 0: Male 
     , b2 as gender
     , id 
     , id_verify 
     , redcap_event_name 
from homes.raw.HomesNewFinal_1
--where redcap_event_name = 'contact_arm_1'
;

--273 id_verify 
insert into homes.stage.id_crosswalk ( study_id, hnf_redcap_id)
select distinct id_verify, id
from homes.raw.HomesNewFinal_1 
where id_verify <> ''
;


create table homes.stage.id_crosswalk ( study_id varchar(50), hnf_redcap_id varchar(20), gpra_redcap_id varchar(20) );


update homes.stage.id_crosswalk set gpra_redcap_id = g.record_id 
from homes.raw.GPRANewFinal_20220809 g 
  join homes.stage.id_crosswalk x 
    on x.study_id = g.pid 
where redcap_repeat_instrument = 'baseline'


select * from homes.stage.id_crosswalk 
;


select dem4 as birth_date, record_id, pid, id,  *
from homes.raw.GPRANewFinal_20220809 gf 
where record_id = '61' 
;

select cif_001 , cif_002, cif_003, ssn, cif_007 as birth_date
     , b1 as sex_at_birth -- 1: Female, 0: Male 
     , b2 as gender
     , id as redcap_id 
     , id_verify 
     , redcap_event_name 
from homes.raw.HomesNewFinal_1
--where  id = '76'


--HNF id = "id", "id_verify"
--GPRA id = "record_id", "pid"
--TLFB id = "participantid", "participant_id_complete"


---- FORM STATUS

--0 = incomplete, 2 = complete, 1 = unverified 
select distinct baseline_complete
from homes.raw.GPRANewFinal_20220809 gf 
where redcap_repeat_instrument = 'baseline'
;

select distinct redcap_repeat_instrument,  discharge_complete, m_followup_complete, m_follow_up_complete 
from homes.raw.GPRANewFinal_20220809 gf 
where redcap_repeat_instrument <> 'baseline' 
order by redcap_repeat_instrument 
;


select distinct participantid , participant_id_complete 
from homes.raw.HOMES_TLFB_Final_20220809 htf 


select count(*)  ,participant_id_complete  , redcap_repeat_instrument 
from homes.raw.HomesNewFinal_1 hnf 
where redcap_repeat_instrument = ''
group by participant_id_complete  , redcap_repeat_instrument 
;




---- HOMES NEW FINAL 
--go to baseline_arm_1 to get id, id_verify, sex_at_birth, and gender 
--go to contact_arm_1 to get first, middle, last, ssn, date of birth 

