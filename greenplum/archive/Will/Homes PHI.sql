

drop table homes.stage.Homes_PHI;

select id, max(cif_001) as first_name, max(cif_002) as middle_name, max(cif_003) as last_name, max(ssn_final) as ssn, max(cif_007) as dob
into homes.stage.Homes_PHI
from ( 
select   id
     , case when cif_001 = '' then cif_001_rh else cif_001 end as cif_001
     , case when cif_002 = '' then cif_002_rh else cif_002 end as cif_002 
     , case when cif_003 = '' then cif_003_rh else cif_003 end as cif_003 
     , case when cif_007 = '' then cif_007_rh else cif_007 end as cif_007
     , b1 as sex_at_birth -- 1: Female, 0: Male 
     , b2 as gender
     ,case when ssn_rh = '' then left(ssn,3) + '-' + right(left(ssn,5),2) + '-' + right(left(ssn,9),4) 
                            else left(ssn_rh ,3) + '-' + right(left(ssn_rh,5),2) + '-' + right(left(ssn_rh,9),4)
      end as ssn_final
from homes.HomesNewFinalArms.contact_arm_1 ca  
) x
group by id ;




alter table homes.stage.homes_phi add id_verify varchar(25);


update homes.stage.homes_phi set id_verify = b.id_verify 
from homes.HomesNewFinalArms.baseline_arm_1 b 
where b.id = homes.stage.homes_phi.id 
;

select count(*) 
from homes.stage.homes_phi 
where len(ssn) = 11
--order by cast(id as int)
;

select count(*)
from homes.raw.HOMES_TLFB_Final_20220809 
where participantid =''
--where participantid not in ( select id_verify from homes.stage.homes_phi) 
;

select rrentry , rrentry_rh , currentdate , date  
from Homes.raw.HomesNewFinal_1 hnf 
where redcap_event_name = 'contact_arm_1'


--------------------------------------------------------------
select distinct redcap_event_name 
from homes.raw.HomesNewFinal_1 hnf 
;


create schema HomesNewFinalArms;



select * 
into homes.HomesNewFinalArms.baseline_arm_1
from homes.raw.HomesNewFinal_1 
where redcap_event_name = 'baseline_arm_1'
;

select * 
into homes.HomesNewFinalArms.contact_arm_1
from homes.raw.HomesNewFinal_1 
where redcap_event_name = 'contact_arm_1'
;

select * 
into homes.HomesNewFinalArms.biological_arm_1
from homes.raw.HomesNewFinal_1 
where redcap_event_name = 'biological_arm_1'
;

select * 
into homes.HomesNewFinalArms.for_rural_homes_arm_1
from homes.raw.HomesNewFinal_1 
where redcap_event_name = 'for_rural_homes_arm_1'
;

select * 
into homes.HomesNewFinalArms.follow_up_arm_1
from homes.raw.HomesNewFinal_1 
where redcap_event_name = 'follow_up_arm_1'
;

select * 
into homes.HomesNewFinalArms.dischargereentry_arm_1
from homes.raw.HomesNewFinal_1 
where redcap_event_name = 'dischargereentry_arm_1'
;







