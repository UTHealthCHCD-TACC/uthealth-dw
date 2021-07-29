----2018 and 2019 ambulance claims for TEXAS
drop table dev.wc_ambulance_clm_detail;


select count(distinct clm_id), "year" , 
	case when hcpcs_cd in ('A0430','A0431') then 'A' else 'G' end as air_ground
from medicare_texas.bcarrier_line_k k 
where k.hcpcs_cd in ('A0225','A0422','A0424','A0425','A0426','A0427','A0428','A0429','A0430','A0431',
	                     'A0432','A0433','A0434','A0435','A0436','A0998','A0999','T2003','T2003') 
  and k.year::int2 between 2018 and 2019 
group by year , case when hcpcs_cd in ('A0430','A0431') then 'A' else 'G' end
order by year , case when hcpcs_cd in ('A0430','A0431') then 'A' else 'G' end
;



select * 
into dev.wc_ambulance_clm_detail
from (
	select a.uth_member_id , a.uth_claim_id , a.data_source , a."year" , 
	case when procedure_cd in ('A0430','A0431') then 'A' else 'G' end as air_ground, 
	b.bus_cd,
	case when a.network_ind is true then '1' else '0' end as net_ind , a.claim_id_src , a.member_id_src 
	from data_warehouse.claim_detail a 
		join data_warehouse.member_enrollment_yearly b  
		 on a.uth_member_id = b.uth_member_id 
	     and a.year = b.year 
	     and b.bus_cd in ('MDCR','MCR')
	     and b.state = 'TX'
	where a.data_source in ('truv','mcrt')
	and a.year between 2018 and 2019 
	and procedure_cd in ('A0225','A0422','A0424','A0425','A0426','A0427','A0428','A0429','A0430','A0431',
	                     'A0432','A0433','A0434','A0435','A0436','A0998','A0999','T2003','T2003') 
union 
	select a.uth_member_id , a.uth_claim_id , a.data_source , a."year" , 
	case when procedure_cd in ('A0430','A0431') then 'A' else 'G' end as air_ground, 
	b.bus_cd ,
	case when a.network_ind is true then '1' else '0' end as net_ind , a.claim_id_src , a.member_id_src 
	from data_warehouse.claim_detail a 
	    join data_warehouse.member_enrollment_yearly b  
	      on a.uth_member_id = b.uth_member_id 
	     and a.year = b.year 
	     and b.bus_cd = 'COM'
	     and b.state = 'TX'
	where a.data_source in ('truv','optz') 
	and a.year between 2018 and 2019 
	and procedure_cd in ('A0225','A0422','A0424','A0425','A0426','A0427','A0428','A0429','A0430','A0431',
	                     'A0432','A0433','A0434','A0435','A0436','A0998','A0999','T2003','T2003') 
)  inr;
                     

--get 1 claim id and air ground flag
drop table dev.wc_ambulance_claims;

select uth_member_id , uth_claim_id , data_source , "year", bus_cd, max(net_ind) as net_ind, min(air_ground) as ag_flag , claim_id_src , member_id_src 
into dev.wc_ambulance_claims
from dev.wc_ambulance_clm_detail 
group by uth_member_id , uth_claim_id , data_source , "year",bus_cd , claim_id_src , member_id_src 
;


select count(*), count(distinct uth_claim_id), year  
from dev.wc_ambulance_claims
group by year;


with upd as (
select max(prov_par) as prov_par , clmid, patid, year 
from optum_dod.medical m 
where m.year > 2016
 group by clmid, patid, year 
) 
update dev.wc_ambulance_claims a set net_ind = case when prov_par in ('C','P','T') then '1' else '0' end
from upd 
 join data_warehouse.dim_uth_claim_id c 
    on upd.patid::text = c.member_id_src 
   and upd.clmid = c.claim_id_src 
   and upd.year = c.data_year 
where a.uth_claim_id = c.uth_claim_id
;
 
drop table dev.wc_amb_cleanup;

--only keep professional claims
select distinct a.uth_claim_id 
into dev.wc_amb_cleanup
from data_warehouse.claim_header a
   join  dev.wc_ambulance_claims  b 
      on b.uth_claim_id = a.uth_claim_id 
where a.claim_type <> 'P'

delete from dev.wc_ambulance_claims where uth_claim_id in ( select uth_claim_id from dev.wc_amb_cleanup);


--analytics
select a.data_source , a."year" , b.bus_cd, b.ag_flag, net_ind,
       count(*) as total_records,
       count(distinct a.uth_claim_id) as clms, 
       count(distinct a.uth_member_id) as mems, 
       sum(a.total_charge_amount) as total_charge,
       sum(a.total_allowed_amount) as total_allowed 
--into dev.wc_ambulance_clm_hdr 
from data_warehouse.claim_header a
   join  dev.wc_ambulance_claims  b 
      on b.uth_claim_id = a.uth_claim_id 
     and b.data_source = 'truv' and b.bus_cd = 'COM'
group by a.data_source , a.year , b.bus_cd, b.ag_flag, net_ind
order by year, net_ind asc, ag_flag desc 
;


select a.* --count(*), table_id_src 
from data_warehouse.claim_detail a
   join  dev.wc_ambulance_claims  b 
      on b.uth_claim_id = a.uth_claim_id 
    and b.data_source = 'mcrt'
    


---mileage for medicare 
select a.data_source , a."year" , b.bus_cd, b.ag_flag, net_ind,
       sum(a.units) as total_miles, sum(a.allowed_amount) as line_allowed
from data_warehouse.claim_detail a 
     join  dev.wc_ambulance_claims  b 
      on b.uth_claim_id = a.uth_claim_id 
where a.procedure_cd in ('A0425','A0435','A0436')
 and b.data_source = 'mcrt'
group by a.data_source , a.year , b.bus_cd, b.ag_flag, net_ind
order by a.data_source , a.year , b.bus_cd, b.ag_flag, net_ind

--optz miles
select  b."year" , b.bus_cd, b.ag_flag, net_ind,
       sum(a.units) as units, sum(a.alt_units::int2) as alt_units, count(distinct b.uth_claim_id) as clms 
from optum_dod.medical a
     join  dev.wc_ambulance_claims  b 
      on b.member_id_src = a.patid::text 
     and b.claim_id_src = a.clmid 
     and b.year = extract(year from a.fst_dt)
where a.proc_cd in ('A0425','A0435','A0436')
 and b.data_source = 'optz'
group by b.year , b.bus_cd, b.ag_flag, net_ind
order by b.year , b.bus_cd, b.ag_flag, net_ind



select * from dev.wc_ambulance_claims a where data_source = 'truv' and bus_cd = 'COM';

select * from truven.ccaeo where enrolid::text = '4197741001' and msclmid::text = '2551515'


select * from truven.ccaes where proc1 = 'A0425';

--truven commercial, miles
select inr."year" , inr.ag_flag, inr.net_ind,
       sum(inr.units) as units, sum(inr.clms ) as claims
from ( 
select  b."year" , b.ag_flag, net_ind,
       sum(a.units) as units, count(distinct b.uth_claim_id) as clms
from truven.ccaeo a 
     join  dev.wc_ambulance_claims  b 
      on b.member_id_src = a.enrolid::text
     and b.claim_id_src = a.msclmid::text 
     and b.year = extract(year from a.svcdate )
where a.proc1 in ('A0425','A0435','A0436')
 and b.data_source = 'truv' 
 and b.bus_cd = 'COM'
group by b.year , b.ag_flag, net_ind
union 
select  b."year" , b.ag_flag, net_ind,
       sum(a.units) as units, count(distinct b.uth_claim_id) as clms
from truven.ccaes a 
     join  dev.wc_ambulance_claims  b 
      on b.member_id_src = a.enrolid::text
     and b.claim_id_src = a.msclmid::text 
     and b.year = extract(year from a.svcdate )
where a.proc1 in ('A0425','A0435','A0436')
 and b.data_source = 'truv' 
 and b.bus_cd = 'COM'
group by b.year , b.ag_flag, net_ind
) inr 
group by year, ag_flag, net_ind 
order by year, net_ind asc, ag_flag desc 
;


--truven ms, miles
select inr."year" , inr.ag_flag, inr.net_ind,
       sum(inr.units) as units, sum(inr.clms ) as claims
from ( 
select  b."year" , b.ag_flag, net_ind,
       sum(a.units) as units, count(distinct b.uth_claim_id) as clms
from truven.mdcro a 
     join  dev.wc_ambulance_claims  b 
      on b.member_id_src = a.enrolid::text
     and b.claim_id_src = a.msclmid::text 
     and b.year = extract(year from a.svcdate )
where a.proc1 in ('A0425','A0435','A0436')
 and b.data_source = 'truv' 
 and b.bus_cd = 'MCR'
group by b.year , b.ag_flag, net_ind
union 
select  b."year" , b.ag_flag, net_ind,
       sum(a.units) as units, count(distinct b.uth_claim_id) as clms
from truven.mdcrs a 
     join  dev.wc_ambulance_claims  b 
      on b.member_id_src = a.enrolid::text
     and b.claim_id_src = a.msclmid::text 
     and b.year = extract(year from a.svcdate )
where a.proc1 in ('A0425','A0435','A0436')
 and b.data_source = 'truv' 
 and b.bus_cd = 'MCR'
group by b.year , b.ag_flag, net_ind
) inr 
group by year, ag_flag, net_ind 
order by year, net_ind asc, ag_flag desc 
;



---truven OOP
select a.data_source , a."year" , b.bus_cd, b.ag_flag, net_ind,
       sum(a.units) as total_miles, sum(a.allowed_amount) as line_allowed, 
       sum(a.copay) as copay, sum(a.deductible ) as deduct, sum(a.coins ) as coins, 
       sum(a.deductible + copay + coins ) as OOP 
from data_warehouse.claim_detail a 
     join  dev.wc_ambulance_claims  b 
      on b.uth_claim_id = a.uth_claim_id 
     and b.data_source = 'truv' and bus_cd = 'MCR'
group by a.data_source , a.year , b.bus_cd, b.ag_flag, net_ind
order by year, net_ind asc, ag_flag desc 


select * from optum_dod.medical m 

---optum OOP
select  b."year" , b.bus_cd, b.ag_flag, net_ind,
        sum(a.alt_units::int2) as alt_units, count(distinct b.uth_claim_id) as clms,
        sum(a.copay) as copay, sum(a.deduct) as deduct, sum(a.coins ) as coins, 
        sum (copay + deduct + coins) as OOP
from optum_dod.medical a
     join  dev.wc_ambulance_claims  b 
      on b.member_id_src = a.patid::text 
     and b.claim_id_src = a.clmid 
     and b.year = extract(year from a.fst_dt)
where  b.data_source = 'optz'
group by b.year , b.bus_cd, b.ag_flag, net_ind
order by b.year, net_ind asc, ag_flag desc 


---medicare OOP
select a.data_source , a."year" , b.bus_cd, b.ag_flag, net_ind,
       sum(a.units) as total_miles, sum(a.allowed_amount) as line_allowed, sum(deductible ) as ded, sum(copay ) as cop, sum(coins) as coins,
       sum(a.deductible + coins ) as OOP 
from data_warehouse.claim_detail a 
     join  dev.wc_ambulance_claims  b 
      on b.uth_claim_id = a.uth_claim_id 
     and b.data_source = 'mcrt'-- and bus_cd = 'COM'
group by a.data_source , a.year , b.bus_cd, b.ag_flag, net_ind
order by year, net_ind asc, ag_flag desc 

vacuum analyze data_warehouse.claim_detail;



----************************plan type oop and allowed************************************
--truven PPO = PPO, POS, EPO
--optum use cdhp flag in mbr_enroll if 1 or 2 then CHHP else PPO 

---optum 
select * from dev.wc_ambulance_claims a where data_source = 'optz';

alter table dev.wc_ambulance_claims add column cdhp_flag bool;




with cdhp_upd as 
(
select c.uth_member_id , c."year" , max(b.cdhp) as cdhp 
from optum_dod.mbr_enroll b 
   join dev.wc_ambulance_claims c 
     on b.patid::text = c.member_id_src 
    and c.year between extract(year from b.eligeff) and extract(year from b.eligend) 
  group by c.uth_member_id , c."year"
)
 update dev.wc_ambulance_claims a set cdhp_flag = case when x.cdhp in ('1','2') then true else false end
 from cdhp_upd x 
 where a.uth_member_id = x.uth_member_id 
   and a."year" = x.year 
;
 

select * from data_warehouse.claim_header ch where data_source = 'optz';
 

---optum OOP and alw 
select  b."year" , b.bus_cd, b.ag_flag, net_ind,  case when cdhp_flag is true then 'CDHP' else 'PPO' end as plan ,
        count(distinct b.uth_claim_id) as clms,
        count(*) as allrecs,
        sum(a.std_cost ) as alw,
        sum (copay + deduct + coins) as OOP,
        sum(copay) as copay, sum(deduct) as ded, sum(coins) as coins
from optum_dod.medical a
     join  dev.wc_ambulance_claims  b 
      on b.member_id_src = a.patid::text 
     and b.claim_id_src = a.clmid 
     and b.year = extract(year from a.fst_dt)
where  b.data_source = 'optz'
group by b.year , b.bus_cd, b.ag_flag, net_ind,  cdhp_flag 
order by b.year, cdhp_flag desc, net_ind desc, ag_flag desc;


----truven
update dev.wc_ambulance_claims a set cdhp_flag = case when b.plan_type in ('CDHP','HDHP') then true 
                                                      when b.plan_type in ('PPO','POS','EPO') then false 
                                                      else null end 
from data_warehouse.member_enrollment_yearly b 
where b.uth_member_id = a.uth_member_id 
  and a.year = b.year 
  and b.data_source = 'truv'
  ;
 
 
---truven OOP and allowed
select a."year" , b.bus_cd, b.ag_flag, net_ind,case when cdhp_flag is true then 'CDHP' else 'PPO' end as plan ,
       count(distinct a.uth_claim_id ) as claim_cnt ,
       sum(a.allowed_amount ) as alw, 
       --sum(a.deductible + copay + coins +cob) as OOPcob,
       --sum(a.deductible + copay + coins ) as OOP ,
       --sum(a.paid_amount ) as pd , 
       sum (a.deductible + copay + coins + paid_amount + cob ) as payment,
       sum(a.deductible) as ded, sum(a.copay ) as cop, sum (a.coins) as coins , sum(a.cob) as cob 
from data_warehouse.claim_detail a 
     join  dev.wc_ambulance_claims  b 
      on b.uth_claim_id = a.uth_claim_id 
     and b.data_source = 'truv' and bus_cd = 'COM'
     and b.cdhp_flag is not null 
group by a.year , b.bus_cd, b.ag_flag, cdhp_flag, net_ind
order by year, cdhp_flag desc, net_ind desc, ag_flag desc 
;

---provider


select year, data_source, bus_cd, count(*), count(distinct uth_member_id) as members, sum(total_enrolled_months) as MM
from data_warehouse.member_enrollment_yearly a
where state = 'TX'
  and year between 2018 and 2019
  and data_source in ('mcrt','optz','truv')
group by year, data_source , bus_cd 
order by year, data_source , bus_cd ;


---example bills


---truven 
select * 
from truven.ccaeo a 
where year = 2018
and a.plantyp = 6     ---9 = hdhp, 6 = ppo
and a.proc1 in ('A0225','A0422','A0424','A0425','A0426','A0427','A0428','A0429','A0430','A0431',
	                     'A0432','A0433','A0434','A0435','A0436','A0998','A0999','T2003','T2003') 
and a.egeoloc = 49
;


--hdhp
select * from truven.ccaeo where msclmid = 1104013 and enrolid = 1186258903 and year = 2018
order by svcdate , seqnum 
;


--ppo
select * from truven.ccaeo where msclmid = 83765 and enrolid = 14025002 and year = 2018
order by svcdate , seqnum 
;



	     
	     ---random medicare claims
select * 
from medicare_texas.bcarrier_line_k 
where bene_id = 'ggggggguuwwAnjn'
  and clm_id = 'ggggBuwygjgBgaf'
  
  select * 
from medicare_texas.bcarrier_claims_k 
where bene_id = 'ggggggguuwwAnjn'
  and clm_id = 'ggggBuwygjgBgaf'

  
  
  ---city of houston npi search
 select * 
from medicare_texas.bcarrier_line_k a 
where a.year::int2 = 2018 
  and a.hcpcs_cd in ('A0225','A0422','A0424','A0425','A0426','A0427','A0428','A0429','A0430','A0431',
	                     'A0432','A0433','A0434','A0435','A0436','A0998','A0999','T2003','T2003') 
	     and a.prf_physn_npi =    '1235307752'    --'1447375282' city of houston

---city of houston
select * 
from medicare_texas.bcarrier_claims_k 
where bene_id = 'gggggggffyjauwn'
  and clm_id = 'ggggBujAanfywwA'
	
	---private ambulance
select * 
from medicare_texas.bcarrier_line_k 
where bene_id = 'gggggggnfBjjnuw'
  and clm_id = 'ggggBufywAyganA'
	      
  
  