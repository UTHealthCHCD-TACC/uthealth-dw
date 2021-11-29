
---*******
--- clm_header clm_dx and clm_proc fields
-----------
drop table if exists  dev.wc_pfp_clm1920_hdr;


with cte_enrl as ( 
	select a.client_nbr, min(a.sex) as sex, min( make_date( substring(dob,1,4)::int, substring(dob,5,2)::int, substring(dob,7,2)::int ) ) as dob_date, min(dob) as dob,
	min(substring(elig_date,5,2) || '01' || substring(elig_date,1,4) ) as enrl_start,  max(substring(elig_date,5,2) || '01' || substring(elig_date,1,4)) as enrl_end
	from medicaid.enrl a 
	where a.year_fy between 2017 and 2020
	group by client_nbr	
 )
select p.pcn, h.icn, e.sex, substring(e.dob,5,2) || substring(e.dob,7,2) || substring(e.dob,1,4) as dob, e.dob_date, e.enrl_start, e.enrl_end,
       trim(h.pat_stat_cd) as DischargeStatus, f.admit_id ,
       case when trim(p.bill) = '' then '' else lpad(trim(p.bill),4,'0') end as TypeOfBill,
       to_char(h.hdr_frm_dos::date,'mmddyyyy') as ItemFromDate, 
       to_char(h.hdr_to_dos::date,'mmddyyyy') as ItemToDate, 
       to_char(case when trim(h.adm_dt) = '' then h.hdr_frm_dos else h.adm_dt end::date,'mmddyyyy') as AdmitDate,
       to_char(case when trim(h.dis_dt) = '' then h.hdr_to_dos else h.dis_dt end::date,'mmddyyyy') as DischargeDate,
       case when BILL_PROV_TY_CD in ('08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31','32','33','34','35','36','37','38','39','40','41','42','43','44','45','46','47','48','49','50','51','52','53','54','55','56','58','59','60','61','62','64','65','66','67','68','69','71','72','73',
                                     '74','75','78','79','80','81','82','83','84','86','90','91','92','93','94','95','96','97','98','AA','AB','AC','CC','CI','DA','DE','EH','FH','HF','HP') then 1
            when BILL_PROV_TY_CD in ('04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31','32','33','34','35','36','37','38','39','40','41','42','43','44','45','46','47','48','49','50','51','52','53','54','55','56','58','59','60','61','62','64','65','66','67','68',
                                     '69','71','72','73','74','75','78','79','80','81','82','83','84','86','90','91','92','93','94','95','96') then 2 
            when BILL_PROV_TY_CD in ('06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31','32','33','34','35','36','37','38','39','40','41','42','43','44','45','46','47','48','49','50','51','52','53','54','55','56','58','59','60','61','62','64','65','66','67','68','69','71',
                                     '72','73','74','75','78','79','80','81','82','83','84','86','90','91','92','93','94','95','96','97','98') then 3
             else 4
          end as ProviderType,
	   case when proc_dt_1 > '0' and proc_dt_1::date between '1990-01-01' and '2025-01-01' then to_char(trim(proc_dt_1)::date,'mmddyyyy') else '' end || 
       case when proc_dt_2 > '0' and proc_dt_2::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_2)::date,'mmddyyyy')  else '' end || 
       case when proc_dt_3 > '0' and proc_dt_3::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_3)::date,'mmddyyyy')  else '' end || 
       case when proc_dt_4 > '0' and proc_dt_4::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_4)::date,'mmddyyyy')  else '' end || 
       case when proc_dt_5 > '0' and proc_dt_5::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_5)::date,'mmddyyyy')  else '' end || 
       case when proc_dt_6 > '0' and proc_dt_6::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_6)::date,'mmddyyyy')  else '' end ||
       case when proc_dt_7 > '0' and proc_dt_7::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_7)::date,'mmddyyyy')  else '' end ||
       case when proc_dt_8 > '0' and proc_dt_8::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_8)::date,'mmddyyyy')  else '' end ||
       case when proc_dt_9 > '0' and proc_dt_9::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_9)::date,'mmddyyyy')  else '' end ||
       case when proc_dt_10 > '0' and proc_dt_10::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_10)::date,'mmddyyyy')  else '' end ||
       case when proc_dt_11 > '0' and proc_dt_11::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_11)::date,'mmddyyyy') else '' end ||
       case when proc_dt_12 > '0' and proc_dt_12::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_12)::date,'mmddyyyy') else '' end ||
       case when proc_dt_13 > '0' and proc_dt_13::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_13)::date,'mmddyyyy') else '' end ||
       case when proc_dt_14 > '0' and proc_dt_14::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_14)::date,'mmddyyyy') else '' end ||
       case when proc_dt_15 > '0' and proc_dt_15::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_15)::date,'mmddyyyy') else '' end ||
       case when proc_dt_16 > '0' and proc_dt_16::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_16)::date,'mmddyyyy') else '' end ||
       case when proc_dt_17 > '0' and proc_dt_17::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_17)::date,'mmddyyyy') else '' end ||
       case when proc_dt_18 > '0' and proc_dt_18::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_18)::date,'mmddyyyy') else '' end ||
       case when proc_dt_19 > '0' and proc_dt_19::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_19)::date,'mmddyyyy') else '' end ||
       case when proc_dt_20 > '0' and proc_dt_20::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_20)::date,'mmddyyyy') else '' end ||
       case when proc_dt_21 > '0' and proc_dt_21::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_21)::date,'mmddyyyy') else '' end ||
       case when proc_dt_22 > '0' and proc_dt_22::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_22)::date,'mmddyyyy') else '' end ||
       case when proc_dt_23 > '0' and proc_dt_23::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_23)::date,'mmddyyyy') else '' end ||
       case when proc_dt_24 > '0' and proc_dt_24::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_24)::date,'mmddyyyy') else '' end 
       as proc_dt_array,
       trim(proc_icd_cd_1)  || 
       case when proc_icd_cd_2 > '0' then ';' || trim(proc_icd_cd_2) else '' end || 
       case when proc_icd_cd_3 > '0' then ';' || trim(proc_icd_cd_3) else '' end || 
       case when proc_icd_cd_4 > '0' then ';' || trim(proc_icd_cd_4) else '' end || 
       case when proc_icd_cd_5 > '0' then ';' || trim(proc_icd_cd_5) else '' end || 
       case when proc_icd_cd_6 > '0' then ';' || trim(proc_icd_cd_6) else '' end ||
       case when proc_icd_cd_7 > '0' then ';' || trim(proc_icd_cd_7) else '' end ||
       case when proc_icd_cd_8 > '0' then ';' || trim(proc_icd_cd_8) else '' end ||
       case when proc_icd_cd_9 > '0' then ';' || trim(proc_icd_cd_9) else '' end ||
       case when proc_icd_cd_10 > '0' then ';' || trim(proc_icd_cd_10) else '' end ||
       case when proc_icd_cd_11 > '0' then ';' || trim(proc_icd_cd_11) else '' end ||
       case when proc_icd_cd_12 > '0' then ';' || trim(proc_icd_cd_12) else '' end ||
       case when proc_icd_cd_13 > '0' then ';' || trim(proc_icd_cd_13) else '' end ||
       case when proc_icd_cd_14 > '0' then ';' || trim(proc_icd_cd_14) else '' end ||
       case when proc_icd_cd_15 > '0' then ';' || trim(proc_icd_cd_15) else '' end ||
       case when proc_icd_cd_16 > '0' then ';' || trim(proc_icd_cd_16) else '' end ||
       case when proc_icd_cd_17 > '0' then ';' || trim(proc_icd_cd_17) else '' end ||
       case when proc_icd_cd_18 > '0' then ';' || trim(proc_icd_cd_18) else '' end ||
       case when proc_icd_cd_19 > '0' then ';' || trim(proc_icd_cd_19) else '' end ||
       case when proc_icd_cd_20 > '0' then ';' || trim(proc_icd_cd_20) else '' end ||
       case when proc_icd_cd_21 > '0' then ';' || trim(proc_icd_cd_21) else '' end ||
       case when proc_icd_cd_22 > '0' then ';' || trim(proc_icd_cd_22) else '' end ||
       case when proc_icd_cd_23 > '0' then ';' || trim(proc_icd_cd_23) else '' end ||
       case when proc_icd_cd_24 > '0' then ';' || trim(proc_icd_cd_24) else '' end ||
       case when proc_icd_cd_25 > '0' then ';' || trim(proc_icd_cd_25) else '' end
       as proc_array,
       d.adm_dx_cd, d.prim_dx_cd, 
       case when trim(dx_cd_1) = '' then '' else trim(dx_cd_1) end ||
       case when trim(dx_cd_2) = '' then '' else ';' || trim(dx_cd_2) end ||
       case when trim(dx_cd_3) = '' then '' else ';' || trim(dx_cd_3) end ||
       case when trim(dx_cd_4) = '' then '' else ';' || trim(dx_cd_4) end ||
       case when trim(dx_cd_5) = '' then '' else ';' || trim(dx_cd_5) end ||
       case when trim(dx_cd_6) = '' then '' else ';' || trim(dx_cd_6) end ||
       case when trim(dx_cd_7) = '' then '' else ';' || trim(dx_cd_7) end ||
       case when trim(dx_cd_8) = '' then '' else ';' || trim(dx_cd_8) end ||
       case when trim(dx_cd_9) = '' then '' else ';' || trim(dx_cd_9) end ||
       case when trim(dx_cd_10) = '' then '' else ';' || trim(dx_cd_10) end ||
       case when trim(dx_cd_11) = '' then '' else ';' || trim(dx_cd_11) end ||
       case when trim(dx_cd_12) = '' then '' else ';' || trim(dx_cd_12) end ||
       case  when trim(dx_cd_13) = '' then '' else ';' || trim(dx_cd_13) end ||
       case  when trim(dx_cd_14) = '' then '' else ';' || trim(dx_cd_14) end ||
       case  when trim(dx_cd_15) = '' then '' else ';' || trim(dx_cd_15) end ||
       case  when trim(dx_cd_16) = '' then '' else ';' || trim(dx_cd_16) end ||
       case  when trim(dx_cd_17) = '' then '' else ';' || trim(dx_cd_17) end ||
       case  when trim(dx_cd_18) = '' then '' else ';' || trim(dx_cd_18) end ||
       case  when trim(dx_cd_19) = '' then '' else ';' || trim(dx_cd_19) end ||
       case  when trim(dx_cd_20) = '' then '' else ';' || trim(dx_cd_20) end ||
       case  when trim(dx_cd_21) = '' then '' else ';' || trim(dx_cd_21) end ||
       case  when trim(dx_cd_22) = '' then '' else ';' || trim(dx_cd_22) end ||
       case  when trim(dx_cd_23) = '' then '' else ';' || trim(dx_cd_23) end ||
       case  when trim(dx_cd_24) = '' then '' else ';' || trim(dx_cd_24) end ||
       case  when trim(dx_cd_25) = '' then '' else ';' || trim(dx_cd_25) end 
       as dx_array
   into dev.wc_pfp_clm1920_hdr
from medicaid.clm_header h 
  join medicaid.clm_proc p  
    on h.icn = p.icn 
   and h.year_fy = p.year_fy 
  join medicaid.clm_dx d 
    on h.icn = d.icn 
   and h.year_fy = d.year_fy 
  join cte_enrl e 
    on e.client_nbr = p.pcn 
  left outer join medicaid.admit_clm f 
      on f.clm_id = h.icn 
where h.year_fy between 2019 and 2020
;


drop table if exists dev.wc_pfp_clm1920_dtl;

---*******
--- clm_detail fields
-----------
select icn, 
       min( lpad(trim(d.pos),2,'0') ) as pos, 
       min(d.proc_cd) as proc, 
       case when min(trim(rev_cd) ) = '' then 'F' else 'P' end as rev_cd,
       string_agg(case when length(trim(rev_cd)) < 5 then null else lpad(trim(rev_cd),4,'0') end,';') as revs, 
       string_agg(case when trim(d.proc_cd) ='' then null else trim(d.proc_cd) end ,';') as cpt_hcpcs_array
   into dev.wc_pfp_clm1920_dtl
from medicaid.clm_detail d
where year_fy between 2019 and 2020
group by icn
;



---**********************************************************************************************************
--- Encounter 
---**********************************************************************************************************

--enc
with cte_enc_enrl as ( 
	select a.client_nbr, min(a.sex) as sex, min( make_date( substring(dob,1,4)::int, substring(dob,5,2)::int, substring(dob,7,2)::int ) ) as dob_date, min(dob) as dob,
	min(substring(elig_date,5,2) || '01' || substring(elig_date,1,4) ) as enrl_start,  max(substring(elig_date,5,2) || '01' || substring(elig_date,1,4)) as enrl_end
	from medicaid.enrl a 
	where a.year_fy between 2017 and 2020
	group by client_nbr	
 )
 insert  into dev.wc_pfp_clm1920_hdr 
select p.mem_id , h.derv_enc , e.sex, substring(e.dob,5,2) || substring(e.dob,7,2) || substring(e.dob,1,4) as dob, e.dob_date, e.enrl_start, e.enrl_end,
       trim(h.pat_stat ) as DischargeStatus, f.admit_id ,
       case when trim(p.bill) = '' then '' else lpad(trim(p.bill),4,'0') end as TypeOfBill,
       case when frm_dos between '1990-01-01' and '2025-01-01' then to_char(frm_dos,'mmddyyyy') else null end as ItemFromDate, 
       case when to_dos between '1990-01-01' and '2025-01-01' then to_char(to_dos,'mmddyyyy') else null end as ItemToDate, 
       case when h.adm_dt between '1990-01-01' and '2025-01-01' then to_char(h.adm_dt,'mmddyyyy') else to_char(frm_dos,'mmddyyyy') end as AdmitDate, 
       case when dis_dt between '1990-01-01' and '2025-01-01' then to_char(dis_dt,'mmddyyyy') else to_char(to_dos,'mmddyyyy') end as DischargeDate, 
       case when BILL_PROV_TYP_CD in ('08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31','32','33','34','35','36','37','38','39','40','41','42','43','44','45','46','47','48','49','50','51','52','53','54','55','56','58','59','60','61','62','64','65','66','67','68','69','71','72','73',
                                     '74','75','78','79','80','81','82','83','84','86','90','91','92','93','94','95','96','97','98','AA','AB','AC','CC','CI','DA','DE','EH','FH','HF','HP') then 1
            when BILL_PROV_TYP_CD in ('04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31','32','33','34','35','36','37','38','39','40','41','42','43','44','45','46','47','48','49','50','51','52','53','54','55','56','58','59','60','61','62','64','65','66','67','68',
                                     '69','71','72','73','74','75','78','79','80','81','82','83','84','86','90','91','92','93','94','95','96') then 2 
            when BILL_PROV_TYP_CD in ('06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31','32','33','34','35','36','37','38','39','40','41','42','43','44','45','46','47','48','49','50','51','52','53','54','55','56','58','59','60','61','62','64','65','66','67','68','69','71',
                                     '72','73','74','75','78','79','80','81','82','83','84','86','90','91','92','93','94','95','96','97','98') then 3
             else 4
          end as ProviderType,
          	   case when proc_dt_1 > '0' and proc_dt_1::date between '1990-01-01' and '2025-01-01' then to_char(trim(proc_dt_1)::date,'mmddyyyy') else '' end || 
       case when proc_dt_2 > '0' and proc_dt_2::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_2)::date,'mmddyyyy')  else '' end || 
       case when proc_dt_3 > '0' and proc_dt_3::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_3)::date,'mmddyyyy')  else '' end || 
       case when proc_dt_4 > '0' and proc_dt_4::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_4)::date,'mmddyyyy')  else '' end || 
       case when proc_dt_5 > '0' and proc_dt_5::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_5)::date,'mmddyyyy')  else '' end || 
       case when proc_dt_6 > '0' and proc_dt_6::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_6)::date,'mmddyyyy')  else '' end ||
       case when proc_dt_7 > '0' and proc_dt_7::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_7)::date,'mmddyyyy')  else '' end ||
       case when proc_dt_8 > '0' and proc_dt_8::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_8)::date,'mmddyyyy')  else '' end ||
       case when proc_dt_9 > '0' and proc_dt_9::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_9)::date,'mmddyyyy')  else '' end ||
       case when proc_dt_10 > '0' and proc_dt_10::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_10)::date,'mmddyyyy')  else '' end ||
       case when proc_dt_11 > '0' and proc_dt_11::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_11)::date,'mmddyyyy') else '' end ||
       case when proc_dt_12 > '0' and proc_dt_12::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_12)::date,'mmddyyyy') else '' end ||
       case when proc_dt_13 > '0' and proc_dt_13::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_13)::date,'mmddyyyy') else '' end ||
       case when proc_dt_14 > '0' and proc_dt_14::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_14)::date,'mmddyyyy') else '' end ||
       case when proc_dt_15 > '0' and proc_dt_15::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_15)::date,'mmddyyyy') else '' end ||
       case when proc_dt_16 > '0' and proc_dt_16::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_16)::date,'mmddyyyy') else '' end ||
       case when proc_dt_17 > '0' and proc_dt_17::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_17)::date,'mmddyyyy') else '' end ||
       case when proc_dt_18 > '0' and proc_dt_18::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_18)::date,'mmddyyyy') else '' end ||
       case when proc_dt_19 > '0' and proc_dt_19::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_19)::date,'mmddyyyy') else '' end ||
       case when proc_dt_20 > '0' and proc_dt_20::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_20)::date,'mmddyyyy') else '' end ||
       case when proc_dt_21 > '0' and proc_dt_21::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_21)::date,'mmddyyyy') else '' end ||
       case when proc_dt_22 > '0' and proc_dt_22::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_22)::date,'mmddyyyy') else '' end ||
       case when proc_dt_23 > '0' and proc_dt_23::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_23)::date,'mmddyyyy') else '' end ||
       case when proc_dt_24 > '0' and proc_dt_24::date between '1990-01-01' and '2025-01-01' then ';' || to_char(trim(proc_dt_24)::date,'mmddyyyy') else '' end 
       as proc_dt_array, 
              trim(proc_icd_cd_1)  || 
       case when proc_icd_cd_2 > '0' then ';' || trim(proc_icd_cd_2) else '' end || 
       case when proc_icd_cd_3 > '0' then ';' || trim(proc_icd_cd_3) else '' end || 
       case when proc_icd_cd_4 > '0' then ';' || trim(proc_icd_cd_4) else '' end || 
       case when proc_icd_cd_5 > '0' then ';' || trim(proc_icd_cd_5) else '' end || 
       case when proc_icd_cd_6 > '0' then ';' || trim(proc_icd_cd_6) else '' end ||
       case when proc_icd_cd_7 > '0' then ';' || trim(proc_icd_cd_7) else '' end ||
       case when proc_icd_cd_8 > '0' then ';' || trim(proc_icd_cd_8) else '' end ||
       case when proc_icd_cd_9 > '0' then ';' || trim(proc_icd_cd_9) else '' end ||
       case when proc_icd_cd_10 > '0' then ';' || trim(proc_icd_cd_10) else '' end ||
       case when proc_icd_cd_11 > '0' then ';' || trim(proc_icd_cd_11) else '' end ||
       case when proc_icd_cd_12 > '0' then ';' || trim(proc_icd_cd_12) else '' end ||
       case when proc_icd_cd_13 > '0' then ';' || trim(proc_icd_cd_13) else '' end ||
       case when proc_icd_cd_14 > '0' then ';' || trim(proc_icd_cd_14) else '' end ||
       case when proc_icd_cd_15 > '0' then ';' || trim(proc_icd_cd_15) else '' end ||
       case when proc_icd_cd_16 > '0' then ';' || trim(proc_icd_cd_16) else '' end ||
       case when proc_icd_cd_17 > '0' then ';' || trim(proc_icd_cd_17) else '' end ||
       case when proc_icd_cd_18 > '0' then ';' || trim(proc_icd_cd_18) else '' end ||
       case when proc_icd_cd_19 > '0' then ';' || trim(proc_icd_cd_19) else '' end ||
       case when proc_icd_cd_20 > '0' then ';' || trim(proc_icd_cd_20) else '' end ||
       case when proc_icd_cd_21 > '0' then ';' || trim(proc_icd_cd_21) else '' end ||
       case when proc_icd_cd_22 > '0' then ';' || trim(proc_icd_cd_22) else '' end ||
       case when proc_icd_cd_23 > '0' then ';' || trim(proc_icd_cd_23) else '' end ||
       case when proc_icd_cd_24 > '0' then ';' || trim(proc_icd_cd_24) else '' end 
       as proc_array,
       d.adm_dx_cd, d.prim_dx_cd, 
       case when trim(dx_cd_1) = '' then '' else trim(dx_cd_1) end ||
       case when trim(dx_cd_2) = '' then '' else ';' || trim(dx_cd_2) end ||
       case when trim(dx_cd_3) = '' then '' else ';' || trim(dx_cd_3) end ||
       case when trim(dx_cd_4) = '' then '' else ';' || trim(dx_cd_4) end ||
       case when trim(dx_cd_5) = '' then '' else ';' || trim(dx_cd_5) end ||
       case when trim(dx_cd_6) = '' then '' else ';' || trim(dx_cd_6) end ||
       case when trim(dx_cd_7) = '' then '' else ';' || trim(dx_cd_7) end ||
       case when trim(dx_cd_8) = '' then '' else ';' || trim(dx_cd_8) end ||
       case when trim(dx_cd_9) = '' then '' else ';' || trim(dx_cd_9) end ||
       case when trim(dx_cd_10) = '' then '' else ';' || trim(dx_cd_10) end ||
       case when trim(dx_cd_11) = '' then '' else ';' || trim(dx_cd_11) end ||
       case when trim(dx_cd_12) = '' then '' else ';' || trim(dx_cd_12) end ||
       case  when trim(dx_cd_13) = '' then '' else ';' || trim(dx_cd_13) end ||
       case  when trim(dx_cd_14) = '' then '' else ';' || trim(dx_cd_14) end ||
       case  when trim(dx_cd_15) = '' then '' else ';' || trim(dx_cd_15) end ||
       case  when trim(dx_cd_16) = '' then '' else ';' || trim(dx_cd_16) end ||
       case  when trim(dx_cd_17) = '' then '' else ';' || trim(dx_cd_17) end ||
       case  when trim(dx_cd_18) = '' then '' else ';' || trim(dx_cd_18) end ||
       case  when trim(dx_cd_19) = '' then '' else ';' || trim(dx_cd_19) end ||
       case  when trim(dx_cd_20) = '' then '' else ';' || trim(dx_cd_20) end ||
       case  when trim(dx_cd_21) = '' then '' else ';' || trim(dx_cd_21) end ||
       case  when trim(dx_cd_22) = '' then '' else ';' || trim(dx_cd_22) end ||
       case  when trim(dx_cd_23) = '' then '' else ';' || trim(dx_cd_23) end ||
       case  when trim(dx_cd_24) = '' then '' else ';' || trim(dx_cd_24) end 
       as dx_array
from medicaid.enc_header h 
  join medicaid.enc_proc p  
    on h.derv_enc = p.derv_enc 
   and h.year_fy = p.year_fy 
  join medicaid.enc_dx d 
    on h.derv_enc = d.derv_enc 
   and h.year_fy = d.year_fy 
  join cte_enc_enrl e 
    on e.client_nbr = trim(p.mem_id) 
  left outer join medicaid.admit_clm f 
    on f.clm_id = h.derv_enc 
where h.year_fy between 2019 and 2020
;




insert  into dev.wc_pfp_clm1920_dtl
select derv_enc , 
       min( lpad(trim(d.pos),2,'0') ) as pos, 
       min(d.proc_cd) as proc, 
       case when min(trim(rev_cd) ) = '' then 'F' else 'P' end as rev_cd,
       string_agg(case when length(trim(rev_cd)) < 5 then null else lpad(trim(rev_cd),4,'0') end,';') as revs, 
       string_agg(case when trim(d.proc_cd) ='' then null else trim(d.proc_cd) end ,';') as cpt_hcpcs_array
from medicaid.enc_det  d
where year_fy between 2019 and 2020
group by derv_enc 
;


select count(*), count(distinct icn) , year_fy 
from dev.wc_pfp_clm1617_hdr;

select count(*), count(distinct icn) 
from dev.wc_pfp_clm1617_dtl;

