with opt_w_dth as (
select m.*, to_date(death_ym::text,'YYYYMM') death_dt
from optum_dod.mbr_enroll_r m
left outer join optum_dod.mbrwdeath dth 
  on dth.patid = m.patid
)
select count(distinct patid)
from opt_w_dth m
join reference_tables.ref_month_year b
  on b.start_of_month between date_trunc('month', m.eligeff) 
                        and case -- this can ignore enrollments if death_dt is not null (ignores enrollments after death_dt)
	                        	when m.death_dt is not null then m.death_dt
                            	else m.eligend
                            end
;

/*
 * 75813823	-> unique members using logic above (the same used to build member_enrollment_monthly table)
 * 75815144	-> overall unique members
 */

select 75815144 - 75813823;
-- 1321


select m.patid, eligeff, eligend, to_date(death_ym::text,'YYYYMM') death_dt
into dev.ip_optd_elig_dt
from optum_dod.mbr_enroll_r m
join optum_dod.mbrwdeath dth 
  on dth.patid = m.patid
;

select count(distinct patid)
from dev.ip_optd_elig_dt
where eligeff > death_dt;

-- 125711

select 125711. / 75815144;

-- 0.00165812518934211877

drop table if exists dev.ip_optd_missing_enrollment;

with missing_ids as (
select a.member_id_src
from (select member_id_src from data_warehouse.dim_uth_member_id where data_source = 'optd' and claim_created_id != true) a
left join (select distinct member_id_src from dw_staging.optd_member_enrollment_monthly) b
on a.member_id_src = b.member_id_src
where b.member_id_src is null
)
select a.patid, eligeff, eligend, to_date(death_ym::text,'YYYYMM') death_dt, death_ym
into dev.ip_optd_missing_enrollment
from optum_dod.mbr_enroll_r a
left join optum_dod.mbrwdeath dth 
  on dth.patid = a.patid
where a.patid in (select member_id_src::bigint from missing_ids)
order by 1,2;


select 'overall dw missing enrollment', count(distinct patid) pat_count, count(*) row_count
from dev.ip_optd_missing_enrollment
union
select 'missing dw enrollment due having enrollment only after death', count(distinct patid), count(*)
from dev.ip_optd_missing_enrollment
where eligeff > death_dt
;

/* unique patid	row count
 * 1321			1475		overall
 * 1321			1475		eligeff > death_dt
*/

select *, age(eligeff, death_dt)
from dev.ip_optd_missing_enrollment;

-- time between enrollment and month of death varies from 1 month to over 10 years