select data_source, year, member_id_src, race_cd
from dw_staging.mcd_member_enrollment_yearly
where member_id_src = '513493056'
and year = 2019;

select data_source, month_year_id, member_id_src, race_cd
from dw_staging.mcd_member_enrollment_monthly
where member_id_src = '513493056'
and month_year_id / 100 = 2019
order by month_year_id;

dw 5 = mdcd 3
dw 2 = mdcd 2

--spc-side code
select * from chcdwork.dbo.xz_mcd_enrl_mismatches where em_mismatch = 1;