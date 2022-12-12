
select * from dw_staging.member_enrollment_monthly_1_prt_truv;
select * from truven.ccaea;


select table_id_src, count(*) as rows, count(distinct member_id_src) as enrolids
from dw_staging.member_enrollment_monthly_1_prt_truv
group by table_id_src;
--13200867984	141480684

select 'ccaea'::text as table_src, count(*) as rows, count(distinct enrolid) as enrolids from truven.ccaea;
ccaea	382510264	134437437

select 'mdcra'::text as table_src, count(*) as rows, count(distinct enrolid) as enrolids from truven.mdcra;
mdcra	28916421	9295969

select * from truven.ccaea
where enrolid::text not in (select member_id_src from dw_staging.member_enrollment_monthly_1_prt_truv)
limit 10;

select a.enrolid
from truven.ccaea a
left join 