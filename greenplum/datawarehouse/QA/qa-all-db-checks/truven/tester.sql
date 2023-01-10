select year, table_id_src, count(distinct claim_id_src) as count from dw_staging.claim_detail
where table_id_src = 'mdcrs'
group by year, table_id_src
order by year;

select year, 'ccaes'::text as table, count(distinct msclmid) as count from truven.ccaes
group by year
order by year;


select year, table_id_src, count(distinct claim_id_src) as count from dw_staging.claim_detail
where year = 2021
group by year, table_id_src
order by table_id_src;

/*
2021	ccaeo	25623813
2021	ccaes	1,461,270
2021	mdcro	4327923
2021	mdcrs	538628 */

select year, 'ccaes'::text as table, count(distinct msclmid) as count from truven.ccaes
where year = 2021
group by year;

2021	ccaes	6,517,441

drop table if exists dev.xz_dwqa_temp1;
drop table if exists dev.xz_dwqa_temp2;

select a.msclmid, b.claim_id_src
into dev.xz_dwqa_temp1
from truven.ccaes a left join dw_staging.claim_detail b
on a.msclmid = b.claim_id_src::bigint
where a.year = 2021
limit 10;

select a.*, b.claim_id_src as dim_clm_id
into dev.xz_dwqa_temp2
from dev.xz_dwqa_temp1 a left join data_warehouse.dim_uth_claim_id c
on a.msclmid::text = c.claim_id_src::text
where c.data_source = ;

select distinct data_source from data_warehouse.dim_uth_claim_id;

staging_clean.truv_dim_id

clmid		memberid
33428387703	15336824
3169897701	4289250

select msclmid, enrolid, svcdate from truven.ccaes
where enrolid = 15336824; --no hits

select msclmid, enrolid, svcdate from truven.ccaes
where enrolid = 33428387703; --yes results

select msclmid, year, svcdate from truven.mdcro
where year != extract(year from svcdate)
limit 5;


select claim_id_src, table_id_src, year, from_date_of_service, to_date_of_service from dw_staging.claim_header
where year = 2010
limit 5;

1179287801	ccaes	2010	2010-12-31	2011-01-01
2390435401	ccaes	2010	2010-12-31	2010-12-31
1179287801	ccaes	2010	2010-12-31	2011-01-01
28576368401	ccaes	2010	2010-12-31	2010-12-31
741518902	mdcrs	2010	2010-12-31	2010-12-31

select claim_id_src, table_id_src, year, from_date_of_service, to_date_of_service from dw_staging.claim_header
where year = 2022
limit 5;

/* figuring out where the hell all the claims went for claim header */

drop table if exists dev.xz_dwqa_temp1;
drop table if exists dev.xz_dwqa_temp2;

select year, table_id_src, member_id_src, from_date_of_service, claim_id_src
into dev.xz_dwqa_temp1
from dw_staging.claim_header
where year = 2021 and table_id_src = 'ccaes';

select year, 'ccaes' as table_id_src, enrolid, svcdate, msclmid
into dev.xz_dwqa_temp2
from truven.ccaes
where year = 2021;

drop table if exists dev.xz_dwqa_temp3;

select a.msclmid, b.claim_id_src
into dev.xz_dwqa_temp3
from dev.xz_dwqa_temp2 a left join dev.xz_dwqa_temp1 b
on a.enrolid::text = b.member_id_src and
a.msclmid::text = b.claim_id_src;

select * from dev.xz_dwqa_temp3
where claim_id_src is null
limit 10;

/*
3489503	
290471	
454561	
22994376	
895609	
312391	
6362865	
812401	
1962950	
756103	*/

select * from dev.xz_dwqa_temp2 where msclmid = '3489503'; --enrolid is null
select * from dev.xz_dwqa_temp2 where msclmid = '290471'; --enrolid is null
--maps to 3 enrolids: NULL, 25036165401, and 2487103602

select enrolid, year, dobyr from truven.ccaea
where enrolid = '25036165401'; --dobyr = 1971

select enrolid, year, dobyr from truven.ccaea
where enrolid = '2487103602'; --dobyr = 1965

drop table if exists dev.xz_dwqa_temp4;

select msclmid, count(distinct enrolid) + count(distinct case when enrolid is null then 1 end) as count
into dev.xz_dwqa_temp4
from truven.ccaes
where year = 2021
group by msclmid;

select count(distinct msclmid) as msclmids from dev.xz_dwqa_temp4
where count > 1;

--1519920 having >1 distinct enrolid
--6517441 total distinct

select 1519920/6517441.0; --0.233208

select * from dev.xz_dwqa_temp4
where count > 1
limit 10;

/*
4035525	2
2147582	2
181750	4
7050	4
64022	4
4120061	2
4006008	2
2173644	3
35338430	2
89228	6 */

select * from truven.ccaes where msclmid = 64022 and year = 2021;

drop table if exists dev.xz_dwqa_temp5;

select enrolid::text || msclmid::text as enrol_clm, enrolid::text || msclmid::text || facprof::text as enrol_clm_facprof
into dev.xz_dwqa_temp5
from truven.ccaeo
where year = 2012;

select count(distinct enrol_clm), count(distinct enrol_clm_facprof)
from dev.xz_dwqa_temp5;

/*enrolid||clmid	enrolid||clmid||facprof
	9098129			9098132					--ccaes, 2021 --difference of 3
	226696842		226696929				--ccaeo, 2021 --difference of 87
	475937095		475937265				--ccaeo, 2012 --difference of 170
*/

drop table if exists dev.xz_dwqa_temp;

select year, 'mdcrs' as table_src, enrolid::text || msclmid::text as concat
into dev.xz_dwqa_temp
from truven.mdcrs;

select year, table_src, count(distinct concat) from dev.xz_dwqa_temp
group by year, table_src
order by year;

drop table if exists dev.xz_dwqa_temp;

create table dev.xz_dwqa_temp as
select year, table_id_src, member_id_src || claim_id_src as concat
from dw_staging.claim_detail
where table_id_src = 'mdcrs'
distributed by (concat);

select year, table_id_src, count(distinct claim_id_src) as count from dw_staging.claim_detail
where table_id_src = 'mdcrs'
group by year, table_id_src
order by year;


select count(*) as count from dw_staging.claim_detail
where table_id_src = 'ccaeo'
and year = 2021;

select count(*) as count from truven.ccaeo
where year = 2021;


select msclmid, enrolid from truven.mdcro
where enrolid is null
limit 10; --nulls exist

select msclmid, enrolid from truven.mdcro
where enrolid = 0
limit 10; --no zeroes


select msclmid, enrolid from truven.mdcro
where length(enrolid::text) = 0
limit 10; --no empty... well there are no strings.
