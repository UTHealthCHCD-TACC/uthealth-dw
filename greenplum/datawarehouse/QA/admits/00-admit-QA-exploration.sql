/*********************
*Let's see if there are issues with the admit table
*
*/

select * from data_warehouse.admission_acute_ip;

select * from data_warehouse.admission_acute_ip_claims;

/********************
 * Question: any dates out of range?
 * 
 * Ans: Possibly yes, but the vast majority are good - nothing to fix here
 */

select data_source,
	sum(case when extract(year from admit_date) not between 2011 and 2023 then 1 else 0 end) as admit_oor,
	sum(case when extract(year from discharge_date) not between 2011 and 2023 then 1 else 0 end) as discharge_oor,
	sum(case when (discharge_date - admit_date) < 0 then 1 else 0 end ) as discharge_before_admit,
	count(*),
	sum(case when extract(year from admit_date) not between 2011 and 2023 then 1 else 0 end) * 1.0 / count(*) as admit_oor_pct,
	sum(case when extract(year from discharge_date) not between 2011 and 2023 then 1 else 0 end) * 1.0 / count(*) as discharge_oor_pct,
	sum(case when (discharge_date - admit_date) < 0 then 1 else 0 end ) * 1.0 / count(*) as discharge_before_admit_pct
from data_warehouse.admission_acute_ip
group by data_source;

data_source	admit_oor	discharge_oor	discharge_before_admit	count	admit_oor_pct	discharge_oor_pct	discharge_before_admit_pct
mcpp	0	0	0	306043	0.000000000000000000000000	0.000000000000000000000000	0.000000000000000000000000
mhtw	0	0	0	12740	0.000000000000000000000000	0.000000000000000000000000	0.000000000000000000000000
mcrn	7	0	0	2432050	0.000002878230299541539031	0.000000000000000000000000	0.000000000000000000000000
mcrt	7	0	0	3441852	0.000002033788785804851574	0.000000000000000000000000	0.000000000000000000000000
optz	0	1	0	10519834	0.000000000000000000000000	0.000000095058534193600393	0.000000000000000000000000
truv	0	0	0	9737236	0.000000000000000000000000	0.000000000000000000000000	0.000000000000000000000000
optd	0	1	0	10494465	0.000000000000000000000000	0.000000095288325798408971	0.000000000000000000000000
mdcd	0	1	0	4936969	0.000000000000000000000000	0.000000202553429037127841	0.000000000000000000000000

select * from data_warehouse.admission_acute_ip where extract(year from admit_date) not between 2011 and 2023;
--these are probably typos in the raw data, but there are very few of them
mcrn	2007	532962777-000-2007	532962777	2007-06-20	2017-07-09	3672	06
mcrt	2007	358459887-000-2007	358459887	2007-06-20	2017-07-09	3672	06
mcrn	2008	532213726-000-2008	532213726	2008-03-01	2018-03-11	3662	03
mcrn	2008	531994797-000-2008	531994797	2008-08-08	2015-08-17	2565	51
mcrn	2009	533336581-000-2009	533336581	2009-07-10	2019-07-11	3653	03
mcrt	2009	359207208-000-2009	359207208	2009-08-28	2019-06-29	3592	51
mcrn	2010	533467104-000-2010	533467104	2010-02-09	2020-02-10	3653	20
mcrt	2010	530439252-000-2010	530439252	2010-03-27	2015-04-03	1833	02
mcrt	2010	361949627-000-2010	361949627	2010-05-08	2019-05-29	3308	03
mcrt	2010	359372597-000-2010	359372597	2010-05-18	2019-05-22	3291	05
mcrn	2010	531104889-000-2010	531104889	2010-05-27	2020-07-04	3691	01
mcrt	2010	530472500-000-2010	530472500	2010-07-28	2015-10-09	1899	06
mcrn	2010	532021750-000-2010	532021750	2010-09-07	2019-09-12	3292	01
mcrt	2010	361005906-000-2010	361005906	2010-10-03	2016-10-11	2200	62

/*************************
 * Question: any admit dates inside of admit-discharge pair?
 * 
 * Answer: nope
 */

select a.admit_date as a_admit_dt, b.admit_date as b_admit_dt, b.discharge_date as b_discharge_date
from data_warehouse.admission_acute_ip a inner join data_warehouse.admission_acute_ip b
	on a.data_source = b.data_source
	and a.uth_member_id = b.uth_member_id
	and a.admit_date between b.admit_date+1 and b.discharge_date;

/**********************
 * What about the discharge to inpatient and/or code = 30?
 * 
 * Looks like there's a time allowance of 1... let's look for twos
 */

select * from data_warehouse.admission_acute_ip where days_to_readmit = 2 and discharge_status = '30';
--these cases exist

--how frequent?

select data_source,
	sum(case when days_to_readmit < 4 and discharge_status = '30' then 1 else 0 end) as readmit_dschrg_30,
	sum(case when days_to_readmit < 4 and discharge_status = '30' then 1 else 0 end) * 1.0 / count(*) as pct
from data_warehouse.admission_acute_ip
group by data_source;
--extremely few
mhtw	0	0.000000000000000000000000
mcrn	0	0.000000000000000000000000
mcpp	0	0.000000000000000000000000
mcrt	0	0.000000000000000000000000
truv	1272	0.00013063255322146860
optd	403	0.000038401195296758815242
mdcd	304	0.000061576242427286863661
optz	418	0.000039734467292924964405


select data_source,
	sum(case when days_to_readmit < 4 and discharge_status in ('02', '05', '65', '82', '85', '88', '94', '94', '30') then 1 else 0 end) as readmit_dschrg_30,
	sum(case when days_to_readmit < 4 and discharge_status in ('02', '05', '65', '82', '85', '88', '94', '94', '30') then 1 else 0 end) * 1.0 / count(*) as pct
from data_warehouse.admission_acute_ip
group by data_source;
--also extremely few
mhtw	0	0.000000000000000000000000
mcrn	711	0.00029234596328200489
mcpp	1	0.000003267514695647343674
truv	2576	0.00026455145998309993
optd	3423	0.00032617193920795391
mcrt	971	0.00028211555871664441
mdcd	1967	0.00039842259491603046
optz	3647	0.00034667847420406063

/********************
 * Who's got a very long IP stay?
 * 
 * Answer: there are some, but very infrequent
 */

select * from data_warehouse.admission_acute_ip where admission_days > 365;

select data_source,
	sum(case when admission_days > 365 then 1 else 0 end) as long_admit,
	sum(case when admission_days > 365 then 1 else 0 end) * 1.0 / count(*) as pct
from data_warehouse.admission_acute_ip
group by data_source;

/*************************
 * Are there a disproportionate number of IP stays in the first part of the year
 * when people are more likely to mess up the year?
 */

select extract(month from admit_date) as month,
	count(*) as count,
	count(*) * 1.0 / (select count(*) from data_warehouse.admission_acute_ip) as pct
from data_warehouse.admission_acute_ip
where admission_days > 365
group by extract(month from admit_date)
order by extract(month from admit_date);

/*********************
 * Check how bad dates are
 * 
 * cycles through each year and limits to 10K rows for each year
 */

drop table if exists dev.xz_temp1;

create table dev.xz_temp1 (
	data_source char(4),
	year int2,
	claim_id_src varchar(50),
	member_id_src varchar(50),
	bill varchar(4),
	admit_date date,
	discharge_date date,
	from_date_of_service date,
	to_date_of_service date
)
distributed by (claim_id_src);

do $$
declare
	yr int;
	data_sources text[] := array['mcpp', 'mcrn', 'mcrt', 'mdcd', 'mhtw', 'optd', 'optz', 'truv'];
	data_source text;
begin
	foreach data_source in array data_sources
	loop
		for yr in 2015..2022
		loop
			raise notice 'Inserting % data for %', data_source, yr;
			execute 'insert into dev.xz_temp1
				select data_source, year, claim_id_src, member_id_src, bill,
					admit_date, discharge_date, from_date_of_service, to_date_of_service
				from data_warehouse.claim_detail
				where data_source = ''' || data_source || ''' and
				year = ' || yr || ' and bill in (''111'', ''114'')
				limit 10000;';
		end loop;
	end loop;
end $$;

vacuum analyze dev.xz_temp1;

select data_source,
	sum(case when admit_date = '1900-01-01'::date or admit_date is null then 1 else 0 end) as bad_admit,
	sum(case when discharge_date = '1900-01-01'::date or discharge_date is null then 1 else 0 end) as bad_discharge,
	sum(case when from_date_of_service = '1900-01-01'::date or from_date_of_service is null then 1 else 0 end) as bad_from_dos,
	sum(case when to_date_of_service = '1900-01-01'::date or to_date_of_service is null then 1 else 0 end) as bad_to_dos,
	sum(case when (admit_date = '1900-01-01'::date or admit_date is null) and (from_date_of_service = '1900-01-01'::date or from_date_of_service is null)
		then 1 else 0 end) as no_start_dt,
	sum(case when (discharge_date = '1900-01-01'::date or discharge_date is null) and (to_date_of_service = '1900-01-01'::date or to_date_of_service is null)
		then 1 else 0 end) as no_end_dt,
	count(*),
	sum(case when admit_date = '1900-01-01'::date or admit_date is null then 1 else 0 end) * 1.0 / count(*) as bad_admit_pct,
	sum(case when discharge_date = '1900-01-01'::date or discharge_date is null then 1 else 0 end) * 1.0 / count(*)  as bad_discharge_pct,
	sum(case when from_date_of_service = '1900-01-01'::date or from_date_of_service is null then 1 else 0 end) * 1.0 / count(*)  as bad_from_dos_pct,
	sum(case when to_date_of_service = '1900-01-01'::date or to_date_of_service is null then 1 else 0 end) * 1.0 / count(*)  as bad_to_dos_pct,
	sum(case when (admit_date = '1900-01-01'::date or admit_date is null) and (from_date_of_service = '1900-01-01'::date or from_date_of_service is null)
		then 1 else 0 end) * 1.0 / count(*) as no_start_dt_pct,
	sum(case when (discharge_date = '1900-01-01'::date or discharge_date is null) and (to_date_of_service = '1900-01-01'::date or to_date_of_service is null)
		then 1 else 0 end) * 1.0 / count(*) as no_end_dt_pct
from dev.xz_temp1 group by data_source
order by data_source;
data_source	bad_admit	bad_discharge	bad_from_dos	bad_to_dos	no_start_dt	no_end_dt	count	bad_admit_pct	bad_discharge_pct	bad_from_dos_pct	bad_to_dos_pct	no_start_dt_pct	no_end_dt_pct
mcpp	0	0	0	0	0	0	70000	0.000000000000000000000000	0.000000000000000000000000	0.000000000000000000000000	0.000000000000000000000000	0.000000000000000000000000	0.000000000000000000000000
mcrn	0	0	0	0	0	0	60000	0.000000000000000000000000	0.000000000000000000000000	0.000000000000000000000000	0.000000000000000000000000	0.000000000000000000000000	0.000000000000000000000000
mcrt	0	0	0	0	0	0	60000	0.000000000000000000000000	0.000000000000000000000000	0.000000000000000000000000	0.000000000000000000000000	0.000000000000000000000000	0.000000000000000000000000
mdcd	0	0	0	0	0	0	70006	0.000000000000000000000000	0.000000000000000000000000	0.000000000000000000000000	0.000000000000000000000000	0.000000000000000000000000	0.000000000000000000000000
mhtw	0	0	0	0	0	0	68948	0.000000000000000000000000	0.000000000000000000000000	0.000000000000000000000000	0.000000000000000000000000	0.000000000000000000000000	0.000000000000000000000000
optd	9721	9721	0	651	0	0	70000	0.13887142857142857143	0.13887142857142857143	0.000000000000000000000000	0.00930000000000000000	0.000000000000000000000000	0.000000000000000000000000
optz	4971	4971	0	631	0	0	70000	0.07101428571428571429	0.07101428571428571429	0.000000000000000000000000	0.00901428571428571429	0.000000000000000000000000	0.000000000000000000000000
truv	0	0	0	0	0	0	80000	0.000000000000000000000000	0.000000000000000000000000	0.000000000000000000000000	0.000000000000000000000000	0.000000000000000000000000	0.000000000000000000000000

--looks like just optum is a problem. Is it nulls or 1900?
select * from dev.xz_temp1
where data_source like 'opt%' and admit_date = '1900-01-01'::date or admit_date is null;
--looks like a null problem















