/*****************
 * Counts how many rows there are per year in HPM tables
 */

with abs as (select year, count(*) as abs_count
	from truven.hpm_abs
	group by year),
elig as (select year, count(*) as elig_count
	from truven.hpm_elig
	group by year),
ltd as (select year, count(*) as ltd_count
	from truven.hpm_ltd
	group by year),
std as (select year, count(*) as std_count
	from truven.hpm_std
	group by year),
wc as (select year, count(*) as wc_count
	from truven.hpm_wc
	group by year)
select abs.year, abs.abs_count, elig.elig_count, ltd.ltd_count, std.std_count, wc.wc_count
from abs left join elig on abs.year = elig.year
	left join ltd on abs.year = ltd.year
	left join std on abs.year = std.year
	left join wc on abs.year = wc.year
order by abs.year;

--check distinct member counts

select year, count(distinct enrolid)
from truven.hpm_ltd
group by year
order by year;


select year, count(*)
from truven.hpm_ltd
group by year
order by year;








