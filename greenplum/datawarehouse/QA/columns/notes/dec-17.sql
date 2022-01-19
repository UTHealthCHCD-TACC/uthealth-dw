
--------------------------------QA----------------------12-17-2021-----------------------
/*

FIXED 1/4/2022 1) Many CPT codes in medicaid raw data are actually revenue codes
2) There are digits mixed up in optum to date of service in 2014-2016
3) There are medicare claims with null revenue center date (most have revenue code 0001
FIXED 1/4/2022 4) optum discharge status as NA -> should be changed to null
4) There are many, many admit and discharge dates in medicaid that are incorrect
Leave for now 1/4/2022 5) Several strange discharge status codes for medicaid
			There are not that many values 
6) truven claim sequence number actually means sequence in entire table - which is why numbers are huge


1) For all truven sources, some ids are not found - need to load into dim again? 
  
 */

------------------------------------------------------------
---------------------diag-----------------
------------------------------------------------------------

--some truven ids not found

------------------------------------------------------------
---------------------proc-----------------
------------------------------------------------------------

--claim_sequence_number - truven fails - original variable actually means record number in whole raw table for year

------------------------------------------------------------
--------------------claim header-----------------
------------------------------------------------------------

--- FIXED 1/4/2022
--- claim_type wrong source var for claim_type - updated in script


------------------------------------------------------------
------------------------claim detail------------------------
------------------------------------------------------------

-----------------------discharge_status mdcd ---------------

-- Leave be for now 1/4/2022
-- 19270

select discharge_status 
from dw_staging.claim_detail
where  discharge_status !~ '^\d{2}$' 
and discharge_status is not null
and discharge_status not in ('', ' ')
and data_source = 'mdcd'
;


select discharge_status 
into dev.mdcd_discharge_del
from dw_staging.claim_detail
where  discharge_status !~ '^\d{2}$' 
and discharge_status is not null
and discharge_status not in ('', ' ')
and data_source = 'mdcd'
group by discharge_status 
;

select * from dev.mdcd_discharge_del;
/*

|discharge_status|
|----------------|
|HH              |
|3O              |
|5               |
|B3              |
|D9              |
|2               |
|OO              |
|T1              |
|C1              |
|1               |
|D1              |
|D2              |
|0               |
|SL              |
|3               |
|OP              |
|XU              |
|9               |
|DA              |
|7               |
|6               |
*/


-------------------------optum-----discharge status---------------------------------

--- FIXED 1/4/2022

--13823022 values 


select discharge_status 
from dw_staging.claim_detail
where  discharge_status !~ '^\d{2}$' 
and discharge_status is not null
and discharge_status not in ('', ' ')
and data_source = 'optz'
;
/*

|discharge_status|
|----------------|
|NA              |
|NA              |
|NA              |
|NA              |
|NA              |
|NA              |
|NA              |
|NA              |
|NA              |
|NA              |
|NA              |
|NA              |
|NA              |
|NA              |
|NA              |
|NA              |
|NA              |
|NA              |
|NA              |
|NA              |
|NA              |
|NA              |
|NA              |
|NA              |
|NA              |
|NA              |

 */

----------------------admit//////-discharge_date mdcd ---------------


select * from dw_staging.claim_detail 
where data_source = 'mdcd'
and admit_date not between '2000-01-01' and '2021-12-31';

/*

|admit_date|discharge_date|
|----------|--------------|
|1999-01-05|0001-01-01    |
|1999-05-07|0001-01-01    |
|1999-10-10|0001-01-01    |
|1999-10-10|0001-01-01    |
|1998-01-29|0001-01-01    |
|1999-01-05|0001-01-01    |
|1949-11-22|0001-01-01    |
|0001-01-01|0001-01-01    |
|1999-01-05|0001-01-01    |
|1988-07-26|0001-01-01    |
|1988-07-26|0001-01-01    |
|1993-04-17|0001-01-01    |

*/


----------------optum failing to date of service---------------

--- many fail just because of null 

select * from dw_staging.claim_detail 
where to_date_of_service not between '2007-01-01' and current_date
and to_date_of_service is not null
and data_source = 'optz';


--select claim_id_src from data_warehouse.dim_uth_claim_id where uth_claim_id = '34725698104'; --38JRNFRVLO

select * from optum_zip.medical where clmid = '38JRNFRVLO';

/*



|from_date_of_service|to_date_of_service|
|--------------------|------------------|
|2013-06-18          |9999-12-31        |
|2013-01-19          |2103-01-19        |
|2013-05-23          |9999-12-31        |
|2013-06-11          |2023-06-11        |
|2016-11-21          |2106-11-21        |
|2013-04-23          |2103-04-23        |
|2013-10-05          |9999-12-31        |
|2013-04-23          |2103-04-23        |
|2013-11-28          |9999-12-31        |
|2013-11-28          |9999-12-31        |
|2013-12-13          |9999-12-31        |
|2013-04-09          |2043-04-09        |
|2016-11-14          |2106-11-20        |
|2013-03-03          |9999-12-31        |
|2013-10-23          |9999-12-31        |
|2015-01-08          |2105-01-08        |
|2013-03-08          |2031-03-08        |

*/


---------------------------cpt cpdes mdcd-------------------------------

--- FIXED 1/4/2022
--it turns out they are revenue codes
--it is only a problem in medicaid.clm_detail
--added logic to load script 1/4/2022


select * from dw_staging.claim_detail 
where data_source = 'mdcd'
and cpt_hcpcs_cd !~ '^[[:alnum:]]{5,7}$' and cpt_hcpcs_cd is not null 
and cpt_hcpcs_cd not in ('',' ');

/*
|cpt_hcpcs_cd|
|------------|
|762         |
|450         |
|370         |
|302         |
|300         |
|370         |
|391         |
|250         |
|350         |
|801         |
|250         |
|370         |
|450         |
|240         |
|510         |
*/



select count(*) from medicaid.clm_detail 
where proc_cd ~ '^\d{3}$'
and proc_cd = rev_cd ;

--99,110,165

select count(*) from medicaid.clm_detail 
where proc_cd ~ '^\d{3}$'
and sub_proc_cd ~ '^\d{5}$'
and proc_cd = rev_cd ;

--16,251,325



select * from medicaid.clm_detail 
where proc_cd ~ '^\d{3}$'
and proc_cd = rev_cd ;

/*

|proc_cd|sub_proc_cd|
|-------|-----------|
|260    |96375      |
|510    |99214      |
|272    |272        |
|510    |99214      |
|942    |99406      |
|272    |272        |
|710    |710        |
|258    |258        |
|360    |29824      |
|250    |250        |
|370    |370        |

*/






/*
---------------------------mcrt/mcrn from date of service---------
------- revenue center date is null on several institutional claims
------if rev center data is null but base claim is not null should we take baseclaim?-------
outpatient_revenue_center
hha_revenue_center
hospice_revenue_center
*
* When Revenue cd = 0001 (for most part), rev center date is null 
*
*
*
**/


select r.rev_cntr, count(*)
from medicare_national.hospice_revenue_center_k r 
where rev_cntr_dt is null
group by rev_cntr 
;

/*
 * 
|rev_cntr|count  |
|--------|-------|
|0001    |2458465|
|0272    |6      |
|0623    |22     |
|0270    |89     |
*
*/


select r.clm_thru_dt, r.rev_cntr_dt, b.clm_from_dt, b.clm_thru_dt 
from medicare_national.hha_revenue_center_k  r 
join medicare_national.hha_base_claims_k b on r.bene_id = b.bene_id and r.clm_id = b.clm_id 
where rev_cntr_dt is null
and b.clm_from_dt is null;




