-------------------------------
-----------------------
--claim_sequence_number notes
------------------------
------------------------------



------------------------
--claim_sequence_number
------------------------

select claim_sequence_number from data_warehouse.claim_icd_proc cip 
where claim_sequence_number not between 1 and 700 and claim_sequence_number is not null
;


select claim_sequence_number from data_warehouse.claim_icd_proc 
where data_source = 'truv'
;

select claim_sequence_number from data_warehouse.claim_icd_proc 
where data_source = 'truv' and claim_sequence_number is not null;
;

--- truven is all null 
--- not sure what is going on here 
--- 




-------
--icd_type
----------

select icd_type from data_warehouse.claim_icd_proc where data_source = 'mcrn'
group by icd_type;

-- all null for medicare
-- missing from insert file medicare 

-- fine for optum

-- wrong for medicaid starting 2015 - before was all 9 so the 9's check out but then starting using 0 instead of 10 so come up wrong

-- truven some nulls and 0 and 9 instead of 10 and 9
select "year", icd_type, count(*)
from data_warehouse.claim_icd_proc 
        where data_source = 'truv'
        group by "year", icd_type
    order by year;    
    
/*
 * |year|icd_type|count    |
|----|--------|---------|
|2010|        |8997     |
|2011|        |345414802|
|2011|9       |310      |
|2012|        |337525220|
|2012|0       |90       |
|2012|9       |390      |
|2013|0       |25       |
|2013|        |270710411|
|2013|9       |110      |
|2014|        |279135526|
|2014|0       |457      |
|2014|9       |8130     |
|2015|0       |48248824 |
|2015|        |2890515  |
|2015|9       |130370100|
|2016|0       |176700052|
|2016|9       |1578     |
|2016|        |10113    |
|2017|9       |4533     |
|2017|        |837      |
|2017|0       |151677512|
|2018|9       |2434     |
|2018|        |70       |
|2018|0       |144435412|
|2019|9       |3045     |
|2019|        |554      |
|2019|0       |238007803|
|2020|0       |1603192  |

 */    
    
    
    
