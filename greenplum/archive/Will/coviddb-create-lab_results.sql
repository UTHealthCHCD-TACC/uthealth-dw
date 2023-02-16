

select distinct a.ptid, a.test_code, trim(a.test_name) as test_name, a.order_date, a.result_date, b.antibody_test, c.test_result, c.interpret  
into g823066.covid_lab_test_results_20210401
from opt_20210401.lab a 
    join  g823066.lab_tests_20210401 b 
      on trim(a.test_name) = trim(b.test_name)
    join g823066.lab_interpret_20210401 c 
      on trim(a.test_result) = trim(c.test_result)
;


select * 
from g823066.lab_tests_20210401
;


select * 
from g823066.lab_interpret_20210401
;