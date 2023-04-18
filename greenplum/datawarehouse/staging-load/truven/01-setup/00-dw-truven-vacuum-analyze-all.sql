/***********************************
* Script purpose: vacuum analyze all Truven tables
* except for reference tables and table_counts
***********************************/

--code to generate code to vacuum analyze all tables
select 'vacuum analyze ' || schemaname || '.' || relname || ';'
from pg_stat_all_tables
  where schemaname = 'truven' and
  (relname like 'ccae%' or relname like 'mdcr%' or relname like 'hpm%')
  order by n_live_tup;
  
--vacuum analyze all tables in ascending order of size
--smaller tables run just fine, ccaeo is the one that takes > 45 mins and hangs
vacuum analyze truven.hpm_ltd;
vacuum analyze truven.hpm_wc;
vacuum analyze truven.mdcrp;
vacuum analyze truven.hpm_std;
vacuum analyze truven.mdcri;
vacuum analyze truven.ccaep;
vacuum analyze truven.ccaei;
vacuum analyze truven.mdcra;
vacuum analyze truven.hpm_elig;
vacuum analyze truven.hpm_abs;
vacuum analyze truven.mdcrs;
vacuum analyze truven.mdcrt;
vacuum analyze truven.ccaea;
vacuum analyze truven.mdcrf;
vacuum analyze truven.ccaes;
vacuum analyze truven.mdcrd;
vacuum analyze truven.mdcro;
vacuum analyze truven.ccaef;
vacuum analyze truven.ccaed;
vacuum analyze truven.ccaet;
vacuum analyze truven.ccaeo;