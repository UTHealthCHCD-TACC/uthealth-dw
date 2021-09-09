select format('analyse verbose %I.%I;', n.nspname::varchar, t.relname::varchar) 
FROM pg_class t 
JOIN pg_namespace n ON n.oid = t.relnamespace 
WHERE t.relkind = 'r' and n.nspname::varchar = 'opt_20210624' 
order by 1

analyse verbose opt_20210624.carearea;
analyse verbose opt_20210624.diag;
analyse verbose opt_20210624.enc;
analyse verbose opt_20210624.enc_prov;
analyse verbose opt_20210624.ins;
analyse verbose opt_20210624.lab;
analyse verbose opt_20210624.micro;
analyse verbose opt_20210624.obs;
analyse verbose opt_20210624.proc;
analyse verbose opt_20210624.prov;
analyse verbose opt_20210624.pt;
analyse verbose opt_20210624.rx_adm;
analyse verbose opt_20210624.rx_immun;
analyse verbose opt_20210624.rx_patrep;
analyse verbose opt_20210624.rx_presc;
analyse verbose opt_20210624.vis;