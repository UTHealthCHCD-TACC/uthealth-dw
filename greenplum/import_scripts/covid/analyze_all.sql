select format('analyse verbose %I.%I;', n.nspname::varchar, t.relname::varchar) 
FROM pg_class t 
JOIN pg_namespace n ON n.oid = t.relnamespace 
WHERE t.relkind = 'r' and n.nspname::varchar = 'opt_20210916' 
order by 1

analyse verbose opt_20210916.carearea;
analyse verbose opt_20210916.diag;
analyse verbose opt_20210916.enc;
analyse verbose opt_20210916.enc_prov;
analyse verbose opt_20210916.ins;
analyse verbose opt_20210916.lab;
analyse verbose opt_20210916.micro;
analyse verbose opt_20210916.obs;
analyse verbose opt_20210916.proc;
analyse verbose opt_20210916.prov;
analyse verbose opt_20210916.pt;
analyse verbose opt_20210916.rx_adm;
analyse verbose opt_20210916.rx_immun;
analyse verbose opt_20210916.rx_patrep;
analyse verbose opt_20210916.rx_presc;
analyse verbose opt_20210916.vis;