REVOKE connect FROM PUBLIC;

CREATE ROLE rjm_utsw NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT LOGIN;
grant covid_analyst to rjm_utsw;

/*
 * Project Specific Roles
 */
create role g823429;
grant g823429 to rjm_utsw;

create schema g823429;
grant all on schema g823429 to group g823429;
ALTER DEFAULT PRIVILEGES IN SCHEMA g823429 grant all on tables to group g823429;


/*
 * CovidDB Admin Role
 */
create role coviddbadmin;
grant connect on database coviddb to group coviddbadmin;
grant all on database coviddb to coviddbadmin;

ALTER DEFAULT PRIVILEGES IN SCHEMA shared  grant all on tables to group coviddbadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA opt_20200525  grant all on tables to group coviddbadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA opt_20201015  grant all on tables to group coviddbadmin;

/*
 * Analyst Role
 */
create role analyst;
grant connect on database coviddb to group analyst;

grant all on schema shared to group analyst;
grant all on all tables in schema shared to group analyst;
ALTER DEFAULT PRIVILEGES IN SCHEMA shared  grant all on tables to group analyst;

grant usage on schema opt_20210107 to group analyst;
grant select on all tables in schema opt_20210107 to group analyst;
ALTER DEFAULT PRIVILEGES IN SCHEMA opt_20210107 grant select on tables to group analyst; 



/*
 * Initial Setup
*/
revoke all on schema public from public;