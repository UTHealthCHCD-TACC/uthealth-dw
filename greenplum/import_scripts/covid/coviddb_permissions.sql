REVOKE connect FROM PUBLIC;

CREATE ROLE zena21 NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT LOGIN;
grant covid_analyst to zena21;

/*
 * Project Specific Roles
 */
create role g823676;
grant g823429 to johnjero;
revoke g823429 from mdiaz1;

create schema g823429;
grant all on schema g823429 to group g823429;
grant all on all tables in schema g823429 to group g823429;
ALTER DEFAULT PRIVILEGES IN SCHEMA g823429 grant all on tables to group g823429;


/*
 * CovidDB Admin Role
 */
create role coviddbadmin;
grant connect on database coviddb to group coviddbadmin;
grant all on database coviddb to coviddbadmin;

ALTER DEFAULT PRIVILEGES IN SCHEMA shared  grant all on tables to group coviddbadmin;

grant all on all tables in schema opt_20210916 to group coviddbadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA opt_20210916  grant all on tables to group coviddbadmin;
/*
 * Analyst Role
 */
create role covid_analyst;
grant connect on database coviddb to group covid_analyst;

grant all on schema shared to group covid_analyst;
grant all on all tables in schema shared to group covid_analyst;
ALTER DEFAULT PRIVILEGES IN SCHEMA shared  grant all on tables to group covid_analyst;

grant usage on schema opt_20210916 to group covid_analyst;
grant select on all tables in schema opt_20210916 to group covid_analyst;
ALTER DEFAULT PRIVILEGES IN SCHEMA opt_20210916 grant select on tables to group covid_analyst; 



/*
 * Initial Setup
*/
revoke all on schema public from public;