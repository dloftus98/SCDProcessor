drop table phila_schools.employees_filtered;

create table phila_schools.employees_filtered as
select 
   last_name,
   first_name,
   pay_rate_type,
   pay_rate,
   title_description,
   home_organization,
   home_organization_description,
   organization_level,
   type_of_representation,
   gender,
   run_date
   from (
select
   row_number() over (partition by last_name, first_name, home_organization, run_date) as group_key,
   *
   from phila_schools.employees) iq
   where group_key=1;
