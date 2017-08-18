create table employee_d_v2 as
select key,
   first_name,
   last_name,
   home_organization,
   pay_rate_type,
   cast(pay_rate as int) as pay_rate,
   rand() as double_test,
   title_description,
   home_organization_description,
   organization_level,
   type_of_representation,
   gender,
   from_utc_timestamp(begin_date,'EST') as begin_date,
   from_utc_timestamp(end_date,'EST') as end_date,
   version,
   most_recent
   from employee_d;

drop table employees_filtered_v2;
create table employees_filtered_v2 as
select first_name,
   last_name,
   home_organization,
   pay_rate_type,
   cast(pay_rate as int) as pay_rate,
   rand() as double_test,
   title_description,
   home_organization_description,
   organization_level,
   type_of_representation,
   gender,
   run_date
   from employees_filtered;
