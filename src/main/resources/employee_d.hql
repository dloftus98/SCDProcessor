drop table employee_d;

create table employee_d as
select 
   row_number() over () as key,
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
   1 as version,
   from_unixtime(unix_timestamp(run_date, 'MM/dd/yyyy')) as begin_date,
   cast(NULL as string) as end_date,
   'Y' as most_recent
   from school_employees
   where run_date='7/15/2014';

