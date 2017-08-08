select key, last_name, first_name, pay_rate, title_description, home_organization, version, begin_date, end_date, most_recent from employee_d cluster by last_name, first_name, home_organization limit 50;

select last_name, first_name, pay_rate, title_description, home_organization from employees where first_name='MUSLIMAH' and last_name='ABDULLAH';

