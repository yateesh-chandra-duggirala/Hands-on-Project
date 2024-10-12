/* Intermediate Queries*/

-- 1. Retrieve the total number of employees in each department.
select department, count(*) 
from employees
group by 1;

-- 2. List all employees who are not assigned to any projects.
select * 
	from employee_project_assignments epa 
	left join employees e 
	on e.employee_id = epa.employee_id
where epa.project_id is null;

-- 3. Find the highest salary in the IT department.
select max(salary) as max_salary 
from employees 
where department = 'IT';

-- 4. List employees along with the projects they are assigned to.
select e.employee_id, e.first_name, e.last_name, p.project_name 
	from employees e 
	join employee_project_assignments epa
	ON epa.employee_id = e.employee_id
	join projects p
	ON p.project_id = epa.project_id;

-- 5. Retrieve all projects along with the number of employees working on each project.
select p.project_name, count(epa.employee_id) as emp_count
	from employee_project_assignments epa 
	join projects p 
	on p.project_id = epa.project_id
group by 1;

-- Find employees whose first name starts with 'J'.
select * from EMPLOYEES where first_name ilike 'J%';

-- Find employees whose last name contains 'son'.
select * from EMPLOYEES where last_name ilike '%son%';

-- Retrieve the average salary for each department.
select department, avg(salary) from employees
group by 1;