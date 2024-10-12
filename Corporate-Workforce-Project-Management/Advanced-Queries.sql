/* Advanced Queries */

-- 1. Find employees who are assigned to more than one project.
select e.employee_id, e.first_name, e.last_name, count(p.project_id)
	from projects p 
	join employee_project_assignments epa 
	on epa.project_id = p.project_id 
	join employees e 
	on e.employee_id = epa.employee_id 
	group by e.employee_id, e.first_name, e.last_name
	having count(p.project_id) > 1;

-- 2. Retrieve the employee(s) with the second-highest salary in the company.
with cte as (
	select *,
	RANK() OVER(ORDER BY SALARY DESC) As rnk 
	from employees
)
select * 
from cte 
where rnk = 2;

-- 3. Find the project(s) with the longest duration.
with cte as (
	select *, 
	RANK() over(order by (end_date - start_date) desc) as rnk 
	from projects
)
select * 
from cte 
where rnk = 1;

-- 4. List employees who were hired after 'Michael Johnson'.
select * 
from employees 
where hire_date > (
	select hire_date 
	from employees 
	where first_name = 'Michael' and last_name = 'Johnson'
);

-- 5. Retrieve all employees who work in departments that have more than one project assigned to them.
with cte as (
	select e.department, count(epa.project_id) 
	from employees e 
	join employee_project_assignments epa 
	on e.employee_id = epa.employee_id
	group by 1 
	having count(epa.project_id) > 1
)
select * 
from employees 
where department in (
	select department from cte
);

-- 6. Find the employee with the highest salary in each department.
with cte as (
	select first_name, last_name, department, salary, 
	RANK() OVER(partition by  department order by salary desc) as rnk 
	from employees
)
select first_name, last_name, department, salary
from cte 
where rnk = 1;
