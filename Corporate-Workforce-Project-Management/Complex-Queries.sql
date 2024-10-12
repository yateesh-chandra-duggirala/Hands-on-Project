/* Complex Queries */

-- 1. Retrieve employees who are not in the HR department and have a salary less than 60,000.
select * from employees where department <> 'HR' and salary < 60000;

-- 2. Find the average duration of projects.
select AVG(AGE(end_date, start_date)) as avg_duration from projects;

-- 3. List departments that do not have any employees assigned to them.
select * From departments d left join employees e on d.department_name = e.department
where e.department is null;

-- 4. Retrieve employees who are assigned to all projects.
SELECT e.*
FROM employees e
JOIN employee_project_assignments epa ON e.employee_id = epa.employee_id
GROUP BY e.employee_id
HAVING COUNT(DISTINCT epa.project_id) = (SELECT COUNT(*) FROM projects);

-- 5. Find the total salary expenditure for each department.
select department, sum(Salary) from employees group by 1;


-- 6. List employees who were hired within the last 2 years.
SELECT * 
FROM employees 
WHERE hire_date >= current_date - INTERVAL '2 years';


-- 7. Find projects that have employees from multiple departments assigned to them.
SELECT p.project_id, p.project_name
FROM projects p
JOIN employee_project_assignments epa ON p.project_id = epa.project_id
JOIN employees e ON e.employee_id = epa.employee_id
GROUP BY p.project_id, p.project_name
HAVING COUNT(DISTINCT e.department) > 1;