/*Basic Queries*/

-- 1. Find all employees in the Sales department.
select * from employees where department = 'Sales';

-- 2. Find employees who have a salary greater than 60,000.
select * from employees where salary > 60000;

-- 3. List all projects that ended before July 1, 2021.
select * from projects where end_date < '2021-07-01';

-- 4. Find employees who were hired in 2019.
select * from employees where text(hire_date) ilike '2019%';
/* or */
select * from employees where to_char(hire_date, 'YYYY') = '2019';
/* or */
select * from EMPLOYEES where hire_date between '2019-01-01' and '2019-12-31';