-- DDL : 
CREATE TABLE Employees (
    employee_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    department VARCHAR(50) NOT NULL,
    salary NUMERIC(10, 2) NOT NULL,
    hire_date DATE NOT NULL
);

CREATE TABLE Departments (
    department_id SERIAL PRIMARY KEY,
    department_name VARCHAR(50) NOT NULL
);

CREATE TABLE Projects (
    project_id SERIAL PRIMARY KEY,
    project_name VARCHAR(100) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE
);

CREATE TABLE Employee_Project_Assignments (
    employee_id INT NOT NULL,
    project_id INT NOT NULL,
    PRIMARY KEY (employee_id, project_id),
    FOREIGN KEY (employee_id) REFERENCES Employees (employee_id),
    FOREIGN KEY (project_id) REFERENCES Projects (project_id)
);


-- Inserting some Sample Records
INSERT INTO Employees (first_name, last_name, department, salary, hire_date) VALUES
('John', 'Doe', 'Sales', 50000, '2018-03-25'),
('Jane', 'Smith', 'HR', 60000, '2019-06-15'),
('Michael', 'Johnson', 'IT', 70000, '2020-01-10'),
('Emily', 'Davis', 'Sales', 55000, '2021-07-30'),
('James', 'Wilson', 'IT', 80000, '2017-12-22');

INSERT INTO Departments (department_name) VALUES
('Sales'),
('HR'),
('IT');

INSERT INTO Projects (project_id, project_name, start_date, end_date) VALUES
(101, 'Website Redesign', '2021-02-10', '2021-06-30'),
(102, 'Recruitment Drive', '2020-11-01', '2021-01-15'),
(103, 'Cloud Migration', '2020-03-20', '2021-05-10');

INSERT INTO Employee_Project_Assignments (employee_id, project_id) VALUES
(1, 101),
(2, 102),
(3, 103),
(4, 101),
(5, 103);
