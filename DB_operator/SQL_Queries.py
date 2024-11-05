def create_database_default(name):
    COMMAND = fr"CREATE DATABASE {name};"
    return COMMAND


def create_database(name, size=10, maxsize=50, filegrowth="5%"):
    COMMAND = fr"""
    CREATE DATABASE {name}
    ON
    (
    NAME = {name}Database_data,
    FILENAME = 'T:\Program Files\Microsoft SQL Server\MSSQL16.SQLEXPRESS\MSSQL\DATA\{name}Database_data.mdf',
    SIZE = {size}MB,
    MAXSIZE = {maxsize}GB,
    FILEGROWTH={filegrowth}
    )
    LOG ON
    (NAME = {name}Database_log,
    FILENAME = 'T:\Program Files\Microsoft SQL Server\MSSQL16.SQLEXPRESS\MSSQL\DATA\{name}Database_data.ldf',
    SIZE = {size}MB,
    MAXSIZE = {str(round(maxsize * 0.1))}GB,
    FILEGROWTH = {filegrowth}
    )"""
    return COMMAND


def create_employers(table_name):
    QUERY = fr"""CREATE TABLE {table_name}
            (employer_id int PRIMARY KEY,
            employer_name nvarchar(100),
            employer_url nvarchar(200));"""
    return QUERY

def create_employees(table_name):
    return f"""
    CREATE TABLE {table_name} (
        employee_id INT PRIMARY KEY IDENTITY(1,1), -- Автоинкремент
        first_name NVARCHAR(100) NOT NULL,
        last_name NVARCHAR(100) NOT NULL,
        title NVARCHAR(100) NOT NULL,
        birth_date DATE NOT NULL,
        notes NVARCHAR(1000) NOT NULL
    );
    """

def create_customers(table_name):
    return f"""
    CREATE TABLE {table_name} (
        customer_id NVARCHAR(10) PRIMARY KEY,
        company_name NVARCHAR(100) NOT NULL,
        contact_name NVARCHAR(50) NOT NULL
    );
    """

def create_orders(table_name):
    return f"""
    CREATE TABLE {table_name} (
        order_id INT PRIMARY KEY,
        customer_id NVARCHAR(10) NOT NULL,
        employee_id INT NOT NULL,
        order_date DATE NOT NULL,
        ship_city NVARCHAR(100) NOT NULL,
        FOREIGN KEY (customer_id) REFERENCES customers_data(customer_id),
        FOREIGN KEY (employee_id) REFERENCES employees_data(employee_id)
    );
    """

def create_vacancies(table_name):
    QUERY = fr"""CREATE TABLE {table_name}
            (vacancy_id int PRIMARY KEY,
            vacancy_name nvarchar(100),
            vacancy_url nvarchar(200),
            vacancy_salary_from int,
            vacancy_salary_to int,
            employer_id int
            REFERENCES employers(employer_id));"""
    return QUERY


def drop_table(table_name):
    QUERY = fr"DROP TABLE {table_name}"
    return QUERY
