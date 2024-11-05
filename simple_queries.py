import os
from dotenv import load_dotenv
import pyodbc

load_dotenv()
SERVER = os.getenv('MS_SQL_SERVER')
DATABASE = os.getenv('MS_SQL_DATABASE')
USER = os.getenv('MS_SQL_USER')
PASSWORD = os.getenv('MS_SQL_KEY')

# """SimpleConnection"""
# connectionString = f'''DRIVER={{SQL Server}};
#                        SERVER={SERVER};
#                        DATABASE={DATABASE};
#                        Trusted_Connection=yes'''

"""SecureConnection"""
connectionString = f'''DRIVER={{ODBC Driver 18 for SQL Server}};
                       SERVER={SERVER};
                       DATABASE={DATABASE};
                       UID={USER};
                       PWD={PASSWORD}'''

"""Create DB Params"""
SQL_COMMAND = r"""
CREATE DATABASE Products
ON
(
NAME = ProductsDatabase_data,
FILENAME = 'T:\Program Files\Microsoft SQL Server\MSSQL16.SQLEXPRESS\MSSQL\DATA\ProductsDatabase_data.mdf',
SIZE = 10MB,
MAXSIZE = 20GB,
FILEGROWTH=5%
)
LOG ON
(NAME = ProductsDatabase_log,
FILENAME = 'T:\Program Files\Microsoft SQL Server\MSSQL16.SQLEXPRESS\MSSQL\DATA\ProductsDatabase_data.ldf',
SIZE = 5MB,
MAXSIZE = 2GB,
FILEGROWTH = 5%
)"""

conn = pyodbc.connect(connectionString)
conn.autocommit = True
try:
    conn.execute(SQL_COMMAND)
except pyodbc.ProgrammingError as ex:
    # print(ex)
    print('База данных "Products" уже существует. Выберите другое имя базы данных.')
else:
    print("Database Created")
finally:
    conn.close()

SQL_QUERY = r"""
CREATE TABLE products
(product_id int PRIMARY KEY,
product_name nvarchar(50),
price int);
"""

conn = pyodbc.connect(connectionString)
conn.autocommit = True
cursor = conn.cursor()
try:
    cursor.execute("USE Products")
    cursor.execute(SQL_QUERY)
except pyodbc.ProgrammingError as ex:
    print('В базе данных уже существует объект с именем "products"')
else:
    print("Table Created")
finally:
    conn.close()

SQL_QUERY = r"""
INSERT INTO products (product_id, product_name, price)
VALUES
(1,'Desktop Computer',800),
(2,'Laptop',1200),
(3,'Tablet',200),
(4,'Monitor',350),
(5,'Printer',150)
"""

conn = pyodbc.connect(connectionString)
conn.autocommit = True
cursor = conn.cursor()
try:
    cursor.execute("USE Products")
    cursor.execute(SQL_QUERY)
except pyodbc.IntegrityError as ex:
    print(ex)
except pyodbc.ProgrammingError as ex:
    print(ex)
else:
    print("Data Inserted")
finally:
    conn.close()

SQL_QUERY = r"""
SELECT product_id, product_name, price
FROM products
"""

conn = pyodbc.connect(connectionString)
conn.autocommit = True
cursor = conn.cursor()
data_list = []
try:
    cursor.execute("USE Products")
    result = cursor.execute(SQL_QUERY)
except pyodbc.ProgrammingError as ex:
    print(ex)
else:
    records = result.fetchall()
    for record in records:
        data_dict = {'id': record.product_id, 'name': record.product_name, 'price': record.price}
        data_list.append(data_dict)
finally:
    conn.close()

for data in data_list:
    print(data)
