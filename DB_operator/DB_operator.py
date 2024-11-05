import os
import pyodbc
from dotenv import load_dotenv
from DB_operator import SQL_Queries

class ConnectDB:
    @staticmethod
    def connect_to_db(server, database, user, password):
        ConnectionString = f'''DRIVER={{ODBC Driver 18 for SQL Server}};
                               SERVER={server};
                               DATABASE={database};
                               UID={user};
                               PWD={password};
                               TrustServerCertificate=yes;
                               Encrypt=no;'''
        try:
            conn = pyodbc.connect(ConnectionString)
            conn.autocommit = True
        except pyodbc.ProgrammingError as ex:
            print(ex)
        else:
            return conn


class MSSQLOperator:

    def __init__(self, connector_obj):
        self.conn = connector_obj

    def create_database(self, database_name, size=None, maxsize=None, filegrowth=None):
        SQL_COMMAND = SQL_Queries.create_database(database_name, size, maxsize, filegrowth)
        try:
            self.conn.execute(SQL_COMMAND)
        except pyodbc.ProgrammingError as ex:
            print(ex)
        else:
            return f"База данных {database_name} успешно создана"

    def create_table(self, database_name, table_name, sql_query):
        cursor = self.conn.cursor()
        cursor.execute(f'USE {database_name}')
        SQL_Query = sql_query(table_name)
        try:
            cursor.execute(SQL_Query)
        except pyodbc.ProgrammingError as ex:
            return ex
        else:
            return f"Таблица: {table_name} успешно создана"

    def drop_table(self, table_name):
        """
        Удаляет таблицу с указанным именем из базы данных.
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
            cursor.commit()
            print(f"Таблица '{table_name}' успешно удалена.")
        except Exception as e:
            print(f"Ошибка при удалении таблицы '{table_name}': {e}")
        finally:
            cursor.close()

    def drop_database(self, database_name):
        try:
            self.conn.execute("USE master;")  # Переключаемся на базу master
            SQL_COMMAND = f"DROP DATABASE IF EXISTS {database_name};"
            self.conn.execute(SQL_COMMAND)
            print(f"База данных {database_name} успешно удалена.")
        except pyodbc.ProgrammingError as ex:
            print(f"Ошибка при удалении базы данных {database_name}: {ex}")

    def database_exists(self, database_name):
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sys.databases WHERE name = ?", (database_name,))
        result = cursor.fetchone()
        return result is not None

    def table_exists(self, database_name, table_name):
        """
        Проверяет, существует ли таблица в указанной базе данных.

        :param database_name: Название базы данных
        :param table_name: Название таблицы
        :return: True, если таблица существует, иначе False
        """
        query = f"""
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{table_name}'
        """
        cursor = self.conn.cursor()
        cursor.execute(f"USE {database_name}")
        cursor.execute(query)
        return cursor.fetchone()[0] > 0


if __name__ == "__main__":
    load_dotenv()
    SERVER = os.getenv('MS_SQL_SERVER')
    DATABASE = os.getenv('MS_SQL_DATABASE')
    USER = os.getenv('MS_SQL_USER')
    PASSWORD = os.getenv('MS_SQL_KEY')
    new_db = "EmployersVacancies"

    my_conn = ConnectDB.connect_to_db(SERVER, DATABASE, USER, PASSWORD)
    my_db_operator = MSSQLOperator(my_conn)
    print(my_db_operator.create_database(new_db, "10", "20", "5%"))
    print(my_db_operator.drop_table(new_db, "Vacancies", SQL_Queries.drop_table))
    print(my_db_operator.drop_table(new_db, "Employers", SQL_Queries.drop_table))
    print(my_db_operator.create_table(new_db, "Employers", SQL_Queries.create_employers))
    print(my_db_operator.create_table(new_db, "Vacancies", SQL_Queries.create_vacancies))
