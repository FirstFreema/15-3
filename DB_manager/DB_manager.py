import os
import json
import csv
import pyodbc
from dotenv import load_dotenv
from DB_operator.DB_operator import ConnectDB
from . import SQL_Queries


class DBManager:
    """
    Класс для управления базой данных и заполнения таблиц данными.
    """

    def __init__(self, connector_obj):
        self.conn = connector_obj
        self.cursor = self.conn.cursor()

    def fill_table(self, database_name, table_name, data_to_fill_list, sql_query):
        """
        Заполняет таблицу данными из списка.
        Параметры:
        - database_name: Имя базы данных.
        - table_name: Имя таблицы.
        - data_to_fill_list: Список данных для вставки.
        - sql_query: Функция SQL-запроса для вставки данных.
        """
        self.cursor.execute(f"USE {database_name}")
        try:
            for data_to_fill in data_to_fill_list:
                self.cursor.execute(sql_query(table_name, data_to_fill))
            self.conn.commit()
            return f"Данные успешно помещены в таблицу '{table_name}'"
        except pyodbc.Error as ex:
            self.conn.rollback()
            return f"Ошибка при заполнении таблицы '{table_name}': {ex}"

    def fetch_all_data(self, table_name):
        """
        Извлекает все данные из указанной таблицы.

        Параметры:
        - table_name: Имя таблицы, из которой извлекаются данные.

        Возвращает:
        - Результаты запроса в виде списка строк.
        """
        self.cursor.execute(f"SELECT * FROM {table_name}")
        rows = self.cursor.fetchall()
        return rows

class DataLoader:
    """
    Класс для загрузки данных из файлов.
    """

    @staticmethod
    def get_data_from_json(filename):
        """
        Получение данных из JSON файла.
        """
        if not os.path.exists(filename):
            raise FileNotFoundError(f"Файл {filename} не найден.")

        with open(filename, 'r', encoding='utf-8') as file:
            return json.load(file)

    @staticmethod
    def get_data_from_csv(filename):
        """
        Получение данных из CSV файла.
        """
        if not os.path.exists(filename):
            raise FileNotFoundError(f"Файл {filename} не найден.")

        data = []
        with open(filename, encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                data.append(row)
        return data

    def load_data(self, filename, file_format="json"):
        """
        Загрузка данных из файла в указанном формате.

        :param filename: Путь к файлу данных.
        :param file_format: Формат файла ("json" или "csv").
        :return: Данные в формате списка словарей.
        """
        if file_format == "json":
            return self.get_data_from_json(filename)
        elif file_format == "csv":
            return self.get_data_from_csv(filename)
        else:
            raise ValueError("Неподдерживаемый формат файла. Используйте 'json' или 'csv'.")


if __name__ == "__main__":
    load_dotenv()
    SERVER = os.getenv('MS_SQL_SERVER')
    DATABASE = os.getenv('MS_SQL_DATABASE')
    USER = os.getenv('MS_SQL_USER')
    PASSWORD = os.getenv('MS_SQL_KEY')
    active_db = "NorthWind"

    try:
        # Подключение к базе данных
        my_conn = ConnectDB.connect_to_db(SERVER, DATABASE, USER, PASSWORD)
        my_manager = DBManager(my_conn)
        data_loader = DataLoader()

        # Загрузка данных из JSON
        employers_data = data_loader.load_data(r'data/employers.json', file_format="json")
        vacancies_data = data_loader.load_data(r'data/vacancies.json', file_format="json")

        # Заполнение таблиц данными из JSON
        my_manager.fill_table(active_db, 'Employers', employers_data, SQL_Queries.fill_employers)
        my_manager.fill_table(active_db, 'Vacancies', vacancies_data, SQL_Queries.fill_vacancies)

        # Загрузка данных из CSV
        customers_data = data_loader.load_data(r'DB_CSV/north_data/customers_data.csv', file_format="csv")
        employees_data = data_loader.load_data(r'DB_CSV/north_data/employees_data.csv', file_format="csv")
        orders_data = data_loader.load_data(r'DB_CSV/north_data/orders_data.csv', file_format="csv")

        # Заполнение таблиц данными из CSV
        my_manager.fill_table(active_db, 'customers_data', customers_data, SQL_Queries.fill_customers)
        my_manager.fill_table(active_db, 'employees_data', employees_data, SQL_Queries.fill_employees)
        my_manager.fill_table(active_db, 'orders_data', orders_data, SQL_Queries.fill_orders)

    except pyodbc.Error as e:
        print(f"Ошибка подключения к базе данных: {e}")
    except FileNotFoundError as e:
        print(e)
