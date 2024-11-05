import os
import sys
import pyodbc  # Добавлено для обработки исключений

# Добавляем корневую директорию в sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from DB_operator import ConnectDB, MSSQLOperator
from DB_manager import DBManager, DataLoader
import DB_operator.SQL_Queries as OperatorQueries
import DB_manager.SQL_Queries as ManagerQueries

if __name__ == "__main__":
    # Загрузка переменных окружения
    load_dotenv()
    SERVER = os.getenv('MS_SQL_SERVER')
    DATABASE = os.getenv('MS_SQL_DATABASE')
    USER = os.getenv('MS_SQL_USER')
    PASSWORD = os.getenv('MS_SQL_KEY')
    active_db = "NorthWind"

    my_conn = None  # Инициализация переменной my_conn значением None

    try:
        # Подключение к серверу
        my_conn = ConnectDB.connect_to_db(SERVER, "master", USER, PASSWORD)
        my_db_operator = MSSQLOperator(my_conn)

        # Проверка и создание базы данных
        if not my_db_operator.database_exists(active_db):
            print(my_db_operator.create_database(active_db, size=10, maxsize=50, filegrowth="5%"))
        else:
            print(f"База данных '{active_db}' уже существует. Пересоздаём таблицы...")
            # Удаление таблиц, если они существуют
            tables = ["orders_data", "employees_data", "customers_data"]
            for table in tables:
                if my_db_operator.table_exists(active_db, table):
                    print(f"Удаление таблицы '{table}'...")
                    my_db_operator.drop_table(table)

        # Подключение к базе данных NorthWind
        my_conn.close()
        my_conn = ConnectDB.connect_to_db(SERVER, active_db, USER, PASSWORD)
        my_db_operator = MSSQLOperator(my_conn)

        # Создание таблиц
        print("Создание таблиц...")
        print(my_db_operator.create_table(active_db, "customers_data", OperatorQueries.create_customers))
        print(my_db_operator.create_table(active_db, "employees_data", OperatorQueries.create_employees))
        print(my_db_operator.create_table(active_db, "orders_data", OperatorQueries.create_orders))

        # Инициализация менеджера базы данных и загрузчика данных
        db_manager = DBManager(my_conn)
        data_loader = DataLoader()

        # Загрузка и заполнение таблиц данными из CSV
        base_path = os.path.join(os.path.dirname(__file__), 'north_data')
        customers_data = data_loader.load_data(os.path.join(base_path, 'customers_data.csv'), file_format="csv")
        employees_data = data_loader.load_data(os.path.join(base_path, 'employees_data.csv'), file_format="csv")
        orders_data = data_loader.load_data(os.path.join(base_path, 'orders_data.csv'), file_format="csv")
        print(db_manager.fill_table(active_db, 'customers_data', customers_data, ManagerQueries.fill_customers))
        print(db_manager.fill_table(active_db, 'employees_data', employees_data, ManagerQueries.fill_employees))
        print(db_manager.fill_table(active_db, 'orders_data', orders_data, ManagerQueries.fill_orders))

        # Вывод данных из таблиц после заполнения
        tables = ['customers_data', 'employees_data', 'orders_data']
        for table in tables:
            print(f"\nДанные из таблицы '{table}':")
            rows = db_manager.fetch_all_data(table)
            for row in rows:
                print(row)

    except pyodbc.Error as e:
        print(f"Ошибка подключения к базе данных: {e}")
    except FileNotFoundError as e:
        print(e)
    finally:
        # Закрытие соединения с базой данных
        if my_conn is not None:
            my_conn.close()
