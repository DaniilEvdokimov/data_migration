from sqlalchemy import create_engine, MetaData, inspect, Table
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


def connect_db(database):
    """Подключение к бд на основе информации о параметрах подключения"""
    DATABASE = {
        'drivername': 'mysql',
        'host': 'localhost',
        'port': '3306',
        'username': 'root',
        'password': '1234',
        'database': database
    }

    global engine, DeclarativeBase, metadata, session, s

    engine = create_engine(URL.create(**DATABASE))

    DeclarativeBase = declarative_base()
    DeclarativeBase.metadata.create_all(engine)

    metadata = MetaData(engine)

    session = sessionmaker(bind=engine)
    s = session()


def drop_table():
    """Удаление таблицы"""
    database = input('Введите бд для подключения: ')
    connect_db(database)

    all_tables = inspect(engine).get_table_names()
    print('Таблицы: ')
    print(*all_tables, sep=', ')
    name_table = input("Укажите название таблицы для удаления: ")
    if name_table in all_tables:
        table = Table(name_table, metadata, autoload=True)
        table.drop(engine)
        print('Таблица удалена')
    else:
        print('Такой таблицы нет')